"""Parallel HTTP range downloader.

The Lichess CDN throttles each connection (~3 MB/s here) but scales almost
linearly across parallel connections, so splitting the file (or the prefix we
need) into byte ranges and fetching them concurrently is the dominant speedup
for ingestion. Download is I/O-bound, so threads (not processes) are fine.
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

_USER_AGENT = "chess-graph/0.1 (parallel fetch)"
_CHUNK = 1 << 20  # 1 MiB streamed per read
_MAX_RETRIES = 8


def content_length(url: str) -> int:
    req = Request(url, method="HEAD", headers={"User-Agent": _USER_AGENT})
    with urlopen(req) as resp:  # noqa: S310 - trusted database.lichess.org
        return int(resp.headers["Content-Length"])


def _fetch_range(url: str, start: int, end: int, out_path: str) -> int:
    """Stream bytes [start, end] into out_path, resuming + backing off on errors.

    The CDN returns 429 under too many concurrent/rapid connections, so we honor
    Retry-After and resume from the last byte written rather than restarting.
    """
    pos = start
    attempt = 0
    with open(out_path, "r+b") as fh:
        fh.seek(start)
        while pos <= end:
            req = Request(
                url, headers={"User-Agent": _USER_AGENT, "Range": f"bytes={pos}-{end}"}
            )
            try:
                with urlopen(req) as resp:  # noqa: S310
                    while chunk := resp.read(_CHUNK):
                        fh.write(chunk)
                        pos += len(chunk)
                break
            except (HTTPError, URLError) as e:
                attempt += 1
                if attempt > _MAX_RETRIES:
                    raise
                wait = min(60, 2 ** attempt)
                if isinstance(e, HTTPError):
                    retry_after = e.headers.get("Retry-After") if e.headers else None
                    if retry_after and retry_after.isdigit():
                        wait = max(wait, int(retry_after))
                time.sleep(wait)
    return end - start + 1


def parallel_fetch(url: str, out_path: str, n_bytes: int, conns: int) -> int:
    """Download the first ``n_bytes`` of ``url`` into ``out_path`` using ``conns``
    parallel range connections. Returns bytes written."""
    conns = max(1, conns)
    with open(out_path, "wb") as fh:  # preallocate so each range writes its slice
        fh.truncate(n_bytes)

    span = (n_bytes + conns - 1) // conns
    ranges = []
    for i in range(conns):
        start = i * span
        if start >= n_bytes:
            break
        ranges.append((start, min(n_bytes - 1, start + span - 1)))

    with ThreadPoolExecutor(max_workers=len(ranges)) as pool:
        futures = [pool.submit(_fetch_range, url, s, e, out_path) for s, e in ranges]
        return sum(f.result() for f in futures)


def run_fused(
    url: str,
    producer: str,
    shards_dir: str,
    conns: int,
    total_bytes: int | None = None,
    margin: int = 8 << 20,
) -> tuple[int, int]:
    """Fully overlapped: N connections each download a byte-range and pipe it to
    a producer in range mode (frame-sync + own-whole-frames + skip leading
    partial). Each pipe overlaps its own download with its replay. Returns
    (scanned, accepted) summed. Shards land in shards_dir/r<i>/.
    """
    total = total_bytes or content_length(url)
    span = total // conns
    procs = []
    for i in range(conns):
        start = i * span
        end = total - 1 if i == conns - 1 else min(total - 1, (i + 1) * span + margin - 1)
        mode = "head" if i == 0 else "range"
        subdir = os.path.join(shards_dir, f"r{i}")
        os.makedirs(subdir, exist_ok=True)
        curl = subprocess.Popen(
            ["curl", "-sL", "-r", f"{start}-{end}", url], stdout=subprocess.PIPE
        )
        prod = subprocess.Popen(
            [producer, "-", subdir, "0", "1", mode, str(span)],
            stdin=curl.stdout, stderr=subprocess.PIPE, text=True,
        )
        curl.stdout.close()
        procs.append((curl, prod))

    scanned = accepted = 0
    for curl, prod in procs:
        _, err = prod.communicate()
        curl.wait()
        m = re.search(r"scanned=(\d+) accepted=(\d+)", err)
        if m:
            scanned += int(m.group(1))
            accepted += int(m.group(2))
    return scanned, accepted


def estimate_prefix_bytes(url: str, accepted_target: int, bytes_per_scanned: int = 520,
                          accept_rate: float = 0.60, margin: float = 1.8) -> int:
    """Estimate how many compressed bytes to fetch to yield ``accepted_target``
    accepted games, capped at the file size. Over-estimates on purpose."""
    total = content_length(url)
    scanned = accepted_target / max(accept_rate, 0.01)
    est = int(scanned * bytes_per_scanned * margin)
    return min(total, max(est, 1 << 20))
