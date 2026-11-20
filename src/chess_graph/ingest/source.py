"""Byte sources for the Lichess dumps, decompressed as a line stream.

Nothing is ever loaded whole into memory: the ``.zst`` is decompressed on the
fly and yielded line by line. An HTTP source can be abandoned early (we only
read until the sample is full), which closes the socket and stops the transfer
long before the ~29 GB file would finish downloading.
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from urllib.request import Request, urlopen

import zstandard

LICHESS_STANDARD_BASE = "https://database.lichess.org/standard"
_USER_AGENT = "chess-graph/0.1 (sample ingestion; contact via repo)"
# Read buffer for the HTTP body before it hits the zstd decompressor.
_HTTP_CHUNK = 1 << 20  # 1 MiB


def month_url(month: str) -> str:
    """URL of the standard-rated dump for ``YYYY-MM``."""
    return f"{LICHESS_STANDARD_BASE}/lichess_db_standard_rated_{month}.pgn.zst"


def _text_lines(binary: io.BufferedIOBase) -> Iterator[str]:
    """Decompress a ``.zst`` binary stream into decoded text lines."""
    dctx = zstandard.ZstdDecompressor()
    # read_across_frames=True: Lichess dumps may be multi-frame.
    reader = dctx.stream_reader(binary, read_across_frames=True)
    text = io.TextIOWrapper(reader, encoding="utf-8", errors="replace", newline="")
    yield from text


@contextmanager
def open_zst_lines(source: str) -> Iterator[Iterator[str]]:
    """Yield an iterator of decoded text lines from a ``.zst`` source.

    ``source`` is an ``http(s)://`` URL or a local file path. Used as a context
    manager so the underlying socket/file is closed even when the consumer stops
    early (the whole point of streaming: abandon the transfer once we have
    enough games).
    """
    if source.startswith(("http://", "https://")):
        req = Request(source, headers={"User-Agent": _USER_AGENT})
        resp = urlopen(req)  # noqa: S310 - trusted database.lichess.org host
        try:
            buffered = io.BufferedReader(resp, buffer_size=_HTTP_CHUNK)
            yield _text_lines(buffered)
        finally:
            resp.close()
    else:
        with open(Path(source), "rb") as fh:
            yield _text_lines(fh)
