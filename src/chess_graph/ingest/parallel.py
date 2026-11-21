"""Parallel ingestion across OS processes (sidesteps the GIL).

python-chess move replay is CPU-bound pure-Python, so threads don't help. The
work is embarrassingly parallel per game, so we split it: the **main process**
streams, decompresses, and runs the cheap header filter (keeping the exact-N cut
deterministic), while a pool of **worker processes** does the expensive move
replay. For the full-game corpus each worker writes its own Parquet shard, so no
merge is needed — the shards are combined with a single DuckDB pass at the end.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, as_completed, wait

import duckdb

from chess_graph.config import PipelineConfig
from chess_graph.ingest.aggregate_duckdb import DuckDBAggregator
from chess_graph.ingest.fullgame import build_full_game
from chess_graph.ingest.games_store import CorpusStats, GamesCorpusWriter, stats_from_parquet
from chess_graph.ingest.pgn_stream import IngestCounters, classify, iter_raw_games
from chess_graph.ingest.source import open_zst_lines


def _corpus_shard_worker(chunk: list[tuple], shard_path: str) -> tuple[str, int]:
    """Replay a chunk of already-accepted games into one Parquet shard."""
    writer = GamesCorpusWriter(":memory:", shard_path)
    for headers, accepted in chunk:
        writer.add(build_full_game(headers, accepted))
    writer.finalize()
    count = writer.count
    writer.close()
    return shard_path, count


def _accepted_chunks(
    cfg: PipelineConfig,
    src: str,
    limit: int,
    chunk_size: int,
    counters: IngestCounters,
    progress_cb: Callable[[int], None] | None,
    with_headers: bool = True,
) -> Iterator[list]:
    """Yield chunks of accepted games — filtering runs here, in the reader, so the
    exact-``limit`` cut stays deterministic.

    ``with_headers`` controls the chunk element: ``(headers, AcceptedGame)`` for
    the corpus (needs metadata) or bare ``AcceptedGame`` for the opening graph
    (saves pickling the header dict).
    """
    chunk: list = []
    with open_zst_lines(src) as lines:
        for headers, movetext in iter_raw_games(lines):
            counters.scanned += 1
            accepted = classify(headers, movetext, cfg, counters)
            if accepted is None:
                continue
            counters.accepted += 1
            chunk.append((headers, accepted) if with_headers else accepted)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
                if progress_cb:
                    progress_cb(counters.accepted)
            if counters.accepted >= limit:
                break
    if chunk:
        yield chunk


def run_corpus_parallel(
    cfg: PipelineConfig,
    src: str,
    limit: int,
    out_parquet: str,
    jobs: int,
    chunk_size: int,
    parts_dir: str,
    progress_cb: Callable[[int], None] | None = None,
) -> tuple[IngestCounters, CorpusStats]:
    """Build the full-game corpus with a process pool, then combine shards."""
    os.makedirs(parts_dir, exist_ok=True)
    counters = IngestCounters()
    shard_paths: list[str] = []
    shard_idx = 0

    # Bounded in-flight submission: the reader pauses (via wait) when workers are
    # saturated, giving backpressure so chunk text doesn't pile up in memory.
    with ProcessPoolExecutor(max_workers=jobs) as pool:
        futures: set = set()
        for chunk in _accepted_chunks(cfg, src, limit, chunk_size, counters, progress_cb):
            shard = os.path.join(parts_dir, f"shard_{shard_idx:05d}.parquet")
            shard_idx += 1
            futures.add(pool.submit(_corpus_shard_worker, chunk, shard))
            if len(futures) >= jobs * 2:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                shard_paths.extend(f.result()[0] for f in done)
        for f in as_completed(futures):
            shard_paths.append(f.result()[0])

    if not shard_paths:
        # nothing accepted: write an empty-but-typed corpus
        GamesCorpusWriter(":memory:", out_parquet).finalize()
        return counters, stats_from_parquet(out_parquet)

    glob = os.path.join(parts_dir, "shard_*.parquet")
    con = duckdb.connect(":memory:")
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{glob}')) "
        f"TO '{out_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    con.close()
    stats = stats_from_parquet(out_parquet)

    for path in shard_paths:
        try:
            os.remove(path)
        except OSError:
            pass
    try:
        os.rmdir(parts_dir)
    except OSError:
        pass

    return counters, stats


def _opening_shard_worker(
    chunk: list, pos_shard: str, edge_shard: str, cfg: PipelineConfig, first_seen: int
) -> tuple[str, str, int]:
    """Replay a chunk of accepted games into pos/edge visit Parquet shards."""
    agg = DuckDBAggregator(cfg, first_seen, db_path=":memory:")
    for accepted in chunk:
        agg.add_game(accepted)
    agg.export_visits(pos_shard, edge_shard)
    san_errors = agg.san_errors
    agg.close()
    return pos_shard, edge_shard, san_errors


def run_opening_parallel(
    cfg: PipelineConfig,
    src: str,
    limit: int,
    jobs: int,
    chunk_size: int,
    parts_dir: str,
    first_seen: int,
    progress_cb: Callable[[int], None] | None = None,
) -> tuple[IngestCounters, int, str, str]:
    """Replay games across workers into visit shards; return globs for the final
    DuckDB aggregation. Returns ``(counters, san_errors, pos_glob, edge_glob)``."""
    os.makedirs(parts_dir, exist_ok=True)
    counters = IngestCounters()
    san_errors = 0
    idx = 0

    with ProcessPoolExecutor(max_workers=jobs) as pool:
        futures: set = set()

        def _collect(done_futures) -> None:
            nonlocal san_errors
            for fut in done_futures:
                san_errors += fut.result()[2]

        chunks = _accepted_chunks(
            cfg, src, limit, chunk_size, counters, progress_cb, with_headers=False
        )
        for chunk in chunks:
            pos_shard = os.path.join(parts_dir, f"pos_{idx:05d}.parquet")
            edge_shard = os.path.join(parts_dir, f"edge_{idx:05d}.parquet")
            idx += 1
            futures.add(
                pool.submit(_opening_shard_worker, chunk, pos_shard, edge_shard, cfg, first_seen)
            )
            if len(futures) >= jobs * 2:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                _collect(done)
        _collect(as_completed(futures))

    pos_glob = os.path.join(parts_dir, "pos_*.parquet")
    edge_glob = os.path.join(parts_dir, "edge_*.parquet")
    return counters, san_errors, pos_glob, edge_glob
