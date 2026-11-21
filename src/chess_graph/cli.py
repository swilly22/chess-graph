"""Phase-2 sample ingestion CLI.

    python -m chess_graph.cli ingest-sample --month 2026-10 --limit 1000000

Streams a Lichess standard-rated dump, aggregates to positions + MOVE edges,
prunes, loads into FalkorDB, and prints the measurements that drive the phase-2
decisions (ply cap, frequency floor, Rust parser, Game nodes).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import resource
import shutil
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path

from chess_graph.config import PipelineConfig, load_config
from chess_graph.ingest.aggregate import Aggregator
from chess_graph.ingest.aggregate_duckdb import DuckDBAggregator
from chess_graph.ingest.fullgame import parse_full_game
from chess_graph.ingest.games_store import GamesCorpusWriter
from chess_graph.ingest.pgn_stream import IngestCounters, iter_accepted_games, iter_raw_games
from chess_graph.ingest.prune import prune
from chess_graph.ingest.source import month_url, open_zst_lines
from chess_graph.load import falkor

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _peak_rss_mb() -> float:
    # macOS reports ru_maxrss in bytes; Linux in KiB.
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return round(maxrss / divisor, 1)


def _first_seen(month: str) -> int:
    year, mon = month.split("-")
    return int(year) * 100 + int(mon)


def run_ingest(
    cfg: PipelineConfig,
    month: str,
    limit: int,
    source: str | None,
    do_load: bool,
    progress_every: int,
    engine: str,
    duckdb_path: str,
    temp_dir: str | None,
    jobs: int = 1,
    chunk_size: int = 2_000,
) -> dict:
    src = source or month_url(month)
    first_seen = _first_seen(month)
    t0 = time.monotonic()
    closers: list = []  # objects to .close() after loading
    parts_dir: str | None = None

    if engine == "c":
        import duckdb

        from chess_graph.ingest.aggregate_duckdb import (
            build_positions_and_moves,
            iter_edge_rows,
            iter_node_rows,
        )

        producer = _REPO_ROOT / "native" / "producer"
        if not producer.exists():
            raise SystemExit("build the native producer first: make -C native producer")

        out_dir = temp_dir or os.path.dirname(duckdb_path) or "."
        shards_dir = os.path.join(out_dir, "c_shards")
        shutil.rmtree(shards_dir, ignore_errors=True)
        os.makedirs(shards_dir, exist_ok=True)
        parts_dir = shards_dir  # cleaned up after load
        engine_label = "c"
        is_url = (source is None) or source.startswith(("http://", "https://"))

        # URL + no limit (full file): fully fused -- N connections each download a
        # range and pipe it to a range-mode producer (download overlaps replay).
        # URL + limit (sample): parallel-download the needed prefix, then producer.
        # Local file: producer reads directly.
        counters = IngestCounters()
        fused = is_url and limit <= 0
        prefix_path: str | None = None
        if fused:
            from chess_graph.ingest.fetch import run_fused

            conns = min(jobs, 8)
            _log(f"fused parallel download+replay from URL with {conns} connections...")
            scanned, accepted = run_fused(src, str(producer), shards_dir, conns=conns)
            counters.scanned, counters.accepted = scanned, accepted
            shard_glob = f"{shards_dir}/r*"
        else:
            if is_url:
                from chess_graph.ingest.fetch import estimate_prefix_bytes, parallel_fetch

                conns = min(jobs, 8)
                nbytes = estimate_prefix_bytes(src, limit)
                prefix_path = os.path.join(out_dir, "c_source.zst")
                _log(f"parallel-downloading ~{nbytes >> 20} MB with {conns} connections...")
                parallel_fetch(src, prefix_path, nbytes, conns=conns)
                producer_input = prefix_path
            else:
                producer_input = source
            _log(f"running native producer (jobs={jobs}) ...")
            prod = subprocess.run(
                [str(producer), producer_input, shards_dir, str(limit), str(jobs)],
                capture_output=True, text=True,
            )
            _log(prod.stderr.strip())
            m = re.search(r"scanned=(\d+) accepted=(\d+)", prod.stderr)
            if m:
                counters.scanned, counters.accepted = int(m.group(1)), int(m.group(2))
            if prefix_path:
                try:
                    os.remove(prefix_path)
                except OSError:
                    pass
            shard_glob = shards_dir

        con = duckdb.connect(duckdb_path)
        con.execute("SET memory_limit = '8GB'")
        con.execute("PRAGMA threads = 4")
        if temp_dir:
            con.execute(f"SET temp_directory = '{temp_dir}'")
        pos_src = (
            f"read_csv('{shard_glob}/pos_*.csv', header=false, "
            "columns={'key':'BIGINT','epd':'VARCHAR','is_blitz':'BOOLEAN','bracket':'UTINYINT'})"
        )
        edge_src = (
            f"read_csv('{shard_glob}/edge_*.csv', header=false, columns={{'frm':'BIGINT',"
            "'t':'BIGINT','uci':'VARCHAR','san':'VARCHAR','is_blitz':'BOOLEAN',"
            "'bracket':'UTINYINT','result':'UTINYINT'})"
        )
        s = build_positions_and_moves(con, pos_src, edge_src, cfg.min_games_per_position)
        aggregate_seconds = time.monotonic() - t0
        pos_before, pos_after = s.positions_before, s.positions_after
        edge_before, edge_after = s.edges_before, s.edges_after
        node_rows, edge_rows = iter_node_rows(con), iter_edge_rows(con, first_seen)
        san_errors = 0
        closers.append(con)
    elif jobs > 1:
        # Parallel: workers replay into visit shards, then one DuckDB GROUP BY.
        from chess_graph.ingest.aggregate_duckdb import ShardVisitAggregator
        from chess_graph.ingest.parallel import run_opening_parallel

        engine_label = "duckdb-parallel"
        _log(f"streaming {src}  (engine={engine_label}, jobs={jobs})")
        _log(f"target accepted games: {limit:,}")

        def _progress(accepted: int) -> None:
            if accepted % progress_every < chunk_size:
                elapsed = time.monotonic() - t0
                r = accepted / elapsed if elapsed else 0
                _log(f"  {accepted:,} games | {r:,.0f} games/s")

        parts_dir = duckdb_path + ".parts"
        counters, san_errors, pos_glob, edge_glob = run_opening_parallel(
            cfg, src, limit, jobs, chunk_size, parts_dir, first_seen, _progress
        )
        aggregate_seconds = time.monotonic() - t0
        _log("aggregating visit shards...")
        shard_agg = ShardVisitAggregator(
            cfg, first_seen, pos_glob, edge_glob, duckdb_path, temp_dir=temp_dir
        )
        s = shard_agg.stats
        pos_before, pos_after = s.positions_before, s.positions_after
        edge_before, edge_after = s.edges_before, s.edges_after
        node_rows, edge_rows = shard_agg.node_rows(), shard_agg.edge_rows()
        closers.append(shard_agg)
    else:
        engine_label = engine
        counters = IngestCounters()
        if engine == "duckdb":
            agg: Aggregator | DuckDBAggregator = DuckDBAggregator(
                cfg, first_seen, db_path=duckdb_path, temp_dir=temp_dir
            )
        else:
            agg = Aggregator(cfg, first_seen)

        _log(f"streaming {src}  (engine={engine})")
        _log(f"target accepted games: {limit:,}")
        with open_zst_lines(src) as lines:
            for game in iter_accepted_games(lines, cfg, counters):
                agg.add_game(game)
                if agg.games_aggregated % progress_every == 0:
                    elapsed = time.monotonic() - t0
                    r = agg.games_aggregated / elapsed if elapsed else 0
                    extra = (
                        f"{len(agg.positions):,} pos | {len(agg.edges):,} edges | "
                        if isinstance(agg, Aggregator)
                        else ""
                    )
                    _log(f"  {agg.games_aggregated:,} games | {extra}{r:,.0f} games/s")
                if agg.games_aggregated >= limit:
                    break
        aggregate_seconds = time.monotonic() - t0
        san_errors = agg.san_errors

        if isinstance(agg, DuckDBAggregator):
            dstats = agg.finalize()
            pos_before, pos_after = dstats.positions_before, dstats.positions_after
            edge_before, edge_after = dstats.edges_before, dstats.edges_after
            node_rows, edge_rows = agg.node_rows(), agg.edge_rows()
            closers.append(agg)
        else:
            pr = prune(agg, cfg.min_games_per_position)
            pos_before, pos_after = pr.positions_before, pr.positions_after
            edge_before, edge_after = pr.edges_before, pr.edges_after
            node_rows = falkor._node_rows(agg.positions, pr.survivors)
            edge_rows = falkor._edge_rows(pr.edges)

    rate = counters.accepted / aggregate_seconds if aggregate_seconds else 0
    _log(f"aggregation done in {aggregate_seconds:,.1f}s ({rate:,.0f} games/s)")
    _log(
        f"pruned (min_games={cfg.min_games_per_position}): "
        f"positions {pos_before:,} -> {pos_after:,} | edges {edge_before:,} -> {edge_after:,}"
    )

    report: dict = {
        "month": month,
        "limit": limit,
        "engine": engine_label,
        "jobs": jobs,
        "scanned": counters.scanned,
        "accepted": counters.accepted,
        "accept_rate": round(counters.accepted / counters.scanned, 4) if counters.scanned else None,
        "rejections": {
            "variant": counters.rejected_variant,
            "unrated": counters.rejected_unrated,
            "rating_missing": counters.rejected_rating_missing,
            "rating_diff": counters.rejected_rating_diff,
            "time_control": counters.rejected_time_control,
            "result": counters.rejected_result,
        },
        "san_errors": san_errors,
        "aggregate_seconds": round(aggregate_seconds, 1),
        "games_per_second": round(rate, 0),
        "positions_before_prune": pos_before,
        "positions_after_prune": pos_after,
        "edges_before_prune": edge_before,
        "edges_after_prune": edge_after,
        "peak_rss_mb": _peak_rss_mb(),
        "loaded": False,
    }

    if do_load:
        _log("loading into FalkorDB...")
        graph, stats = falkor.load_rows(cfg, node_rows, edge_rows, reset=True)
        node_count = graph.query("MATCH (p:Position) RETURN count(p)").result_set[0][0]
        edge_count = graph.query("MATCH ()-[m:MOVE]->() RETURN count(m)").result_set[0][0]
        report["loaded"] = True
        report["load"] = {
            **asdict(stats),
            "node_seconds": round(stats.node_seconds, 1),
            "edge_seconds": round(stats.edge_seconds, 1),
            "graph_node_count": node_count,
            "graph_edge_count": edge_count,
            "graph_memory_mb": falkor.graph_memory_mb(cfg, graph),
        }
        _log(
            f"loaded {stats.nodes_loaded:,} nodes in {stats.node_seconds:,.1f}s, "
            f"{stats.edges_loaded:,} edges in {stats.edge_seconds:,.1f}s"
        )

    for obj in closers:
        obj.close()
    if parts_dir:
        shutil.rmtree(parts_dir, ignore_errors=True)
    report["peak_rss_mb"] = _peak_rss_mb()
    return report


def run_ingest_games(
    cfg: PipelineConfig,
    month: str,
    limit: int,
    source: str | None,
    out_parquet: str,
    duckdb_path: str,
    progress_every: int,
    jobs: int = 1,
    chunk_size: int = 5_000,
) -> dict:
    """Stream, filter, and record COMPLETE games into the Parquet corpus.

    ``jobs > 1`` fans the move replay out across worker processes.
    """
    src = source or month_url(month)
    counters = IngestCounters()

    # URL: parallel-download the needed prefix first (the CDN throttles per
    # connection, so this is the dominant speedup), then ingest the local file.
    prefix_path: str | None = None
    download_seconds = 0.0
    if src.startswith(("http://", "https://")):
        from chess_graph.ingest.fetch import estimate_prefix_bytes, parallel_fetch

        conns = min(8, jobs) if jobs > 1 else 8
        nbytes = estimate_prefix_bytes(src, limit)
        prefix_path = os.path.join(os.path.dirname(duckdb_path) or ".", "games_source.zst")
        _log(f"parallel-downloading ~{nbytes >> 20} MB with {conns} connections...")
        dt0 = time.monotonic()
        parallel_fetch(src, prefix_path, nbytes, conns=conns)
        download_seconds = time.monotonic() - dt0
        src = prefix_path

    _log(f"streaming {src}  (full-game corpus, jobs={jobs})")
    _log(f"target accepted games: {limit:,}")
    t0 = time.monotonic()

    if jobs > 1:
        from chess_graph.ingest.parallel import run_corpus_parallel

        def _progress(accepted: int) -> None:
            if accepted % progress_every < chunk_size:
                elapsed = time.monotonic() - t0
                rate = accepted / elapsed if elapsed else 0
                _log(f"  {accepted:,} games | {rate:,.0f} games/s")

        parts_dir = out_parquet + ".parts"
        counters, stats = run_corpus_parallel(
            cfg, src, limit, out_parquet, jobs, chunk_size, parts_dir, _progress
        )
    else:
        writer = GamesCorpusWriter(duckdb_path, out_parquet)
        with open_zst_lines(src) as lines:
            for headers, movetext in iter_raw_games(lines):
                counters.scanned += 1
                game = parse_full_game(headers, movetext, cfg, counters)
                if game is not None:
                    counters.accepted += 1
                    writer.add(game)
                    if counters.accepted % progress_every == 0:
                        elapsed = time.monotonic() - t0
                        rate = counters.accepted / elapsed if elapsed else 0
                        _log(f"  {counters.accepted:,} games | {rate:,.0f} games/s")
                    if counters.accepted >= limit:
                        break
        _log("writing Parquet corpus...")
        writer.finalize()
        stats = writer.stats()
        writer.close()

    parse_seconds = time.monotonic() - t0
    rate = counters.accepted / parse_seconds if parse_seconds else 0
    size_mb = round(os.path.getsize(out_parquet) / (1024 * 1024), 1)
    _log(f"corpus: {stats.games:,} games, {stats.total_plies:,} plies, {size_mb} MB")
    if prefix_path:
        try:
            os.remove(prefix_path)
        except OSError:
            pass

    return {
        "mode": "full-game-corpus",
        "month": month,
        "jobs": jobs,
        "out": out_parquet,
        "scanned": counters.scanned,
        "accepted": counters.accepted,
        "download_seconds": round(download_seconds, 1),
        "parse_seconds": round(parse_seconds, 1),
        "games_per_second": round(rate, 0),
        "total_plies": stats.total_plies,
        "avg_plies": stats.avg_plies,
        "max_plies": stats.max_plies,
        "games_with_eval": stats.games_with_eval,
        "eval_coverage": stats.eval_coverage,
        "truncated_games": stats.truncated,
        "distinct_game_ids": stats.distinct_ids,
        "parquet_mb": size_mb,
        "peak_rss_mb": _peak_rss_mb(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="chess_graph")
    sub = parser.add_subparsers(dest="command", required=True)

    ing = sub.add_parser("ingest-sample", help="stream, aggregate, prune, load a sample")
    ing.add_argument("--month", default="2026-10", help="YYYY-MM Lichess dump")
    ing.add_argument(
        "--limit", type=int, default=None,
        help="max accepted games (default: config sample size)",
    )
    ing.add_argument("--source", default=None, help="override source URL or local .pgn.zst path")
    ing.add_argument("--no-load", action="store_true", help="aggregate + prune only, skip FalkorDB")
    ing.add_argument(
        "--engine", choices=["memory", "duckdb", "c"], default="memory",
        help="engine: 'memory'/'duckdb' (Python replay) or 'c' (native producer, local .zst)",
    )
    ing.add_argument("--duckdb-path", default="data/aggregate.duckdb", help="on-disk DuckDB file")
    ing.add_argument("--temp-dir", default=None, help="DuckDB spill directory")
    ing.add_argument(
        "--jobs", type=int, default=1,
        help="worker processes for replay (>1 forces the parallel DuckDB path)",
    )
    ing.add_argument("--chunk-size", type=int, default=2_000, help="games per worker task")
    ing.add_argument("--progress-every", type=int, default=50_000)
    ing.add_argument("--report", default=None, help="write the JSON report to this path")

    games = sub.add_parser(
        "ingest-games", help="stream and record COMPLETE games into a Parquet corpus"
    )
    games.add_argument("--month", default="2026-10", help="YYYY-MM Lichess dump")
    games.add_argument(
        "--limit", type=int, default=None, help="max accepted games (default: config sample size)"
    )
    games.add_argument("--source", default=None, help="override source URL or local .pgn.zst path")
    games.add_argument(
        "--out", default=None, help="output Parquet path (default data/games_<month>.parquet)"
    )
    games.add_argument("--duckdb-path", default="data/games.duckdb", help="on-disk DuckDB file")
    games.add_argument(
        "--jobs", type=int, default=1,
        help="worker processes for move replay (1 = single-process)",
    )
    games.add_argument("--chunk-size", type=int, default=5_000, help="games per worker task")
    games.add_argument("--progress-every", type=int, default=50_000)
    games.add_argument("--report", default=None, help="write the JSON report to this path")

    emp = sub.add_parser("enrich-min-ply", help="compute min_ply (BFS from start) over chess_core")
    emp.add_argument("--report", default=None, help="write the JSON report to this path")

    est = sub.add_parser("enrich-structures", help="link positions to pawn structures")
    est.add_argument("--report", default=None, help="write the JSON report to this path")

    eop = sub.add_parser("enrich-openings", help="load Opening nodes + NAMED/SUBVARIATION_OF")
    eop.add_argument("--dir", default="data/openings", help="dir with a.tsv..e.tsv")
    eop.add_argument("--report", default=None, help="write the JSON report to this path")

    eev = sub.add_parser("enrich-evals", help="local Stockfish evals over positions + edges")
    eev.add_argument("--depth", type=int, default=None, help="fixed depth (else use --movetime)")
    eev.add_argument("--movetime", type=int, default=120, help="ms per position")
    eev.add_argument("--jobs", type=int, default=6, help="parallel Stockfish engines")
    eev.add_argument("--limit", type=int, default=None, help="only evaluate N positions (testing)")
    eev.add_argument("--report", default=None, help="write the JSON report to this path")

    args = parser.parse_args(argv)
    cfg = load_config()

    if args.command == "ingest-sample":
        limit = args.limit if args.limit is not None else cfg.sample_target_games
        report = run_ingest(
            cfg, args.month, limit, args.source, do_load=not args.no_load,
            progress_every=args.progress_every, engine=args.engine,
            duckdb_path=args.duckdb_path, temp_dir=args.temp_dir,
            jobs=args.jobs, chunk_size=args.chunk_size,
        )
    elif args.command == "ingest-games":
        limit = args.limit if args.limit is not None else cfg.sample_target_games
        out = args.out or f"data/games_{args.month}.parquet"
        report = run_ingest_games(
            cfg, args.month, limit, args.source, out_parquet=out,
            duckdb_path=args.duckdb_path, progress_every=args.progress_every,
            jobs=args.jobs, chunk_size=args.chunk_size,
        )
    elif args.command == "enrich-min-ply":
        from dataclasses import asdict as _asdict

        from chess_graph.enrich.min_ply import enrich_min_ply

        report = _asdict(enrich_min_ply(cfg))
    elif args.command == "enrich-structures":
        from dataclasses import asdict as _asdict

        from chess_graph.enrich.pawn_structure import enrich_structures

        report = _asdict(enrich_structures(cfg))
    elif args.command == "enrich-openings":
        from dataclasses import asdict as _asdict

        from chess_graph.enrich.openings import enrich_openings

        report = _asdict(enrich_openings(cfg, args.dir))
    elif args.command == "enrich-evals":
        from dataclasses import asdict as _asdict

        from chess_graph.enrich.evals import enrich_evals

        report = _asdict(
            enrich_evals(cfg, depth=args.depth, movetime_ms=args.movetime,
                         jobs=args.jobs, limit=args.limit)
        )
    else:
        return 1

    text = json.dumps(report, indent=2)
    print(text)
    if args.report:
        with open(args.report, "w") as fh:
            fh.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
