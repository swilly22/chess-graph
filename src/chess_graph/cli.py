"""Phase-2 sample ingestion CLI.

    python -m chess_graph.cli ingest-sample --month 2026-10 --limit 1000000

Streams a Lichess standard-rated dump, aggregates to positions + MOVE edges,
prunes, loads into FalkorDB, and prints the measurements that drive the phase-2
decisions (ply cap, frequency floor, Rust parser, Game nodes).
"""

from __future__ import annotations

import argparse
import json
import resource
import sys
import time
from dataclasses import asdict

from chess_graph.config import PipelineConfig, load_config
from chess_graph.ingest.aggregate import Aggregator
from chess_graph.ingest.aggregate_duckdb import DuckDBAggregator
from chess_graph.ingest.pgn_stream import IngestCounters, iter_accepted_games
from chess_graph.ingest.prune import prune
from chess_graph.ingest.source import month_url, open_zst_lines
from chess_graph.load import falkor


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
) -> dict:
    src = source or month_url(month)
    counters = IngestCounters()
    first_seen = _first_seen(month)
    if engine == "duckdb":
        agg: Aggregator | DuckDBAggregator = DuckDBAggregator(
            cfg, first_seen, db_path=duckdb_path, temp_dir=temp_dir
        )
    else:
        agg = Aggregator(cfg, first_seen)

    _log(f"streaming {src}  (engine={engine})")
    _log(f"target accepted games: {limit:,}")
    t0 = time.monotonic()
    with open_zst_lines(src) as lines:
        for game in iter_accepted_games(lines, cfg, counters):
            agg.add_game(game)
            if agg.games_aggregated % progress_every == 0:
                elapsed = time.monotonic() - t0
                rate = agg.games_aggregated / elapsed if elapsed else 0
                extra = (
                    f"{len(agg.positions):,} pos | {len(agg.edges):,} edges | "
                    if isinstance(agg, Aggregator)
                    else ""
                )
                _log(f"  {agg.games_aggregated:,} games | {extra}{rate:,.0f} games/s")
            if agg.games_aggregated >= limit:
                break
    aggregate_seconds = time.monotonic() - t0
    rate = agg.games_aggregated / aggregate_seconds if aggregate_seconds else 0
    _log(f"aggregation done in {aggregate_seconds:,.1f}s ({rate:,.0f} games/s)")

    # Aggregate + prune, producing row iterators the loader can consume.
    if isinstance(agg, DuckDBAggregator):
        dstats = agg.finalize()
        pos_before, pos_after = dstats.positions_before, dstats.positions_after
        edge_before, edge_after = dstats.edges_before, dstats.edges_after
        node_rows, edge_rows = agg.node_rows(), agg.edge_rows()
    else:
        pr = prune(agg, cfg.min_games_per_position)
        pos_before, pos_after = pr.positions_before, pr.positions_after
        edge_before, edge_after = pr.edges_before, pr.edges_after
        node_rows = falkor._node_rows(agg.positions, pr.survivors)
        edge_rows = falkor._edge_rows(pr.edges)
    _log(
        f"pruned (min_games={cfg.min_games_per_position}): "
        f"positions {pos_before:,} -> {pos_after:,} | edges {edge_before:,} -> {edge_after:,}"
    )

    report: dict = {
        "month": month,
        "limit": limit,
        "engine": engine,
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
        "san_errors": agg.san_errors,
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

    if isinstance(agg, DuckDBAggregator):
        agg.close()
    report["peak_rss_mb"] = _peak_rss_mb()
    return report


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
        "--engine", choices=["memory", "duckdb"], default="memory",
        help="aggregation engine: 'memory' for small samples, 'duckdb' for the full 1M run",
    )
    ing.add_argument("--duckdb-path", default="data/aggregate.duckdb", help="on-disk DuckDB file")
    ing.add_argument("--temp-dir", default=None, help="DuckDB spill directory")
    ing.add_argument("--progress-every", type=int, default=50_000)
    ing.add_argument("--report", default=None, help="write the JSON report to this path")

    args = parser.parse_args(argv)
    cfg = load_config()

    if args.command == "ingest-sample":
        limit = args.limit if args.limit is not None else cfg.sample_target_games
        report = run_ingest(
            cfg, args.month, limit, args.source, do_load=not args.no_load,
            progress_every=args.progress_every, engine=args.engine,
            duckdb_path=args.duckdb_path, temp_dir=args.temp_dir,
        )
        text = json.dumps(report, indent=2)
        print(text)
        if args.report:
            with open(args.report, "w") as fh:
                fh.write(text)
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
