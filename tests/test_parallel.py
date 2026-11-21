"""Parallel corpus ingestion must produce the same games as single-process."""

from __future__ import annotations

import dataclasses

import duckdb
import zstandard

from chess_graph.cli import run_ingest_games
from chess_graph.config import load_config
from chess_graph.ingest.aggregate_duckdb import DuckDBAggregator, ShardVisitAggregator
from chess_graph.ingest.fullgame import build_full_game, parse_full_game
from chess_graph.ingest.parallel import run_opening_parallel
from chess_graph.ingest.pgn_stream import (
    IngestCounters,
    classify,
    iter_accepted_games,
    iter_raw_games,
)
from chess_graph.ingest.source import open_zst_lines

CFG = load_config()


def _header(gid, tc="300+0"):
    return (
        f'[Event "Rated Blitz game"]\n[Site "https://lichess.org/{gid}"]\n'
        f'[WhiteElo "1600"]\n[BlackElo "1600"]\n[TimeControl "{tc}"]\n[Result "1-0"]\n'
    )


# 5 accepted games (distinct ids) + 1 bullet that must be filtered out.
_LINES = [
    _header("g1") + "\n1. e4 e5 2. Nf3 Nc6 1-0\n",
    _header("g2") + "\n1. d4 d5 2. c4 e6 3. Nc3 Nf6 1-0\n",
    _header("g3") + "\n1. e4 c5 2. Nf3 d6 1-0\n",
    _header("g4") + "\n1. c4 e5 2. g3 Nf6 1-0\n",
    _header("g5") + "\n1. e4 e6 2. d4 d5 1-0\n",
    _header("bullet", tc="60+0") + "\n1. e4 e5 1-0\n",
]
_PGN = "\n".join(_LINES) + "\n"


def _fixture(tmp_path):
    path = tmp_path / "sample.pgn.zst"
    path.write_bytes(zstandard.ZstdCompressor().compress(_PGN.encode()))
    return str(path)


def _game_ids(parquet):
    rows = duckdb.connect(":memory:").execute(
        f"SELECT game_id FROM read_parquet('{parquet}') ORDER BY game_id"
    ).fetchall()
    return [r[0] for r in rows]


def test_build_full_game_matches_parse_full_game():
    pgn = _header("x") + "\n1. e4 e5 2. Nf3 Nc6 1-0\n"
    headers, movetext = next(iter_raw_games(iter(pgn.splitlines(keepends=True))))
    accepted = classify(headers, movetext, CFG, IngestCounters())
    assert build_full_game(headers, accepted) == parse_full_game(
        headers, movetext, CFG, IngestCounters()
    )


def test_parallel_matches_single(tmp_path):
    src = _fixture(tmp_path)
    single = str(tmp_path / "single.parquet")
    par = str(tmp_path / "par.parquet")

    rep1 = run_ingest_games(
        CFG, "2026-10", 1000, src, out_parquet=single,
        duckdb_path=str(tmp_path / "s.duckdb"), progress_every=1000, jobs=1,
    )
    # chunk_size=2 with jobs=2 forces several shards that must recombine cleanly
    rep2 = run_ingest_games(
        CFG, "2026-10", 1000, src, out_parquet=par,
        duckdb_path=str(tmp_path / "p.duckdb"), progress_every=1000, jobs=2, chunk_size=2,
    )

    assert rep1["accepted"] == rep2["accepted"] == 5
    assert rep1["scanned"] == rep2["scanned"] == 6  # bullet filtered in both
    assert rep1["total_plies"] == rep2["total_plies"]
    assert _game_ids(single) == _game_ids(par) == ["g1", "g2", "g3", "g4", "g5"]


def test_parallel_exact_limit(tmp_path):
    # the reader enforces the exact cut even in parallel
    src = _fixture(tmp_path)
    out = str(tmp_path / "limited.parquet")
    rep = run_ingest_games(
        CFG, "2026-10", 3, src, out_parquet=out,
        duckdb_path=str(tmp_path / "l.duckdb"), progress_every=1000, jobs=2, chunk_size=1,
    )
    assert rep["accepted"] == 3
    assert len(_game_ids(out)) == 3


def test_parallel_opening_matches_serial(tmp_path):
    """The parallel opening path (visit shards -> union GROUP BY) must produce the
    same Position/MOVE rows as the serial DuckDB aggregator."""
    cfg = dataclasses.replace(load_config(), min_games_per_position=1)
    src = _fixture(tmp_path)

    serial = DuckDBAggregator(cfg, 202610, db_path=":memory:")
    counters = IngestCounters()
    with open_zst_lines(src) as lines:
        for game in iter_accepted_games(lines, cfg, counters):
            serial.add_game(game)
    serial.finalize()
    s_nodes = {r["key"]: r for r in serial.node_rows()}
    s_edges = {(r["a"], r["b"]): r for r in serial.edge_rows()}

    parts = str(tmp_path / "parts")
    _, _, pos_glob, edge_glob = run_opening_parallel(cfg, src, 1000, 2, 2, parts, 202610)
    shard = ShardVisitAggregator(cfg, 202610, pos_glob, edge_glob, ":memory:")
    p_nodes = {r["key"]: r for r in shard.node_rows()}
    p_edges = {(r["a"], r["b"]): r for r in shard.edge_rows()}

    assert s_nodes.keys() == p_nodes.keys()
    for key in s_nodes:
        assert s_nodes[key] == p_nodes[key], key
    assert s_edges.keys() == p_edges.keys()
    for ekey in s_edges:
        assert s_edges[ekey] == p_edges[ekey], ekey

    shard.close()
    serial.close()
