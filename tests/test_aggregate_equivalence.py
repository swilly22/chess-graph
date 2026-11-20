"""The DuckDB aggregator must produce byte-for-byte the same loadable rows as
the in-memory aggregator — otherwise the full-run numbers wouldn't match the
sample-run numbers. Three games on one line, spanning different brackets, time
controls, and results, exercise every reach/W-D-B bucket."""

from __future__ import annotations

import dataclasses

from chess_graph.config import load_config
from chess_graph.ingest.aggregate import Aggregator
from chess_graph.ingest.aggregate_duckdb import DuckDBAggregator
from chess_graph.ingest.pgn_stream import IngestCounters, iter_accepted_games
from chess_graph.ingest.prune import prune
from chess_graph.load.falkor import _edge_rows, _node_rows

# Same mainline (1.e4 e5 2.Nf3 Nc6) three times:
#   g1: <1200 bracket, blitz, white win
#   g2: 1500-1799 bracket, blitz, draw
#   g3: 2400+ bracket, slow, black win
# With min_games=2 the whole line (positions + edges) survives pruning.
EQUIV_PGN = """[Event "Rated Blitz game"]
[WhiteElo "1100"]
[BlackElo "1100"]
[TimeControl "300+0"]
[Result "1-0"]

1. e4 e5 2. Nf3 Nc6 1-0

[Event "Rated Blitz game"]
[WhiteElo "1600"]
[BlackElo "1600"]
[TimeControl "300+0"]
[Result "1/2-1/2"]

1. e4 e5 2. Nf3 Nc6 1/2-1/2

[Event "Rated Classical game"]
[WhiteElo "2500"]
[BlackElo "2500"]
[TimeControl "600+0"]
[Result "0-1"]

1. e4 e5 2. Nf3 Nc6 0-1
"""


def _memory_rows(cfg):
    counters = IngestCounters()
    agg = Aggregator(cfg, first_seen=202610)
    for game in iter_accepted_games(iter(EQUIV_PGN.splitlines(keepends=True)), cfg, counters):
        agg.add_game(game)
    pr = prune(agg, cfg.min_games_per_position)
    nodes = {r["key"]: r for r in _node_rows(agg.positions, pr.survivors)}
    edges = {(r["a"], r["b"]): r for r in _edge_rows(pr.edges)}
    return nodes, edges


def _duckdb_rows(cfg):
    counters = IngestCounters()
    agg = DuckDBAggregator(cfg, first_seen=202610, db_path=":memory:")
    for game in iter_accepted_games(iter(EQUIV_PGN.splitlines(keepends=True)), cfg, counters):
        agg.add_game(game)
    agg.finalize()
    nodes = {r["key"]: r for r in agg.node_rows()}
    edges = {(r["a"], r["b"]): r for r in agg.edge_rows()}
    agg.close()
    return nodes, edges


def test_duckdb_matches_memory_rows():
    cfg = dataclasses.replace(load_config(), min_games_per_position=2)
    mem_nodes, mem_edges = _memory_rows(cfg)
    duck_nodes, duck_edges = _duckdb_rows(cfg)

    # A 4-ply line survives: 5 positions (start + 4) and 4 edges.
    assert len(mem_nodes) == 5
    assert mem_nodes.keys() == duck_nodes.keys()
    for key in mem_nodes:
        assert mem_nodes[key] == duck_nodes[key], key

    assert len(mem_edges) == 4
    assert mem_edges.keys() == duck_edges.keys()
    for ekey in mem_edges:
        assert mem_edges[ekey] == duck_edges[ekey], ekey


def test_bucket_and_tc_mapping_is_correct():
    """Spot-check the actual values, so an identical-but-wrong mapping can't pass."""
    cfg = dataclasses.replace(load_config(), min_games_per_position=2)
    nodes, edges = _duckdb_rows(cfg)
    import chess

    from chess_graph.identity import position_key

    start = position_key(chess.Board())
    # start reached by all 3 games: 2 blitz (buckets 0 and 2), 1 slow (bucket 5).
    assert nodes[start]["reach_blitz"] == [1, 0, 1, 0, 0, 0]
    assert nodes[start]["reach_slow"] == [0, 0, 0, 0, 0, 1]
    assert nodes[start]["games"] == 3

    board = chess.Board()
    e4 = position_key((board.push_san("e4"), board)[1])
    edge = edges[(start, e4)]
    assert edge["bw"] == [1, 0, 0, 0, 0, 0]  # g1: bucket 0, blitz, white win
    assert edge["bd"] == [0, 0, 1, 0, 0, 0]  # g2: bucket 2, blitz, draw
    assert edge["sb"] == [0, 0, 0, 0, 0, 1]  # g3: bucket 5, slow, black win
    assert edge["games"] == 3
