"""Loader row-builders (pure) and a gated FalkorDB integration test.

The integration test uses a throwaway graph name so it never touches
`chess_core`, and skips when FalkorDB isn't reachable.
"""

from __future__ import annotations

import dataclasses

import chess
import pytest

from chess_graph.config import load_config
from chess_graph.identity import position_key
from chess_graph.ingest.aggregate import Aggregator
from chess_graph.ingest.pgn_stream import AcceptedGame
from chess_graph.ingest.prune import prune
from chess_graph.load import falkor
from chess_graph.load.falkor import _batched, _edge_rows, _node_rows

CFG = load_config()


def _game(movetext: str, result="w") -> AcceptedGame:
    return AcceptedGame(tc="blitz", bracket=2, result=result, movetext=movetext)


# --- pure helpers -----------------------------------------------------------


def test_batched_chunks_exactly():
    assert list(_batched(iter(range(5)), 2)) == [[0, 1], [2, 3], [4]]
    assert list(_batched(iter([]), 2)) == []
    assert list(_batched(iter(range(4)), 10)) == [[0, 1, 2, 3]]


def test_node_rows_derive_side_and_castling():
    agg = Aggregator(CFG, first_seen=202610)
    agg.add_game(_game("1. e4 c5 1-0"))
    survivors = set(agg.positions)
    rows = {r["key"]: r for r in _node_rows(agg.positions, survivors)}

    start = position_key(chess.Board())
    assert rows[start]["side"] == "w"
    assert rows[start]["castling"] == "KQkq"
    board = chess.Board()
    e4 = position_key((board.push_san("e4"), board)[1])
    assert rows[e4]["side"] == "b"
    assert rows[e4]["reach_blitz"] == [0, 0, 1, 0, 0, 0]
    assert rows[e4]["reach_slow"] == [0, 0, 0, 0, 0, 0]


def test_node_rows_reflect_reduced_castling():
    agg = Aggregator(CFG, first_seen=202610)
    # White loses castling by moving the king; black still has KQkq minus... keep simple.
    agg.add_game(_game("1. e4 e5 2. Ke2 Nf6 1-0"))
    board = chess.Board()
    for san in ["e4", "e5", "Ke2"]:
        board.push_san(san)
    key = position_key(board)
    rows = {r["key"]: r for r in _node_rows(agg.positions, set(agg.positions))}
    assert rows[key]["castling"] == "kq"  # white's KQ gone, black retains kq


def test_edge_rows_shape():
    agg = Aggregator(CFG, first_seen=202610)
    agg.add_game(_game("1. e4 c5 1-0"))
    rows = list(_edge_rows(agg.edges))
    start = position_key(chess.Board())
    board = chess.Board()
    e4 = position_key((board.push_san("e4"), board)[1])
    row = next(r for r in rows if r["a"] == start and r["b"] == e4)
    assert row["uci"] == "e2e4"
    assert row["san"] == "e4"
    assert row["games"] == 1
    assert row["bw"] == [0, 0, 1, 0, 0, 0]
    assert row["fs"] == 202610
    assert set(row) == {"a", "b", "uci", "san", "games", "bw", "bd", "bb", "sw", "sd", "sb", "fs"}


# --- gated FalkorDB integration --------------------------------------------


@pytest.fixture
def falkor_test_graph():
    """A disposable graph; skips if FalkorDB is unreachable. Always cleaned up."""
    cfg = dataclasses.replace(load_config(), core_graph="chess_core_pytest_itest")
    try:
        graph = falkor.connect(cfg)
        falkor.reset_graph(graph)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"FalkorDB not reachable: {exc}")
    yield cfg, graph
    try:
        falkor.connect(cfg).delete()
    except Exception:
        pass


def test_integration_load_and_query(falkor_test_graph):
    cfg, _ = falkor_test_graph
    agg = Aggregator(cfg, first_seen=202610)
    agg.add_game(_game("1. e4 e5 2. Nf3 Nc6 1-0", result="w"))
    agg.add_game(_game("1. e4 e5 2. Nf3 Nc6 0-1", result="b"))
    pr = prune(agg, min_games=1)

    graph, stats = falkor.load_all(cfg, agg, pr.survivors, pr.edges, reset=True)

    node_count = graph.query("MATCH (p:Position) RETURN count(p)").result_set[0][0]
    edge_count = graph.query("MATCH ()-[m:MOVE]->() RETURN count(m)").result_set[0][0]
    assert node_count == len(pr.survivors) == 5
    assert edge_count == len(pr.edges) == 4
    assert stats.nodes_loaded == 5
    assert stats.edges_loaded == 4

    # start position carries both games
    start = position_key(chess.Board())
    games = graph.query("MATCH (p:Position {key:$k}) RETURN p.games", {"k": start}).result_set[0][0]
    assert games == 2

    # the e4 edge split both results (one white win, one black win)
    row = graph.query(
        "MATCH (:Position {key:$k})-[m:MOVE {uci:'e2e4'}]->() RETURN m.blitz_w, m.blitz_b",
        {"k": start},
    ).result_set[0]
    assert row[0][2] == 1  # blitz_w bucket 2
    assert row[1][2] == 1  # blitz_b bucket 2


def test_integration_unique_constraint_active(falkor_test_graph):
    cfg, graph = falkor_test_graph
    falkor.create_schema(graph)
    constraints = graph.list_constraints()
    assert any(
        c["type"] == "UNIQUE" and c["label"] == "Position" and "key" in c["properties"]
        for c in constraints
    )
    assert all(c["status"] == "OPERATIONAL" for c in constraints)
