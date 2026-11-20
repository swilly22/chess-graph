"""Pruning: min-games floor and edge endpoint survival."""

from __future__ import annotations

import dataclasses

import chess

from chess_graph.config import load_config
from chess_graph.identity import position_key
from chess_graph.ingest.aggregate import Aggregator
from chess_graph.ingest.pgn_stream import AcceptedGame
from chess_graph.ingest.prune import prune


def _game(movetext: str, result="w") -> AcceptedGame:
    return AcceptedGame(tc="blitz", bracket=2, result=result, movetext=movetext)


def _aggregate(n_mainline: int, n_sideline: int) -> Aggregator:
    """n_mainline games of 1.e4 e5, n_sideline games of 1.e4 d5."""
    agg = Aggregator(load_config(), first_seen=202610)
    for _ in range(n_mainline):
        agg.add_game(_game("1. e4 e5 1-0"))
    for _ in range(n_sideline):
        agg.add_game(_game("1. e4 d5 0-1"))
    return agg


def test_prune_keeps_only_frequent_positions():
    cfg = dataclasses.replace(load_config(), min_games_per_position=3)
    # start: 5 games, e4: 5, e4e5: 3 (survive); e4d5: 2 (pruned).
    agg = _aggregate(n_mainline=3, n_sideline=2)
    result = prune(agg, cfg.min_games_per_position)

    start = position_key(chess.Board())
    board = chess.Board()
    e4 = position_key((board.push_san("e4"), board)[1])
    e4e5 = position_key((board.push_san("e5"), board)[1])
    b2 = chess.Board()
    b2.push_san("e4")
    e4d5 = position_key((b2.push_san("d5"), b2)[1])

    assert start in result.survivors
    assert e4 in result.survivors
    assert e4e5 in result.survivors
    assert e4d5 not in result.survivors  # only 2 games < 3
    assert result.positions_before == 4  # start, e4, e4e5, e4d5
    assert result.positions_after == 3


def test_prune_drops_edges_with_pruned_endpoint():
    cfg = dataclasses.replace(load_config(), min_games_per_position=3)
    agg = _aggregate(n_mainline=3, n_sideline=2)
    result = prune(agg, cfg.min_games_per_position)

    start = position_key(chess.Board())
    board = chess.Board()
    e4 = position_key((board.push_san("e4"), board)[1])
    e4e5 = position_key((board.push_san("e5"), board)[1])

    assert (start, e4) in result.edges  # both endpoints survive
    assert (e4, e4e5) in result.edges
    # the e4->d5 edge is dropped because e4d5 was pruned
    assert result.edges_after == 2
    assert all(a in result.survivors and b in result.survivors for (a, b) in result.edges)


def test_prune_everything_below_floor():
    cfg = dataclasses.replace(load_config(), min_games_per_position=100)
    agg = _aggregate(n_mainline=3, n_sideline=2)
    result = prune(agg, cfg.min_games_per_position)
    assert result.positions_after == 0
    assert result.edges_after == 0
