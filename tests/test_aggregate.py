"""Aggregator internals: SAN tokenizer edge cases and within-game dedup."""

from __future__ import annotations

import chess

from chess_graph.config import load_config
from chess_graph.identity import position_key
from chess_graph.ingest.aggregate import Aggregator, iter_san_moves
from chess_graph.ingest.pgn_stream import AcceptedGame

CFG = load_config()


def _game(movetext: str, tc="blitz", bracket=2, result="w") -> AcceptedGame:
    return AcceptedGame(tc=tc, bracket=bracket, result=result, movetext=movetext)


# --- iter_san_moves ---------------------------------------------------------


def test_tokenizer_strips_comments():
    text = "1. e4 { [%eval 0.1] [%clk 0:03:00] } 1... c5 { comment } 1-0"
    assert list(iter_san_moves(text, 30)) == ["e4", "c5"]


def test_tokenizer_strips_variations_defensively():
    text = "1. e4 e5 (1... c5 2. Nf3) 2. Nf3 1-0"
    assert list(iter_san_moves(text, 30)) == ["e4", "e5", "Nf3"]


def test_tokenizer_strips_nested_variations():
    text = "1. e4 e5 (1... c5 (1... e6 2. d4) 2. Nf3) 2. Nf3 Nc6 1-0"
    assert list(iter_san_moves(text, 30)) == ["e4", "e5", "Nf3", "Nc6"]


def test_tokenizer_handles_glued_move_number():
    assert list(iter_san_moves("1.e4 e5 2.Nf3", 30)) == ["e4", "e5", "Nf3"]


def test_tokenizer_skips_nags():
    assert list(iter_san_moves("1. e4 $1 e5 $2 1-0", 30)) == ["e4", "e5"]


def test_tokenizer_stops_at_every_result_token():
    for result in ("1-0", "0-1", "1/2-1/2", "*"):
        assert list(iter_san_moves(f"1. e4 e5 {result}", 30)) == ["e4", "e5"]


def test_tokenizer_black_first_ellipsis():
    # continuation after a comment: black move written as "1... c5"
    assert list(iter_san_moves("1. e4 { c } 1... c5 2. Nf3", 30)) == ["e4", "c5", "Nf3"]


# --- within-game dedup ------------------------------------------------------


def test_position_revisited_in_one_game_counts_once():
    # Nf3 Nf6 Ng1 Ng8 returns to the start; then Nf3 Nf6 repeats ply-2 position.
    agg = Aggregator(CFG, first_seen=202610)
    agg.add_game(_game("1. Nf3 Nf6 2. Ng1 Ng8 3. Nf3 Nf6 1-0"))
    start = position_key(chess.Board())
    # start is passed through twice (ply 0 and ply 4) but counted once.
    assert agg.positions[start].games == 1
    assert agg.games_aggregated == 1


def test_edge_revisited_in_one_game_counts_once():
    agg = Aggregator(CFG, first_seen=202610)
    agg.add_game(_game("1. Nf3 Nf6 2. Ng1 Ng8 3. Nf3 Nf6 1-0"))
    start = position_key(chess.Board())
    board = chess.Board()
    nf3 = position_key((board.push_san("Nf3"), board)[1])
    # The start->Nf3 edge occurs at ply 1 and ply 5, but is counted once.
    assert agg.edges[(start, nf3)].games == 1


def test_illegal_san_truncates_but_keeps_prefix():
    agg = Aggregator(CFG, first_seen=202610)
    agg.add_game(_game("1. e4 e5 2. Ke2zz garbage 1-0"))  # 3rd token illegal
    start = position_key(chess.Board())
    board = chess.Board()
    e4 = position_key((board.push_san("e4"), board)[1])
    assert agg.san_errors == 1
    assert start in agg.positions
    assert (start, e4) in agg.edges  # valid prefix was recorded
    assert agg.games_aggregated == 1


def test_depth_cap_limits_recorded_plies():
    # cap the replay at 4 plies; a 10-ply game records only the first 4.
    import dataclasses

    cfg = dataclasses.replace(load_config(), depth_cap_plies=4)
    agg = Aggregator(cfg, first_seen=202610)
    agg.add_game(_game("1. e4 e5 2. Nf3 Nc6 3. Bb5 a6 4. Ba4 Nf6 5. O-O Be7 1-0"))
    assert len(agg.edges) == 4
    assert len(agg.positions) == 5  # start + 4


def test_stored_epd_matches_key():
    agg = Aggregator(CFG, first_seen=202610)
    agg.add_game(_game("1. e4 c5 1-0"))
    board = chess.Board()
    for san in ["e4", "c5"]:
        board.push_san(san)
    key = position_key(board)
    assert agg.positions[key].epd == board.epd()
