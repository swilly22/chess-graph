"""Ingestion + aggregation tests on a tiny in-memory PGN.

The key property: two games that reach the same position by different move
orders must merge into one position node with summed stats, and that position
must have one incoming MOVE edge per distinct parent.
"""

from __future__ import annotations

import chess

from chess_graph.config import load_config
from chess_graph.identity import position_key
from chess_graph.ingest.aggregate import Aggregator, iter_san_moves
from chess_graph.ingest.pgn_stream import IngestCounters, iter_accepted_games

# Game A and Game B transpose: both reach the position after d4 Nf6 c4 e6.
# Comments/clocks are present to exercise the tokenizer. Both games land in the
# same rating bracket (avg ~1510 and ~1590 -> bucket 2) and are blitz (300+0).
FIXTURE_PGN = """[Event "Rated Blitz game"]
[WhiteElo "1500"]
[BlackElo "1520"]
[TimeControl "300+0"]
[Result "1-0"]

1. d4 { [%clk 0:05:00] } 1... Nf6 2. c4 e6 1-0

[Event "Rated Blitz game"]
[WhiteElo "1600"]
[BlackElo "1580"]
[TimeControl "300+0"]
[Result "0-1"]

1. c4 e6 2. d4 Nf6 0-1
"""


def _final_key() -> int:
    board = chess.Board()
    for san in ["d4", "Nf6", "c4", "e6"]:
        board.push_san(san)
    return position_key(board)


def _aggregate_fixture() -> tuple[Aggregator, IngestCounters]:
    cfg = load_config()
    counters = IngestCounters()
    agg = Aggregator(cfg, first_seen=202610)
    lines = iter(FIXTURE_PGN.splitlines(keepends=True))
    for game in iter_accepted_games(lines, cfg, counters):
        agg.add_game(game)
    return agg, counters


def test_san_tokenizer_strips_comments_and_numbers():
    text = "1. d4 { [%clk 0:05:00] } 1... Nf6 2. c4 e6 1-0"
    assert list(iter_san_moves(text, 30)) == ["d4", "Nf6", "c4", "e6"]


def test_san_tokenizer_respects_depth_cap():
    text = "1. e4 e5 2. Nf3 Nc6 3. Bb5 a6"
    assert list(iter_san_moves(text, 3)) == ["e4", "e5", "Nf3"]


def test_san_tokenizer_strips_glued_annotations():
    # Lichess glues NAG annotations onto moves; python-chess rejects them, so the
    # tokenizer must strip them (this caused ~11% of games to truncate).
    text = "1. e4 e5 2. Nf3?! Nc6 3. Bb5?? a6!? 4. Ba4!! Nf6 1-0"
    moves = list(iter_san_moves(text, 30))
    assert moves == ["e4", "e5", "Nf3", "Nc6", "Bb5", "a6", "Ba4", "Nf6"]
    # And every token actually parses from the real game line.
    board = chess.Board()
    for san in moves:
        board.push_san(san)


def test_both_games_accepted():
    _, counters = _aggregate_fixture()
    assert counters.scanned == 2
    assert counters.accepted == 2


def test_transposition_merges_into_one_position():
    agg, _ = _aggregate_fixture()
    final = _final_key()
    # Both move orders produce: start + 4 (A) + 3 new (B) = 8 distinct positions.
    assert len(agg.positions) == 8
    # The transposition node is reached by both games.
    assert agg.positions[final].games == 2
    # ...and it has exactly one incoming edge per distinct parent (two here).
    incoming = [ek for ek in agg.edges if ek[1] == final]
    assert len(incoming) == 2


def test_start_position_counts_every_game():
    agg, _ = _aggregate_fixture()
    start = position_key(chess.Board())
    assert agg.positions[start].games == 2
    assert agg.positions[start].reach_blitz[2] == 2  # both games, bucket 2, blitz


def test_edge_result_split_by_bracket():
    agg, _ = _aggregate_fixture()
    start = position_key(chess.Board())
    board = chess.Board()
    d4_key = position_key((board.push_san("d4"), board)[1])
    # Game A (1-0 = white win) played start->d4 in bracket 2, blitz.
    edge = agg.edges[(start, d4_key)]
    assert edge.blitz_w[2] == 1
    assert edge.games == 1
    assert edge.uci == "d2d4"
    assert edge.first_seen == 202610
