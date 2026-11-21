"""Full-game parsing: whole move lists, metadata, and [%eval] extraction."""

from __future__ import annotations

import datetime

from chess_graph.config import load_config
from chess_graph.ingest.fullgame import (
    eval_to_cp,
    iter_moves_with_eval,
    parse_full_game,
)
from chess_graph.ingest.pgn_stream import IngestCounters, iter_raw_games

CFG = load_config()

# Morphy's "Opera Game" — 33 plies, ends in mate. Well beyond the 30-ply opening
# cap, with sparse [%eval] annotations (move 1 and the mating move 16).
OPERA_PGN = """[Event "Rated Blitz game"]
[Site "https://lichess.org/opera123"]
[UTCDate "2026.10.01"]
[UTCTime "12:00:00"]
[WhiteElo "1800"]
[BlackElo "1750"]
[TimeControl "300+0"]
[ECO "C41"]
[Opening "Philidor Defense"]
[Termination "Normal"]
[Result "1-0"]

1. e4 { [%eval 0.2] [%clk 0:05:00] } e5 2. Nf3 d6 3. d4 Bg4 4. dxe5 Bxf3
5. Qxf3 dxe5 6. Bc4 Nf6 7. Qb3 Qe7 8. Nc3 c6 9. Bg5 b5 10. Nxb5 cxb5
11. Bxb5+ Nbd7 12. O-O-O Rd8 13. Rxd7 Rxd7 14. Rd1 Qe6 15. Bxd7+ Nxd7
16. Qb8+ { [%eval #2] } Nxb8 17. Rd8# 1-0
"""


# --- eval_to_cp -------------------------------------------------------------


def test_eval_to_cp_material():
    assert eval_to_cp("0.2") == 20
    assert eval_to_cp("-1.35") == -135
    assert eval_to_cp("0") == 0
    assert eval_to_cp("  2.5 ") == 250


def test_eval_to_cp_mate_sentinel():
    assert eval_to_cp("#5") == 100000 - 5  # White mates in 5
    assert eval_to_cp("#-5") == -(100000 - 5)  # Black mates in 5
    assert eval_to_cp("#1") == 99999
    assert eval_to_cp("#-1") == -99999
    # shorter mates outrank longer ones; any mate outranks material
    assert eval_to_cp("#3") > eval_to_cp("#8")
    assert eval_to_cp("#8") > eval_to_cp("9.0")


def test_eval_to_cp_invalid():
    assert eval_to_cp("not-a-number") is None
    assert eval_to_cp("#x") is None


# --- iter_moves_with_eval ---------------------------------------------------


def test_iter_moves_extracts_aligned_evals():
    text = "1. e4 { [%eval 0.3] } e5 { [%eval 0.1] } 2. Nf3 { [%eval 0.25] } 1-0"
    moves = iter_moves_with_eval(text)
    assert [m[0] for m in moves] == ["e4", "e5", "Nf3"]
    assert [m[1] for m in moves] == [30, 10, 25]


def test_iter_moves_missing_evals_are_none():
    moves = iter_moves_with_eval("1. e4 e5 { [%clk 0:03:00] } 2. Nf3 1-0")
    assert moves == [["e4", None], ["e5", None], ["Nf3", None]]


def test_iter_moves_has_no_ply_cap():
    moves = iter_moves_with_eval(OPERA_PGN.split("\n\n", 1)[1])
    assert len(moves) == 33  # full game, not capped at 30


# --- parse_full_game --------------------------------------------------------


def _parse_opera():
    headers, movetext = next(iter_raw_games(iter(OPERA_PGN.splitlines(keepends=True))))
    return parse_full_game(headers, movetext, CFG, IngestCounters())


def test_parse_full_game_records_all_plies():
    g = _parse_opera()
    assert g is not None
    assert g.ply_count == 33
    assert len(g.moves_uci) == 33
    assert g.moves_uci[0] == "e2e4"
    assert g.moves_uci[-1] == "d1d8"  # Rd8#
    assert g.truncated is False


def test_parse_full_game_metadata():
    g = _parse_opera()
    assert g.game_id == "opera123"
    assert g.source == "lichess"
    assert g.white_elo == 1800 and g.black_elo == 1750
    assert g.bracket == 2  # avg 1775 -> 1500-1799
    assert g.tc == "blitz"
    assert g.result == "w"
    assert g.eco == "C41"
    assert g.opening == "Philidor Defense"
    assert g.termination == "Normal"
    utc = datetime.UTC
    expected = int(datetime.datetime(2026, 10, 1, 12, 0, 0, tzinfo=utc).timestamp())
    assert g.played_at == expected


def test_parse_full_game_evals_aligned():
    g = _parse_opera()
    assert g.has_eval is True
    assert len(g.evals_cp) == 33
    assert g.evals_cp[0] == 20  # move 1 e4 -> 0.2
    assert g.evals_cp[30] == 99998  # move 16 Qb8+ -> #2
    assert g.evals_cp[1] is None  # move with no eval


def test_parse_full_game_no_eval_stores_empty():
    headers, movetext = next(iter_raw_games(iter(OPERA_PGN.splitlines(keepends=True))))
    movetext_no_eval = movetext.replace("[%eval 0.2] ", "").replace("[%eval #2] ", "")
    g = parse_full_game(headers, movetext_no_eval, CFG, IngestCounters())
    assert g.has_eval is False
    assert g.evals_cp == []
    assert g.ply_count == 33  # moves still fully recorded


def test_parse_full_game_filtered_returns_none():
    headers = {
        "Event": "Rated Bullet game", "WhiteElo": "1800", "BlackElo": "1750",
        "TimeControl": "60+0", "Result": "1-0",
    }
    assert parse_full_game(headers, "1. e4 e5 1-0", CFG, IngestCounters()) is None


def test_parse_full_game_truncates_on_bad_san():
    headers = {
        "Event": "Rated Blitz game", "WhiteElo": "1600", "BlackElo": "1600",
        "TimeControl": "300+0", "Result": "1-0", "Site": "https://lichess.org/x",
    }
    g = parse_full_game(headers, "1. e4 e5 2. Zz9 1-0", CFG, IngestCounters())
    assert g.truncated is True
    assert g.moves_uci == ["e2e4", "e7e5"]  # valid prefix kept
    assert g.ply_count == 2
