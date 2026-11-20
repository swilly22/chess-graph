"""Raw PGN parsing, filters, and time-control parsing."""

from __future__ import annotations

from chess_graph.config import load_config
from chess_graph.ingest.pgn_stream import (
    IngestCounters,
    _parse_time_control,
    classify,
    iter_raw_games,
)

CFG = load_config()


def _lines(text: str):
    return iter(text.splitlines(keepends=True))


def _headers(**overrides) -> dict[str, str]:
    base = {
        "Event": "Rated Blitz game",
        "WhiteElo": "1600",
        "BlackElo": "1600",
        "TimeControl": "300+0",
        "Result": "1-0",
    }
    base.update(overrides)
    return base


# --- iter_raw_games ---------------------------------------------------------


def test_iter_raw_games_parses_two_games():
    pgn = (
        '[Event "A"]\n[WhiteElo "1500"]\n\n1. e4 e5 1-0\n\n'
        '[Event "B"]\n[WhiteElo "1600"]\n\n1. d4 d5 0-1\n\n'
    )
    games = list(iter_raw_games(_lines(pgn)))
    assert len(games) == 2
    assert games[0][0]["Event"] == "A"
    assert games[0][1] == "1. e4 e5 1-0"
    assert games[1][0]["WhiteElo"] == "1600"


def test_iter_raw_games_multiline_movetext():
    pgn = '[Event "A"]\n\n1. e4 e5\n2. Nf3 Nc6\n3. Bb5 a6 1-0\n\n'
    games = list(iter_raw_games(_lines(pgn)))
    assert len(games) == 1
    assert games[0][1] == "1. e4 e5 2. Nf3 Nc6 3. Bb5 a6 1-0"


def test_iter_raw_games_handles_final_game_without_trailing_blank():
    pgn = '[Event "A"]\n\n1. e4 e5 1-0'  # no trailing blank line
    games = list(iter_raw_games(_lines(pgn)))
    assert len(games) == 1
    assert games[0][1] == "1. e4 e5 1-0"


def test_iter_raw_games_quoted_value_with_brackets():
    pgn = '[Event "Rated Blitz tournament https://x/[y]"]\n\n1. e4 1-0\n\n'
    games = list(iter_raw_games(_lines(pgn)))
    assert games[0][0]["Event"] == "Rated Blitz tournament https://x/[y]"


# --- _parse_time_control ----------------------------------------------------


def test_parse_time_control():
    assert _parse_time_control("300+0") == (300, 0)
    assert _parse_time_control("180+2") == (180, 2)
    assert _parse_time_control("600") == (600, 0)
    assert _parse_time_control("-") == (10**6, 0)  # correspondence -> very long
    assert _parse_time_control("abc") is None
    assert _parse_time_control("300+x") is None


def test_correspondence_classifies_as_slow():
    c = IngestCounters()
    game = classify(_headers(TimeControl="-"), "1. e4 1-0", CFG, c)
    assert game is not None and game.tc == "slow"


# --- classify: acceptance and each rejection path ---------------------------


def test_classify_accepts_and_fills_fields():
    c = IngestCounters()
    game = classify(_headers(WhiteElo="1550", BlackElo="1490", Result="1/2-1/2"), "1. e4", CFG, c)
    assert game is not None
    assert game.tc == "blitz"
    assert game.result == "d"
    assert game.bracket == 2  # avg 1520 -> 1500-1799


def test_classify_rejects_non_standard_variant():
    c = IngestCounters()
    assert classify(_headers(Variant="Chess960"), "1. e4", CFG, c) is None
    assert c.rejected_variant == 1


def test_classify_accepts_explicit_standard_variant():
    c = IngestCounters()
    assert classify(_headers(Variant="Standard"), "1. e4", CFG, c) is not None


def test_classify_rejects_unrated():
    c = IngestCounters()
    assert classify(_headers(Event="Casual Blitz game"), "1. e4", CFG, c) is None
    assert c.rejected_unrated == 1


def test_classify_rejects_missing_or_unknown_rating():
    c = IngestCounters()
    assert classify(_headers(WhiteElo="?"), "1. e4", CFG, c) is None
    no_black = {k: v for k, v in _headers().items() if k != "BlackElo"}
    assert classify(no_black, "1. e4", CFG, c) is None
    assert c.rejected_rating_missing == 2


def test_classify_rating_diff_boundary():
    c = IngestCounters()
    # exactly 400 is allowed; 401 is rejected
    assert classify(_headers(WhiteElo="1600", BlackElo="2000"), "1. e4", CFG, c) is not None
    assert classify(_headers(WhiteElo="1600", BlackElo="2001"), "1. e4", CFG, c) is None
    assert c.rejected_rating_diff == 1


def test_classify_rejects_bullet():
    c = IngestCounters()
    assert classify(_headers(TimeControl="60+0"), "1. e4", CFG, c) is None  # 60s -> bullet
    assert classify(_headers(TimeControl="120+1"), "1. e4", CFG, c) is None  # 160s -> bullet
    assert c.rejected_time_control == 2


def test_classify_rejects_unfinished_result():
    c = IngestCounters()
    assert classify(_headers(Result="*"), "1. e4", CFG, c) is None
    assert c.rejected_result == 1


def test_classify_blitz_slow_boundary():
    c = IngestCounters()
    blitz = classify(_headers(TimeControl="300+0"), "1. e4", CFG, c)  # 300 -> blitz
    slow = classify(_headers(TimeControl="600+0"), "1. e4", CFG, c)  # 600 -> slow
    assert blitz.tc == "blitz"
    assert slow.tc == "slow"
