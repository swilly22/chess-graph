"""End-to-end CLI orchestration against a local .zst fixture (no network/DB)."""

from __future__ import annotations

import dataclasses

import zstandard

from chess_graph.cli import run_ingest
from chess_graph.config import load_config

# Four games on 1.e4 e5 2.Nf3 (two into Nc6, one into d6), plus a 4th bullet
# game that must be filtered out. All rated, bracket 2.
_PGN = """[Event "Rated Blitz game"]
[WhiteElo "1600"]
[BlackElo "1600"]
[TimeControl "300+0"]
[Result "1-0"]

1. e4 e5 2. Nf3 Nc6 1-0

[Event "Rated Blitz game"]
[WhiteElo "1600"]
[BlackElo "1600"]
[TimeControl "300+0"]
[Result "0-1"]

1. e4 e5 2. Nf3 Nc6 0-1

[Event "Rated Blitz game"]
[WhiteElo "1600"]
[BlackElo "1600"]
[TimeControl "300+0"]
[Result "1/2-1/2"]

1. e4 e5 2. Nf3 d6 1/2-1/2

[Event "Rated Bullet game"]
[WhiteElo "1600"]
[BlackElo "1600"]
[TimeControl "60+0"]
[Result "1-0"]

1. e4 e5 2. Nf3 Nc6 1-0
"""


def _fixture_zst(tmp_path):
    path = tmp_path / "sample.pgn.zst"
    path.write_bytes(zstandard.ZstdCompressor().compress(_PGN.encode()))
    return str(path)


def _cfg(min_games=1):
    return dataclasses.replace(load_config(), min_games_per_position=min_games)


def test_cli_memory_engine_report(tmp_path):
    cfg = _cfg(min_games=1)
    report = run_ingest(
        cfg, month="2026-10", limit=1000, source=_fixture_zst(tmp_path),
        do_load=False, progress_every=1000, engine="memory",
        duckdb_path=":memory:", temp_dir=None,
    )
    assert report["scanned"] == 4
    assert report["accepted"] == 3  # the bullet game is excluded
    assert report["rejections"]["time_control"] == 1
    assert report["san_errors"] == 0
    assert report["loaded"] is False
    # positions: start, e4, e4e5, e4e5Nf3, +Nc6, +d6 = 6 distinct
    assert report["positions_after_prune"] == 6


def test_cli_engines_agree_on_report(tmp_path):
    src = _fixture_zst(tmp_path)
    cfg = _cfg(min_games=1)
    mem = run_ingest(
        cfg, "2026-10", 1000, src, False, 1000, "memory", ":memory:", None
    )
    duck = run_ingest(
        cfg, "2026-10", 1000, src, False, 1000, "duckdb", ":memory:", str(tmp_path)
    )
    for field in (
        "scanned", "accepted", "positions_before_prune", "positions_after_prune",
        "edges_before_prune", "edges_after_prune", "san_errors",
    ):
        assert mem[field] == duck[field], field


def test_cli_min_games_floor_prunes(tmp_path):
    # With min_games=3, only the start/e4/e4e5/e4e5Nf3 chain (3 games) survives;
    # the Nc6 (2 games) and d6 (1 game) leaves are pruned.
    cfg = _cfg(min_games=3)
    report = run_ingest(
        cfg, "2026-10", 1000, _fixture_zst(tmp_path), False, 1000, "duckdb",
        ":memory:", str(tmp_path),
    )
    assert report["positions_after_prune"] == 4  # start, e4, e4e5, e4e5Nf3
    assert report["edges_after_prune"] == 3
