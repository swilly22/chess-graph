"""Config loader + classification logic against the CLAUDE.md ingestion rules."""

from __future__ import annotations

import textwrap

import pytest

from chess_graph.config import RatingBrackets, TimeControl, load_config


def test_config_loads():
    cfg = load_config()
    assert cfg.depth_cap_plies == 30
    assert cfg.max_rating_diff == 400
    assert cfg.min_games_per_position == 20
    assert cfg.sample_target_games == 1_000_000
    assert cfg.core_graph == "chess_core"
    assert len(cfg.brackets.labels) == 6
    assert len(cfg.brackets.edges) == 5


def test_time_control_classification():
    """base + 40*inc: <180 bullet (excluded), <480 blitz, else slow."""
    tc = load_config().time_control
    assert tc.classify(60, 0) is None  # 1+0 bullet -> excluded
    assert tc.classify(120, 1) is None  # 120 + 40 = 160 < 180 -> bullet
    assert tc.classify(180, 0) == "blitz"  # exactly 180 -> blitz
    assert tc.classify(300, 0) == "blitz"  # 5+0
    assert tc.classify(180, 2) == "blitz"  # 180 + 80 = 260 < 480 -> blitz
    assert tc.classify(300, 5) == "slow"  # 300 + 200 = 500 >= 480 -> slow
    assert tc.classify(479, 0) == "blitz"  # just under the blitz ceiling
    assert tc.classify(480, 0) == "slow"  # exactly 480 -> slow
    assert tc.classify(600, 0) == "slow"  # 10+0 rapid


def test_rating_buckets():
    """Six buckets: <1200, 1200-1499, 1500-1799, 1800-2099, 2100-2399, 2400+."""
    brackets = load_config().brackets
    assert brackets.bucket(900) == 0
    assert brackets.bucket(1199) == 0
    assert brackets.bucket(1200) == 1
    assert brackets.bucket(1499) == 1
    assert brackets.bucket(1500) == 2
    assert brackets.bucket(1800) == 3
    assert brackets.bucket(2100) == 4
    assert brackets.bucket(2399) == 4
    assert brackets.bucket(2400) == 5
    assert brackets.bucket(3000) == 5


def test_time_control_unit_boundaries():
    tc = TimeControl(increment_multiplier=40, bullet_max_seconds=180, blitz_max_seconds=480)
    assert tc.classify(179, 0) is None  # just bullet
    assert tc.classify(180, 0) == "blitz"  # exactly the bullet ceiling -> blitz
    assert tc.classify(0, 12) == "slow"  # 0 + 480 = 480 -> slow
    assert tc.classify(0, 11) == "blitz"  # 0 + 440 = 440 -> blitz


def test_rating_bucket_unit():
    rb = RatingBrackets(edges=(1200, 1500, 1800, 2100, 2400), labels=("a", "b", "c", "d", "e", "f"))
    assert rb.bucket(0) == 0
    assert rb.bucket(1200.0) == 1
    assert rb.bucket(2399.9) == 4
    assert rb.bucket(10_000) == 5


def test_load_config_rejects_malformed_brackets(tmp_path):
    bad = tmp_path / "bad.toml"
    bad.write_text(
        textwrap.dedent(
            """
            [ingestion]
            variant = "standard"
            rated_only = true
            depth_cap_plies = 30
            max_rating_diff = 400
            [time_control]
            increment_multiplier = 40
            bullet_max_seconds = 180
            blitz_max_seconds = 480
            [rating_brackets]
            edges = [1200, 1500]
            labels = ["<1200", "1200+"]
            [pruning]
            min_games_per_position = 20
            [sample]
            target_games = 1000
            [falkordb]
            host = "localhost"
            port = 6379
            core_graph = "chess_core"
            batch_rows = 10000
            """
        )
    )
    with pytest.raises(ValueError, match="6 labels and 5 edges"):
        load_config(bad)
