"""Puzzle scoring: find_rate, overall rate, Glicko seed."""

from __future__ import annotations

from chess_graph.puzzles.generate import find_rate, glicko_seed, overall_find_rate


def test_find_rate_per_bracket():
    sol = [10, 20, 30, 40, 0, 5]
    reach = [100, 100, 100, 100, 0, 10]
    assert find_rate(sol, reach) == [0.1, 0.2, 0.3, 0.4, 0.0, 0.5]


def test_find_rate_missing_solution_edge_is_zero():
    # the best move was too rare to survive pruning -> no edge -> nobody found it
    assert find_rate(None, [100, 100, 100, 100, 100, 100]) == [0.0] * 6


def test_overall_find_rate():
    assert overall_find_rate([10, 10], [100, 100]) == 0.1
    assert overall_find_rate(None, [100]) == 0.0
    assert overall_find_rate([5], [0]) == 0.0  # no games reached


def test_glicko_seed_monotonic_and_bounded():
    assert glicko_seed(1.0) == 800    # everyone finds it -> easy
    assert glicko_seed(0.0) == 2400   # nobody finds it -> hard
    assert glicko_seed(0.5) == 1600
    assert glicko_seed(1.0) < glicko_seed(0.3) < glicko_seed(0.0)
    assert 800 <= glicko_seed(0.73) <= 2400
