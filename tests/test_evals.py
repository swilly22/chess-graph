"""Eval score -> centipawn conversion with the mate sentinel."""

from __future__ import annotations

from chess.engine import Cp, Mate

from chess_graph.enrich.evals import score_to_cp


def test_material_scores_passthrough():
    assert score_to_cp(Cp(50)) == 50
    assert score_to_cp(Cp(-120)) == -120
    assert score_to_cp(Cp(0)) == 0


def test_white_mate_sentinel():
    assert score_to_cp(Mate(3)) == 100000 - 3  # mating in 3
    assert score_to_cp(Mate(1)) == 99999


def test_losing_mate_sentinel():
    assert score_to_cp(Mate(-3)) == -(100000 - 3)  # getting mated in 3
    assert score_to_cp(Mate(-1)) == -99999


def test_mate_outranks_material_and_shorter_is_better():
    assert score_to_cp(Mate(5)) > score_to_cp(Cp(900))   # any mate > material
    assert score_to_cp(Mate(3)) > score_to_cp(Mate(8))   # shorter mate better
    assert score_to_cp(Mate(-3)) < score_to_cp(Mate(-8)) # being mated sooner is worse
