"""Opening parsing: replay to final position, family/variation, parent tree."""

from __future__ import annotations

import chess

from chess_graph.enrich.openings import Opening, compute_parents, parse_opening
from chess_graph.identity import position_key


def test_parse_opening_replays_to_final_position():
    o = parse_opening("B20", "Sicilian Defense", "1. e4 c5")
    board = chess.Board()
    board.push_san("e4")
    board.push_san("c5")
    assert o.key == position_key(board)
    assert o.ply == 2
    assert o.uci_seq == ("e2e4", "c7c5")


def test_family_and_variation_split():
    najdorf = "1. e4 c5 2. Nf3 d6 3. d4 cxd4 4. Nxd4 Nf6 5. Nc3 a6"
    o = parse_opening("B90", "Sicilian Defense: Najdorf Variation", najdorf)
    assert o.family == "Sicilian Defense"
    assert o.variation == "Najdorf Variation"


def test_no_variation_is_none():
    o = parse_opening("C20", "King's Pawn Game", "1. e4 e5")
    assert o.family == "King's Pawn Game"
    assert o.variation is None


def test_compute_parents_prefix_tree():
    openings = [
        parse_opening("B20", "Sicilian Defense", "1. e4 c5"),
        parse_opening("B27", "Sicilian Defense: Hyperaccelerated", "1. e4 c5 2. Nf3 g6"),
        parse_opening("C20", "King's Pawn", "1. e4 e5"),
    ]
    parents = compute_parents(openings)
    # the deeper Sicilian is a subvariation of the base Sicilian
    assert parents["Sicilian Defense: Hyperaccelerated"] == "Sicilian Defense"
    # bases have no parent
    assert "Sicilian Defense" not in parents
    assert "King's Pawn" not in parents


def test_opening_is_dataclass():
    o = parse_opening("A00", "Start", "")
    assert isinstance(o, Opening)
    assert o.ply == 0  # empty line -> start position
    assert o.key == position_key(chess.Board())
