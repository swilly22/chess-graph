"""Pawn-structure signature + key."""

from __future__ import annotations

import chess

from chess_graph.enrich.pawn_structure import pawn_signature, structure_key


def _placement(board: chess.Board) -> str:
    return board.epd().split(" ", 1)[0]


def test_start_signature_strips_pieces():
    sig = pawn_signature(_placement(chess.Board()))
    assert sig == "8/pppppppp/8/8/8/8/PPPPPPPP/8"


def test_signature_after_e4():
    board = chess.Board()
    board.push_san("e4")
    assert pawn_signature(_placement(board)) == "8/pppppppp/8/8/4P3/8/PPPP1PPP/8"


def test_same_pawns_different_pieces_share_structure():
    # Same pawn skeleton, different piece placement -> same signature + key.
    a = chess.Board("r3k2r/pppppppp/8/8/8/8/PPPPPPPP/R3K2R w - - 0 1")
    b = chess.Board("4k3/pppppppp/8/8/8/8/PPPPPPPP/4K3 w - - 0 1")
    sig_a = pawn_signature(a.epd().split(" ", 1)[0])
    sig_b = pawn_signature(b.epd().split(" ", 1)[0])
    assert sig_a == sig_b == "8/pppppppp/8/8/8/8/PPPPPPPP/8"
    assert structure_key(sig_a) == structure_key(sig_b)


def test_key_is_signed_int64_and_stable():
    sig = "8/pppppppp/8/8/4P3/8/PPPP1PPP/8"
    k = structure_key(sig)
    assert -(2**63) <= k <= 2**63 - 1
    assert k == structure_key(sig)  # deterministic


def test_different_structures_differ():
    assert structure_key("8/pppppppp/8/8/8/8/PPPPPPPP/8") != structure_key(
        "8/pppppppp/8/8/4P3/8/PPPP1PPP/8"
    )
