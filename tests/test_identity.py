"""Identity tests — the required cases from CLAUDE.md plus a few invariants.

These guard the one rule that, if broken, silently corrupts the whole graph:
every distinct position maps to exactly one key, and transpositions collapse.
"""

from __future__ import annotations

import chess

from chess_graph.identity import (
    board_from_epd,
    normalize_board,
    normalize_epd,
    normalize_fen,
    position_epd,
    position_key,
)

# int64 range, for the signed-fold check.
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


def key_after(sans: list[str]) -> int:
    board = chess.Board()
    for san in sans:
        board.push_san(san)
    return position_key(board)


def board_after(sans: list[str]) -> chess.Board:
    board = chess.Board()
    for san in sans:
        board.push_san(san)
    return board


# --- The five required cases ------------------------------------------------


def test_transposition_same_key():
    """1.d4 Nf6 2.c4 e6  ==  1.c4 e6 2.d4 Nf6."""
    assert key_after(["d4", "Nf6", "c4", "e6"]) == key_after(["c4", "e6", "d4", "Nf6"])


def test_knight_round_trip_returns_start():
    """1.Nf3 Nf6 2.Ng1 Ng8 returns to the starting position's key."""
    assert key_after(["Nf3", "Nf6", "Ng1", "Ng8"]) == position_key(chess.Board())


def test_non_capturable_en_passant_is_ignored():
    """After 1.e4 the key equals the same placement with no en-passant square.

    The e3 en-passant square exists mechanically, but no black pawn can capture
    it, so it must not affect identity.
    """
    board = board_after(["e4"])
    assert board.ep_square == chess.E3
    assert not board.has_legal_en_passant()

    fields = board.fen().split()
    fields[3] = "-"  # strip the en-passant square from an identical placement
    board_without_ep = chess.Board(" ".join(fields))

    assert position_key(board) == position_key(board_without_ep)
    # And the canonical display string already drops it.
    assert position_epd(board).split()[3] == "-"


def test_legal_en_passant_changes_identity():
    """A legal en-passant possibility is part of identity.

    After 1.e4 e6 2.e5 d5 the white e5 pawn can capture d6 en passant, so this
    position must differ from the same placement without that possibility.
    """
    board = board_after(["e4", "e6", "e5", "d5"])
    assert board.has_legal_en_passant()
    assert board.ep_square == chess.D6

    fields = board.fen().split()
    fields[3] = "-"
    board_without_ep = chess.Board(" ".join(fields))

    assert position_key(board) != position_key(board_without_ep)


def test_lost_castling_rights_change_identity():
    """A king that walks out and back (losing castling) is a different position.

    Same piece placement, same side to move, but castling rights differ.
    """
    start = chess.Board("4k3/8/8/8/8/8/8/R3K2R w KQ - 0 1")
    key_before = position_key(start)
    placement_before = start.board_fen()

    walked = start.copy()
    for san in ["Ke2", "Ke7", "Ke1", "Ke8"]:  # white king leaves e1 and returns
        walked.push_san(san)

    # Isolate the variable: placement and side to move are unchanged...
    assert walked.board_fen() == placement_before
    assert walked.turn == chess.WHITE
    assert walked.epd().split()[2] == "-"  # ...only castling rights were lost.

    assert position_key(walked) != key_before


# --- Supporting invariants --------------------------------------------------


def test_key_is_signed_int64():
    """Keys fit the signed int64 range FalkorDB stores them in."""
    for sans in ([], ["e4"], ["e4", "e6", "e5", "d5"], ["d4", "d5", "c4"]):
        key = key_after(sans)
        assert _INT64_MIN <= key <= _INT64_MAX


def test_move_counters_are_not_part_of_identity():
    """Halfmove clock and fullmove number never change identity."""
    a = chess.Board("rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR w KQkq - 0 2")
    b = chess.Board("rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR w KQkq - 9 40")
    assert position_key(a) == position_key(b)
    assert normalize_board(a) == normalize_board(b)


def test_color_flipped_positions_differ():
    """Same placement, opposite side to move → different positions."""
    white_to_move = chess.Board("4k3/8/8/8/8/8/8/4K3 w - - 0 1")
    black_to_move = chess.Board("4k3/8/8/8/8/8/8/4K3 b - - 0 1")
    assert position_key(white_to_move) != position_key(black_to_move)


def test_normalize_board_fields():
    """normalize_board derives side/castling consistently with the EPD."""
    pos = normalize_board(board_after(["e4"]))
    assert pos.side == "b"
    assert pos.castling == "KQkq"
    assert pos.epd.startswith("rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR")
    assert pos.epd == position_epd(board_after(["e4"]))


def test_normalize_fen_and_epd_agree():
    """A full FEN and the matching EPD normalize to the same identity."""
    fen = "rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR w KQkq - 0 2"
    epd = "rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR w KQkq -"
    assert normalize_fen(fen) == normalize_epd(epd)
    # round-trip through our EPD builder is stable
    rebuilt = board_from_epd(epd)
    assert position_key(rebuilt) == normalize_fen(fen).key
