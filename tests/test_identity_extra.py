"""Additional identity invariants beyond the five required cases."""

from __future__ import annotations

import chess

from chess_graph.identity import (
    Position,
    board_from_epd,
    board_from_fen,
    normalize_board,
    position_key,
)


def _board_after(sans: list[str]) -> chess.Board:
    board = chess.Board()
    for san in sans:
        board.push_san(san)
    return board


def test_key_is_deterministic():
    b1 = _board_after(["e4", "c5", "Nf3"])
    b2 = _board_after(["e4", "c5", "Nf3"])
    assert position_key(b1) == position_key(b2)
    assert position_key(b1) == position_key(b1)  # stable across repeated calls


def test_four_move_orders_reach_one_key():
    # The same Queen's Gambit Declined-ish position via four move orders.
    orders = [
        ["d4", "d5", "c4", "e6"],
        ["d4", "e6", "c4", "d5"],
        ["c4", "e6", "d4", "d5"],
        ["c4", "d5", "d4", "e6"],
    ]
    keys = {position_key(_board_after(o)) for o in orders}
    assert len(keys) == 1


def test_board_from_fen_round_trip():
    fen = "r1bqkbnr/pppp1ppp/2n5/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R w KQkq - 2 3"
    board = board_from_fen(fen)
    assert board.fen() == fen
    assert position_key(board) == position_key(_board_after(["e4", "e5", "Nf3", "Nc6"]))


def test_board_from_epd_round_trip():
    epd = "rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR w KQkq -"
    board = board_from_epd(epd)
    assert board.epd() == epd
    assert position_key(board) == position_key(_board_after(["e4", "c5"]))


def test_castling_lost_via_rook_move_changes_key():
    # Move the a1 rook out and back: queenside castling right is gone.
    start = chess.Board("r3k2r/8/8/8/8/8/8/R3K2R w KQkq - 0 1")
    key_before = position_key(start)
    placement = start.board_fen()

    walked = start.copy()
    for san in ["Ra2", "Ra7", "Ra1", "Ra8"]:
        walked.push_san(san)

    assert walked.board_fen() == placement
    assert walked.turn == chess.WHITE
    # White lost Q (a1 rook moved); black lost q (a8 rook moved). K/k remain.
    assert walked.epd().split()[2] == "Kk"
    assert position_key(walked) != key_before


def test_promotions_to_different_pieces_differ():
    base = "8/P6k/8/8/8/8/8/7K w - - 0 1"
    queen = chess.Board(base)
    queen.push_san("a8=Q")
    knight = chess.Board(base)
    knight.push_san("a8=N")
    assert position_key(queen) != position_key(knight)


def test_black_en_passant_is_part_of_identity():
    # 1.Nf3 d5 2.Nd4 e5? no — build a black-capturable ep: 1.d4 e5? Use a clean line.
    # 1. e4 Nf6 2. e5 d5  -> white e5 pawn, black just played d5; white can take d6 ep.
    # For a BLACK ep: 1. Nf3 e5 2. d4?! e4 3. d5? no. Use explicit FEN instead.
    # Black to move, black pawn on d4, white just played e2-e4 -> black d4 takes e3 ep.
    board = chess.Board("4k3/8/8/8/3pP3/8/8/4K3 b - e3 0 1")
    assert board.has_legal_en_passant()
    key_with_ep = position_key(board)
    no_ep = chess.Board("4k3/8/8/8/3pP3/8/8/4K3 b - - 0 1")
    assert key_with_ep != position_key(no_ep)


def test_position_dataclass_is_frozen_and_hashable():
    pos = normalize_board(chess.Board())
    assert isinstance(hash(pos), int)
    # frozen -> equality by value; two normalizations of the start are equal
    assert pos == normalize_board(chess.Board())
    assert {pos, normalize_board(chess.Board())} == {pos}


def test_normalize_board_does_not_mutate():
    board = _board_after(["e4", "c5"])
    before = board.fen()
    normalize_board(board)
    assert board.fen() == before


def test_side_field_tracks_turn():
    assert normalize_board(chess.Board()).side == "w"
    assert normalize_board(_board_after(["e4"])).side == "b"
    assert Position(key=1, epd="x w - -", side="w", castling="-").side == "w"
