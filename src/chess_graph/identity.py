"""Position identity — the single source of truth for "which node is this?".

Every position in the graph must map to exactly one node. A duplicate node
silently breaks a transposition, so *every* source of positions (game imports,
the eval DB, openings, puzzles) must pass through this module before anything is
matched or merged on a key.

Identity rules (see CLAUDE.md "Position identity rules"):

* **Key** — ``chess.polyglot.zobrist_hash(board)`` folded into a signed int64.
  This is the node identity. Two positions are the same node iff their keys
  match.
* **Display string** — ``board.epd()`` with default arguments. Drops move
  counters; keeps the en-passant square only when a legal en-passant capture
  exists.
* **Same position** means identical piece placement, side to move, castling
  rights, and legal en-passant possibility. Move counters are never part of
  identity. Castling rights are. Color-flipped positions are different.

Both the key and the EPD get their en-passant handling from python-chess, which
only encodes the en-passant square when a capture is actually available — so
``1.e4`` hashes the same as the identical placement with no en-passant square.
"""

from __future__ import annotations

from dataclasses import dataclass

import chess
import chess.polyglot

_INT64_SIGN_BIT = 1 << 63
_UINT64_MODULUS = 1 << 64


@dataclass(frozen=True, slots=True)
class Position:
    """The canonical identity of a board, ready to load as a ``Position`` node.

    ``key`` is the graph identity; everything else is derived display/query data.
    """

    key: int
    epd: str
    side: str  # "w" or "b"
    castling: str  # e.g. "KQkq" or "-"


def position_key(board: chess.Board) -> int:
    """Return the signed int64 Zobrist key identifying ``board``.

    ``chess.polyglot.zobrist_hash`` returns an unsigned 64-bit int; we fold it
    into the signed int64 range so it round-trips through FalkorDB's integer
    type unchanged.
    """
    h = chess.polyglot.zobrist_hash(board)
    return h - _UINT64_MODULUS if h >= _INT64_SIGN_BIT else h


def position_epd(board: chess.Board) -> str:
    """Return the canonical display string: ``board.epd()`` with defaults."""
    return board.epd()


def normalize_board(board: chess.Board) -> Position:
    """Collapse a board to its canonical :class:`Position` identity.

    The board is not mutated. Move counters on the board are ignored by both the
    key and the EPD, so boards that differ only in those counters normalize to
    the same :class:`Position`.
    """
    epd = position_epd(board)
    # EPD default fields: "<placement> <side> <castling> <ep>". Derive side and
    # castling from the same canonical string we store, so they can never drift.
    _, side, castling, _ep = epd.split(" ", 3)
    return Position(
        key=position_key(board),
        epd=epd,
        side=side,
        castling=castling,
    )


def board_from_fen(fen: str) -> chess.Board:
    """Build a board from a full FEN (6 fields)."""
    return chess.Board(fen)


def board_from_epd(epd: str) -> chess.Board:
    """Build a board from an EPD string (4 fields + optional opcodes).

    ``chess.Board(fen)`` requires the two move-counter fields, which an EPD
    lacks, so we go through ``set_epd`` instead.
    """
    board = chess.Board()
    board.set_epd(epd)
    return board


def normalize_fen(fen: str) -> Position:
    """Normalize an external full-FEN position (e.g. the eval DB)."""
    return normalize_board(board_from_fen(fen))


def normalize_epd(epd: str) -> Position:
    """Normalize an external EPD position."""
    return normalize_board(board_from_epd(epd))
