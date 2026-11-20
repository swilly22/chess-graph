"""Replay accepted games and aggregate into Position and MOVE structures.

For the sample this holds everything in memory (per CLAUDE.md: "in memory for
samples; DuckDB or Parquet partial aggregates at full scale"). The aggregation
guarantees the invariants that a DB constraint can't:

* one entry per position key → one node;
* one entry per ``(from_key, to_key)`` → one MOVE edge, with per-game data
  summed in;
* within a single game, a position or edge reached twice (a repetition that
  transposes back) is counted once.
"""

from __future__ import annotations

import re
from collections.abc import Iterator

import chess

from chess_graph.config import PipelineConfig
from chess_graph.identity import position_key
from chess_graph.ingest.pgn_stream import AcceptedGame

_RESULT_TOKENS = frozenset({"1-0", "0-1", "1/2-1/2", "*"})
_MOVE_NUMBER_RE = re.compile(r"^\d+\.+")
_COMMENT_RE = re.compile(r"\{[^}]*\}")
_VARIATION_RE = re.compile(r"\([^()]*\)")


def iter_san_moves(movetext: str, max_plies: int) -> Iterator[str]:
    """Yield up to ``max_plies`` SAN move tokens from raw movetext.

    Strips ``{...}`` comments, ``(...)`` variations (absent in Lichess mainline
    dumps, handled defensively), move numbers, NAGs, and the result token.
    """
    s = _COMMENT_RE.sub(" ", movetext)
    if "(" in s:
        prev = None
        while "(" in s and s != prev:
            prev = s
            s = _VARIATION_RE.sub(" ", s)

    count = 0
    for tok in s.split():
        if tok in _RESULT_TOKENS:
            return
        if tok.startswith("$"):  # NAG
            continue
        tok = _MOVE_NUMBER_RE.sub("", tok)  # strip "1." / "1..." / glued "1.e4"
        # Strip glued annotation NAGs: "h6?", "Qe2?!", "Qb5??", "Qf6+?".
        # python-chess rejects these, and no legal SAN ends in '!' or '?'.
        tok = tok.rstrip("!?")
        if not tok:
            continue
        yield tok
        count += 1
        if count >= max_plies:
            return


class PosAgg:
    """Accumulated stats for one position."""

    __slots__ = ("epd", "games", "reach_blitz", "reach_slow")

    def __init__(self, epd: str) -> None:
        self.epd = epd
        self.games = 0
        self.reach_blitz = [0] * 6
        self.reach_slow = [0] * 6


class EdgeAgg:
    """Accumulated stats for one MOVE edge."""

    __slots__ = (
        "uci", "san", "first_seen", "games",
        "blitz_w", "blitz_d", "blitz_b", "slow_w", "slow_d", "slow_b",
    )

    def __init__(self, uci: str, san: str, first_seen: int) -> None:
        self.uci = uci
        self.san = san
        self.first_seen = first_seen
        self.games = 0
        self.blitz_w = [0] * 6
        self.blitz_d = [0] * 6
        self.blitz_b = [0] * 6
        self.slow_w = [0] * 6
        self.slow_d = [0] * 6
        self.slow_b = [0] * 6


class Aggregator:
    """Builds the in-memory position and edge maps from accepted games."""

    def __init__(self, cfg: PipelineConfig, first_seen: int) -> None:
        self.cfg = cfg
        self.first_seen = first_seen  # yyyymm
        self.positions: dict[int, PosAgg] = {}
        self.edges: dict[tuple[int, int], EdgeAgg] = {}
        self.games_aggregated = 0
        self.san_errors = 0

    def add_game(self, game: AcceptedGame) -> None:
        board = chess.Board()
        from_key = position_key(board)
        self._touch_position(from_key, board, game)
        visited_pos = {from_key}
        visited_edge: set[tuple[int, int]] = set()

        for san in iter_san_moves(game.movetext, self.cfg.depth_cap_plies):
            try:
                move = board.parse_san(san)
            except (ValueError, AssertionError):
                # Malformed / illegal token: keep the valid prefix, drop the rest.
                self.san_errors += 1
                break
            uci = move.uci()
            board.push(move)
            to_key = position_key(board)

            if to_key not in visited_pos:
                self._touch_position(to_key, board, game)
                visited_pos.add(to_key)

            edge_key = (from_key, to_key)
            if edge_key not in visited_edge:
                self._touch_edge(edge_key, uci, san, game)
                visited_edge.add(edge_key)

            from_key = to_key

        self.games_aggregated += 1

    def _touch_position(self, key: int, board: chess.Board, game: AcceptedGame) -> None:
        agg = self.positions.get(key)
        if agg is None:
            agg = PosAgg(board.epd())
            self.positions[key] = agg
        agg.games += 1
        if game.tc == "blitz":
            agg.reach_blitz[game.bracket] += 1
        else:
            agg.reach_slow[game.bracket] += 1

    def _touch_edge(
        self, edge_key: tuple[int, int], uci: str, san: str, game: AcceptedGame
    ) -> None:
        agg = self.edges.get(edge_key)
        if agg is None:
            agg = EdgeAgg(uci, san, self.first_seen)
            self.edges[edge_key] = agg
        agg.games += 1
        if game.tc == "blitz":
            w, d, b = agg.blitz_w, agg.blitz_d, agg.blitz_b
        else:
            w, d, b = agg.slow_w, agg.slow_d, agg.slow_b
        if game.result == "w":
            w[game.bracket] += 1
        elif game.result == "d":
            d[game.bracket] += 1
        else:
            b[game.bracket] += 1
