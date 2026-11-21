"""Parse complete games (all phases) for the puzzle-mining corpus.

Unlike the opening aggregator, this keeps the *entire* move list of each game
plus per-move Lichess ``[%eval]`` annotations when present (free engine evals,
so the puzzle generator can skip Stockfish on those). Games are filtered by the
exact same predicate as the opening sample (`pgn_stream.classify`), so the two
corpora cover the same population.
"""

from __future__ import annotations

import datetime
import re
from dataclasses import dataclass

import chess

from chess_graph.config import PipelineConfig
from chess_graph.ingest.pgn_stream import AcceptedGame, IngestCounters, classify

_RESULT_TOKENS = frozenset({"1-0", "0-1", "1/2-1/2", "*"})
_MOVE_NUMBER_RE = re.compile(r"^\d+\.+")
_VARIATION_RE = re.compile(r"\([^()]*\)")
_TOKEN_OR_COMMENT_RE = re.compile(r"\{([^}]*)\}|(\S+)")
_EVAL_RE = re.compile(r"\[%eval\s+([^\]]+)\]")

# Mate sentinel (matches SCHEMA.md / eval_cp convention): White mating in N is
# +(100000 - N), Black mating in N is -(100000 - N). Shorter mates outrank
# longer ones; any mate outranks any material score.
_MATE_BASE = 100000


@dataclass(slots=True)
class FullGame:
    """One complete game, ready to serialize into the corpus."""

    game_id: str
    source: str
    white_elo: int
    black_elo: int
    bracket: int
    tc: str
    result: str  # "w" / "d" / "b"
    termination: str
    eco: str
    opening: str
    played_at: int  # epoch seconds, 0 if unknown
    ply_count: int
    moves_uci: list[str]
    evals_cp: list[int | None]  # aligned to moves_uci; empty when has_eval is False
    has_eval: bool
    truncated: bool  # a SAN failed to parse; prefix kept


def eval_to_cp(token: str) -> int | None:
    """Convert a Lichess ``[%eval]`` value to our signed centipawn integer."""
    token = token.strip()
    if token.startswith("#"):
        try:
            n = int(token[1:])
        except ValueError:
            return None
        # #5 -> White mates in 5 -> +(100000-5); #-5 -> Black -> -(100000-5).
        return (_MATE_BASE - n) if n >= 0 else -(_MATE_BASE + n)
    try:
        return round(float(token) * 100)
    except ValueError:
        return None


def iter_moves_with_eval(movetext: str) -> list[list]:
    """Return ``[[san, eval_cp_or_None], ...]`` for the whole game (no ply cap)."""
    s = movetext
    if "(" in s:  # strip variations defensively (absent in Lichess mainline)
        prev = None
        while "(" in s and s != prev:
            prev = s
            s = _VARIATION_RE.sub(" ", s)

    moves: list[list] = []
    for match in _TOKEN_OR_COMMENT_RE.finditer(s):
        comment, tok = match.group(1), match.group(2)
        if comment is not None:
            if moves:  # attach the eval to the move it annotates
                em = _EVAL_RE.search(comment)
                if em:
                    cp = eval_to_cp(em.group(1))
                    if cp is not None:
                        moves[-1][1] = cp
            continue
        if tok in _RESULT_TOKENS:
            break
        if tok.startswith("$"):  # NAG
            continue
        tok = _MOVE_NUMBER_RE.sub("", tok)
        tok = tok.rstrip("!?")
        if tok:
            moves.append([tok, None])
    return moves


def _game_id(headers: dict[str, str]) -> str:
    site = headers.get("Site", "")
    return site.rstrip("/").rsplit("/", 1)[-1] if site else ""


def _played_at(headers: dict[str, str]) -> int:
    date = headers.get("UTCDate")
    if not date:
        return 0
    try:
        y, mo, da = (int(x) for x in date.split("."))
        hh = mm = ss = 0
        if headers.get("UTCTime"):
            hh, mm, ss = (int(x) for x in headers["UTCTime"].split(":"))
        tz = datetime.UTC
        return int(datetime.datetime(y, mo, da, hh, mm, ss, tzinfo=tz).timestamp())
    except (ValueError, IndexError):
        return 0


def build_full_game(headers: dict[str, str], accepted: AcceptedGame) -> FullGame:
    """Build a FullGame from an already-accepted game (the expensive replay half).

    Split out from :func:`parse_full_game` so the cheap header filter can run in
    the reader process while this move replay runs in worker processes.
    """
    board = chess.Board()
    uci: list[str] = []
    evals: list[int | None] = []
    has_eval = False
    truncated = False
    for san, ev in iter_moves_with_eval(accepted.movetext):
        try:
            move = board.parse_san(san)
        except (ValueError, AssertionError):
            truncated = True
            break
        uci.append(move.uci())
        board.push(move)
        evals.append(ev)
        if ev is not None:
            has_eval = True

    return FullGame(
        game_id=_game_id(headers),
        source="lichess",
        white_elo=int(headers["WhiteElo"]),
        black_elo=int(headers["BlackElo"]),
        bracket=accepted.bracket,
        tc=accepted.tc,
        result=accepted.result,
        termination=headers.get("Termination", ""),
        eco=headers.get("ECO", ""),
        opening=headers.get("Opening", ""),
        played_at=_played_at(headers),
        ply_count=len(uci),
        moves_uci=uci,
        evals_cp=evals if has_eval else [],
        has_eval=has_eval,
        truncated=truncated,
    )


def parse_full_game(
    headers: dict[str, str], movetext: str, cfg: PipelineConfig, counters: IngestCounters
) -> FullGame | None:
    """Apply the sample filters, then extract the whole game. ``None`` if filtered."""
    accepted = classify(headers, movetext, cfg, counters)
    if accepted is None:
        return None
    return build_full_game(headers, accepted)
