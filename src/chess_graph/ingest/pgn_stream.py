"""Parse a Lichess PGN line stream into filtered, classified games.

This stays deliberately lightweight: we read headers as ``[Key "Value"]`` lines
and keep the movetext as a raw string. SAN tokenization and board replay happen
in :mod:`chess_graph.ingest.aggregate`, and only up to the ply cap, so we never
pay to parse the tail of long games.

Filters and classification follow CLAUDE.md "Ingestion rules":

* standard variant, rated only;
* time control via ``base + 40*inc`` seconds → drop bullet (<180s), else
  blitz (<480s) or slow;
* skip games whose rating difference exceeds the configured max;
* bracket = average of both ratings, bucketed into the six brackets.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass

from chess_graph.config import PipelineConfig

_HEADER_RE = re.compile(r'^\[([A-Za-z0-9_]+)\s+"(.*)"\]\s*$')
_RESULT_TO_WDB = {"1-0": "w", "1/2-1/2": "d", "0-1": "b"}


@dataclass(slots=True)
class AcceptedGame:
    """A game that passed all filters, ready to replay."""

    tc: str  # "blitz" or "slow"
    bracket: int  # 0..5
    result: str  # "w", "d", or "b"
    movetext: str  # raw SAN movetext (comments still present)


@dataclass
class IngestCounters:
    """Running tallies for the report."""

    scanned: int = 0
    accepted: int = 0
    rejected_variant: int = 0
    rejected_unrated: int = 0
    rejected_rating_missing: int = 0
    rejected_rating_diff: int = 0
    rejected_time_control: int = 0
    rejected_result: int = 0


def iter_raw_games(lines: Iterator[str]) -> Iterator[tuple[dict[str, str], str]]:
    """Yield ``(headers, movetext)`` for each game in a PGN line stream.

    A game is a run of header lines, then movetext lines, terminated by a blank
    line. Lichess always writes at least one movetext line per game.
    """
    headers: dict[str, str] = {}
    movetext_parts: list[str] = []
    in_movetext = False

    for raw in lines:
        line = raw.rstrip("\n")
        if not in_movetext:
            m = _HEADER_RE.match(line)
            if m:
                headers[m.group(1)] = m.group(2)
                continue
            if line == "":
                if headers:  # blank line between headers and movetext
                    in_movetext = True
                continue
            # a non-header, non-blank line is the start of movetext
            in_movetext = True
            movetext_parts.append(line)
        else:
            if line == "":  # blank line ends the game
                if headers or movetext_parts:
                    yield headers, " ".join(movetext_parts)
                headers, movetext_parts, in_movetext = {}, [], False
            else:
                movetext_parts.append(line)

    if headers and movetext_parts:
        yield headers, " ".join(movetext_parts)


def _parse_time_control(tc: str) -> tuple[int, int] | None:
    """Return ``(base_seconds, increment_seconds)`` or ``None`` if unusable.

    ``"-"`` is correspondence: treated as a very long base so it classifies as
    slow.
    """
    if tc == "-":
        return (10**6, 0)
    base, _, inc = tc.partition("+")
    try:
        return (int(base), int(inc) if inc else 0)
    except ValueError:
        return None


def classify(
    headers: dict[str, str], movetext: str, cfg: PipelineConfig, counters: IngestCounters
) -> AcceptedGame | None:
    """Apply filters; return an :class:`AcceptedGame` or ``None`` (and tally)."""
    variant = headers.get("Variant")
    if variant is not None and variant != "Standard":
        counters.rejected_variant += 1
        return None

    if cfg.rated_only and "rated" not in headers.get("Event", "").lower():
        counters.rejected_unrated += 1
        return None

    try:
        white_elo = int(headers["WhiteElo"])
        black_elo = int(headers["BlackElo"])
    except (KeyError, ValueError):
        counters.rejected_rating_missing += 1
        return None

    if abs(white_elo - black_elo) > cfg.max_rating_diff:
        counters.rejected_rating_diff += 1
        return None

    parsed = _parse_time_control(headers.get("TimeControl", "-"))
    if parsed is None:
        counters.rejected_time_control += 1
        return None
    tc = cfg.time_control.classify(*parsed)
    if tc is None:  # bullet / ultrabullet
        counters.rejected_time_control += 1
        return None

    result = _RESULT_TO_WDB.get(headers.get("Result", "*"))
    if result is None:  # unfinished "*"
        counters.rejected_result += 1
        return None

    bracket = cfg.brackets.bucket((white_elo + black_elo) / 2)
    return AcceptedGame(tc=tc, bracket=bracket, result=result, movetext=movetext)


def iter_accepted_games(
    lines: Iterator[str], cfg: PipelineConfig, counters: IngestCounters
) -> Iterator[AcceptedGame]:
    """Stream raw games through the filters, yielding accepted ones."""
    for headers, movetext in iter_raw_games(lines):
        counters.scanned += 1
        game = classify(headers, movetext, cfg, counters)
        if game is not None:
            counters.accepted += 1
            yield game
