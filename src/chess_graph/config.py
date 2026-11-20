"""Load pipeline thresholds from ``config/pipeline.toml``.

Thresholds belong in config, not code (see CLAUDE.md "Working agreements").
This module is the only place that reads the TOML; everything else takes a
:class:`PipelineConfig`.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

# repo_root/src/chess_graph/config.py -> repo_root
_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = _REPO_ROOT / "config" / "pipeline.toml"


@dataclass(frozen=True, slots=True)
class TimeControl:
    increment_multiplier: int
    bullet_max_seconds: int
    blitz_max_seconds: int

    def classify(self, base_seconds: int, increment_seconds: int) -> str | None:
        """Return ``"blitz"``, ``"slow"``, or ``None`` when the game is excluded.

        Estimated duration = ``base + increment_multiplier * increment``. Below
        ``bullet_max_seconds`` is bullet (excluded → ``None``), below
        ``blitz_max_seconds`` is blitz, otherwise slow (rapid + classical).
        """
        est = base_seconds + self.increment_multiplier * increment_seconds
        if est < self.bullet_max_seconds:
            return None
        if est < self.blitz_max_seconds:
            return "blitz"
        return "slow"


@dataclass(frozen=True, slots=True)
class RatingBrackets:
    edges: tuple[int, ...]  # lower bounds of brackets 1..5
    labels: tuple[str, ...]  # length 6

    def bucket(self, avg_rating: float) -> int:
        """Index 0..5 of the bracket an average rating falls into."""
        idx = 0
        for edge in self.edges:
            if avg_rating < edge:
                break
            idx += 1
        return idx


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    depth_cap_plies: int
    max_rating_diff: int
    rated_only: bool
    variant: str
    time_control: TimeControl
    brackets: RatingBrackets
    min_games_per_position: int
    sample_target_games: int
    falkordb_host: str
    falkordb_port: int
    core_graph: str
    batch_rows: int


def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> PipelineConfig:
    with open(path, "rb") as fh:
        raw = tomllib.load(fh)

    ing = raw["ingestion"]
    tc = raw["time_control"]
    rb = raw["rating_brackets"]
    pr = raw["pruning"]
    sm = raw["sample"]
    fdb = raw["falkordb"]

    brackets = RatingBrackets(edges=tuple(rb["edges"]), labels=tuple(rb["labels"]))
    if len(brackets.labels) != 6 or len(brackets.edges) != 5:
        raise ValueError(
            "rating_brackets must have 6 labels and 5 edges; "
            f"got {len(brackets.labels)} labels, {len(brackets.edges)} edges"
        )

    return PipelineConfig(
        depth_cap_plies=ing["depth_cap_plies"],
        max_rating_diff=ing["max_rating_diff"],
        rated_only=ing["rated_only"],
        variant=ing["variant"],
        time_control=TimeControl(
            increment_multiplier=tc["increment_multiplier"],
            bullet_max_seconds=tc["bullet_max_seconds"],
            blitz_max_seconds=tc["blitz_max_seconds"],
        ),
        brackets=brackets,
        min_games_per_position=pr["min_games_per_position"],
        sample_target_games=sm["target_games"],
        falkordb_host=fdb["host"],
        falkordb_port=fdb["port"],
        core_graph=fdb["core_graph"],
        batch_rows=fdb["batch_rows"],
    )
