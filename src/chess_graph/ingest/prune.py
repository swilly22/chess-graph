"""Prune the aggregate down to what we load.

Keep positions reached by at least ``min_games_per_position`` games, then keep
only edges whose *both* endpoints survived. (``min_ply`` and any ply-cap based
on the shortest route from the start are computed in phase 3 enrichment, not
here — see CLAUDE.md.)
"""

from __future__ import annotations

from dataclasses import dataclass

from chess_graph.ingest.aggregate import Aggregator, EdgeAgg


@dataclass(slots=True)
class PruneResult:
    survivors: set[int]  # surviving position keys
    edges: dict[tuple[int, int], EdgeAgg]
    positions_before: int
    positions_after: int
    edges_before: int
    edges_after: int


def prune(agg: Aggregator, min_games: int) -> PruneResult:
    survivors = {key for key, pos in agg.positions.items() if pos.games >= min_games}
    kept_edges = {
        edge_key: edge
        for edge_key, edge in agg.edges.items()
        if edge_key[0] in survivors and edge_key[1] in survivors
    }
    return PruneResult(
        survivors=survivors,
        edges=kept_edges,
        positions_before=len(agg.positions),
        positions_after=len(survivors),
        edges_before=len(agg.edges),
        edges_after=len(kept_edges),
    )
