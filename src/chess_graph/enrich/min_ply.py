"""Compute ``min_ply`` — the shortest route from the start position.

CLAUDE.md requires the ply cap to be applied using ``min_ply`` (the shortest
route from the start), never the ply at which one particular game reached a
position. Every MOVE edge advances exactly one ply, so a breadth-first pass from
the start position over the MOVE graph gives the shortest distance directly.

Note: pruning keeps edges only when *both* endpoints survive, so a surviving
position whose shortest-game predecessor was pruned can be unreachable in the
pruned graph. Those are reported as orphans and left with ``min_ply`` unset.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

import chess

from chess_graph.config import PipelineConfig
from chess_graph.identity import position_key
from chess_graph.load.falkor import connect


def bfs_min_ply(edges: list[tuple[int, int]], start: int) -> dict[int, int]:
    """Shortest directed distance (in plies) from ``start`` to each node."""
    adj: dict[int, list[int]] = {}
    for a, b in edges:
        adj.setdefault(a, []).append(b)

    dist = {start: 0}
    queue = deque([start])
    while queue:
        u = queue.popleft()
        for v in adj.get(u, ()):
            if v not in dist:
                dist[v] = dist[u] + 1
                queue.append(v)
    return dist


@dataclass(slots=True)
class MinPlyStats:
    total_positions: int
    reachable: int
    orphans: int
    max_min_ply: int | None


def _fetch_edges(graph) -> list[tuple[int, int]]:
    res = graph.query("MATCH (a:Position)-[:MOVE]->(b:Position) RETURN a.key, b.key")
    return [(row[0], row[1]) for row in res.result_set]


def _write_min_ply(graph, dist: dict[int, int], batch: int) -> None:
    rows = [{"k": k, "mp": v} for k, v in dist.items()]
    query = "UNWIND $rows AS r MATCH (p:Position {key: r.k}) SET p.min_ply = r.mp"
    for i in range(0, len(rows), batch):
        graph.query(query, {"rows": rows[i : i + batch]})


def enrich_min_ply(cfg: PipelineConfig) -> MinPlyStats:
    graph = connect(cfg)
    start = position_key(chess.Board())
    dist = bfs_min_ply(_fetch_edges(graph), start)
    _write_min_ply(graph, dist, cfg.batch_rows)

    total = graph.query("MATCH (p:Position) RETURN count(p)").result_set[0][0]
    return MinPlyStats(
        total_positions=total,
        reachable=len(dist),
        orphans=total - len(dist),
        max_min_ply=max(dist.values()) if dist else None,
    )
