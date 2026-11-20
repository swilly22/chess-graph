"""Create the `chess_core` schema and bulk-load Position nodes and MOVE edges.

Load order matters (CLAUDE.md + docs/SCHEMA.md):

1. indexes + constraints first — edges are matched by position key, and the
   unique constraint on ``Position.key`` both needs its range index and proves
   the loader's dedup held;
2. Position nodes via batched ``UNWIND $rows CREATE`` (data is already
   deduplicated, so ``CREATE`` not ``MERGE``);
3. MOVE edges via batched ``UNWIND`` + ``MATCH`` by key + ``CREATE``.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass

from falkordb import FalkorDB, Graph

from chess_graph.config import PipelineConfig
from chess_graph.ingest.aggregate import Aggregator, EdgeAgg, PosAgg

_CREATE_NODES = """
UNWIND $rows AS r
CREATE (p:Position {
  key: r.key, epd: r.epd, side: r.side, castling: r.castling,
  games: r.games, reach_blitz: r.reach_blitz, reach_slow: r.reach_slow
})
"""

_CREATE_EDGES = """
UNWIND $rows AS r
MATCH (a:Position {key: r.a}), (b:Position {key: r.b})
CREATE (a)-[:MOVE {
  uci: r.uci, san: r.san, games: r.games,
  blitz_w: r.bw, blitz_d: r.bd, blitz_b: r.bb,
  slow_w: r.sw, slow_d: r.sd, slow_b: r.sb,
  first_seen: r.fs
}]->(b)
"""


@dataclass(slots=True)
class LoadStats:
    nodes_loaded: int = 0
    edges_loaded: int = 0
    node_seconds: float = 0.0
    edge_seconds: float = 0.0


def connect(cfg: PipelineConfig) -> Graph:
    db = FalkorDB(host=cfg.falkordb_host, port=cfg.falkordb_port)
    return db.select_graph(cfg.core_graph)


def reset_graph(graph: Graph) -> None:
    """Delete the graph if it exists, so a re-run starts clean."""
    try:
        graph.delete()
    except Exception:
        pass  # graph did not exist


def create_schema(graph: Graph, wait_seconds: float = 30.0) -> None:
    """Create the phase-2 indexes and constraints, then wait until active."""
    # Composite index used by the explorer / transposition map (min_ply filled
    # in phase 3; indexing it now is harmless).
    graph.create_node_range_index("Position", "min_ply", "games")
    # Unique key (auto-creates the range index on key) + mandatory props.
    graph.create_node_unique_constraint("Position", "key")
    graph.create_node_mandatory_constraint("Position", "key", "epd")
    graph.create_edge_mandatory_constraint("MOVE", "uci")
    _wait_for_constraints(graph, wait_seconds)


def _wait_for_constraints(graph: Graph, wait_seconds: float) -> None:
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        statuses = [c["status"] for c in graph.list_constraints()]
        if any(s == "FAILED" for s in statuses):
            raise RuntimeError(f"constraint creation FAILED: {graph.list_constraints()}")
        if all(s == "OPERATIONAL" for s in statuses):
            return
        time.sleep(0.2)
    raise TimeoutError(f"constraints not operational within {wait_seconds}s")


def _batched(iterable: Iterator[dict], size: int) -> Iterator[list[dict]]:
    batch: list[dict] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _node_rows(positions: dict[int, PosAgg], survivors: set[int]) -> Iterator[dict]:
    for key in survivors:
        pos = positions[key]
        # EPD default fields: "<placement> <side> <castling> <ep>".
        _, side, castling, _ep = pos.epd.split(" ", 3)
        yield {
            "key": key,
            "epd": pos.epd,
            "side": side,
            "castling": castling,
            "games": pos.games,
            "reach_blitz": pos.reach_blitz,
            "reach_slow": pos.reach_slow,
        }


def _edge_rows(edges: dict[tuple[int, int], EdgeAgg]) -> Iterator[dict]:
    for (a, b), e in edges.items():
        yield {
            "a": a, "b": b,
            "uci": e.uci, "san": e.san, "games": e.games,
            "bw": e.blitz_w, "bd": e.blitz_d, "bb": e.blitz_b,
            "sw": e.slow_w, "sd": e.slow_d, "sb": e.slow_b,
            "fs": e.first_seen,
        }


def load_position_rows(
    graph: Graph, rows_iter: Iterator[dict], batch: int, stats: LoadStats
) -> None:
    """Load Position nodes from an iterator of node-row dicts (any aggregator)."""
    for rows in _batched(rows_iter, batch):
        start = time.monotonic()
        graph.query(_CREATE_NODES, {"rows": rows})
        stats.node_seconds += time.monotonic() - start
        stats.nodes_loaded += len(rows)


def load_move_rows(graph: Graph, rows_iter: Iterator[dict], batch: int, stats: LoadStats) -> None:
    """Load MOVE edges from an iterator of edge-row dicts (any aggregator)."""
    for rows in _batched(rows_iter, batch):
        start = time.monotonic()
        graph.query(_CREATE_EDGES, {"rows": rows})
        stats.edge_seconds += time.monotonic() - start
        stats.edges_loaded += len(rows)


def graph_memory_mb(cfg: PipelineConfig, graph: Graph) -> float | None:
    """Total resident size of the graph in FalkorDB, in MB (``GRAPH.MEMORY``)."""
    try:
        db = FalkorDB(host=cfg.falkordb_host, port=cfg.falkordb_port)
        raw = db.connection.execute_command("GRAPH.MEMORY", "USAGE", graph.name)
        flat = {raw[i]: raw[i + 1] for i in range(0, len(raw) - 1, 2)}
        value = flat.get("total_graph_sz_mb")
        return float(value) if value is not None else None
    except Exception:
        return None


def load_rows(
    cfg: PipelineConfig,
    node_rows: Iterator[dict],
    edge_rows: Iterator[dict],
    reset: bool = True,
) -> tuple[Graph, LoadStats]:
    """Create the schema and load from row iterators (aggregator-agnostic)."""
    graph = connect(cfg)
    if reset:
        reset_graph(graph)
    create_schema(graph)
    stats = LoadStats()
    load_position_rows(graph, node_rows, cfg.batch_rows, stats)
    load_move_rows(graph, edge_rows, cfg.batch_rows, stats)
    return graph, stats


def load_all(
    cfg: PipelineConfig,
    agg: Aggregator,
    survivors: set[int],
    edges: dict[tuple[int, int], EdgeAgg],
    reset: bool = True,
) -> tuple[Graph, LoadStats]:
    """Load from the in-memory aggregator (samples / tests)."""
    return load_rows(cfg, _node_rows(agg.positions, survivors), _edge_rows(edges), reset=reset)
