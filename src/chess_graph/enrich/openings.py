"""Opening names: Opening nodes, NAMED edges, and the SUBVARIATION_OF tree.

Source: the lichess-org/chess-openings TSVs (eco, name, pgn). Each opening's
canonical line is replayed and normalized through the identity function, so a
NAMED edge attaches to the exact final position in the core graph — when that
position survived pruning. SUBVARIATION_OF links an opening to the longest other
opening whose move sequence is a prefix of its own (the opening tree).
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

import chess

from chess_graph.config import PipelineConfig
from chess_graph.identity import normalize_board
from chess_graph.ingest.aggregate import iter_san_moves
from chess_graph.load.falkor import connect

_FILES = ["a.tsv", "b.tsv", "c.tsv", "d.tsv", "e.tsv"]


@dataclass(slots=True)
class Opening:
    name: str
    eco: str
    family: str
    variation: str | None
    pgn: str
    ply: int
    key: int
    epd: str
    uci_seq: tuple[str, ...]


def parse_opening(eco: str, name: str, pgn: str) -> Opening:
    board = chess.Board()
    uci: list[str] = []
    for san in iter_san_moves(pgn, 100):
        move = board.parse_san(san)
        uci.append(move.uci())
        board.push(move)
    pos = normalize_board(board)
    family, sep, variation = name.partition(":")
    return Opening(
        name=name, eco=eco, family=family.strip(),
        variation=variation.strip() if sep else None,
        pgn=pgn, ply=len(uci), key=pos.key, epd=pos.epd, uci_seq=tuple(uci),
    )


def load_tsvs(dirpath: str | Path) -> list[Opening]:
    out: list[Opening] = []
    seen: set[str] = set()
    for fname in _FILES:
        path = Path(dirpath) / fname
        if not path.exists():
            continue
        with open(path, newline="") as fh:
            reader = csv.reader(fh, delimiter="\t")
            next(reader, None)  # header
            for row in reader:
                if len(row) < 3 or row[1] in seen:
                    continue
                seen.add(row[1])
                out.append(parse_opening(row[0], row[1], row[2]))
    return out


def compute_parents(openings: list[Opening]) -> dict[str, str]:
    """Map each opening to its parent (longest other opening that is a prefix)."""
    seq_to_name = {o.uci_seq: o.name for o in openings}
    parents: dict[str, str] = {}
    for o in openings:
        for length in range(len(o.uci_seq) - 1, 0, -1):
            parent = seq_to_name.get(o.uci_seq[:length])
            if parent is not None and parent != o.name:
                parents[o.name] = parent
                break
    return parents


@dataclass(slots=True)
class OpeningStats:
    openings: int
    named_edges: int
    subvariation_edges: int


def enrich_openings(cfg: PipelineConfig, dirpath: str = "data/openings") -> OpeningStats:
    openings = load_tsvs(dirpath)
    if not openings:
        raise SystemExit(f"no opening TSVs in {dirpath} (a.tsv..e.tsv)")
    parents = compute_parents(openings)

    graph = connect(cfg)
    graph.create_node_range_index("Opening", "name")
    graph.create_node_range_index("Opening", "eco")
    try:
        graph.create_node_unique_constraint("Opening", "name")
    except Exception:
        pass

    batch = cfg.batch_rows
    node_rows = [
        {"name": o.name, "eco": o.eco, "family": o.family,
         "variation": o.variation, "pgn": o.pgn, "ply": o.ply}
        for o in openings
    ]
    create_nodes = (
        "UNWIND $rows AS r CREATE (:Opening {name: r.name, eco: r.eco, "
        "family: r.family, variation: r.variation, pgn: r.pgn, ply: r.ply})"
    )
    for i in range(0, len(node_rows), batch):
        graph.query(create_nodes, {"rows": node_rows[i : i + batch]})

    # NAMED only attaches where the final position exists in the core graph.
    named_rows = [{"key": o.key, "name": o.name} for o in openings]
    create_named = (
        "UNWIND $rows AS r MATCH (p:Position {key: r.key}), (o:Opening {name: r.name}) "
        "CREATE (p)-[:NAMED]->(o)"
    )
    for i in range(0, len(named_rows), batch):
        graph.query(create_named, {"rows": named_rows[i : i + batch]})

    sub_rows = [{"child": c, "parent": p} for c, p in parents.items()]
    create_sub = (
        "UNWIND $rows AS r MATCH (c:Opening {name: r.child}), (p:Opening {name: r.parent}) "
        "CREATE (c)-[:SUBVARIATION_OF]->(p)"
    )
    for i in range(0, len(sub_rows), batch):
        graph.query(create_sub, {"rows": sub_rows[i : i + batch]})

    named = graph.query("MATCH (:Position)-[:NAMED]->() RETURN count(*)").result_set[0][0]
    sub = graph.query("MATCH (:Opening)-[:SUBVARIATION_OF]->() RETURN count(*)").result_set[0][0]
    return OpeningStats(openings=len(openings), named_edges=named, subvariation_edges=sub)
