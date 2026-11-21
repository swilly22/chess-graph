"""Pawn-structure layer: link positions to their pawns-only structure.

A structure is the board with everything but pawns removed, so players can study
a structure (e.g. Carlsbad) across the different openings that reach it. The
signature is the pawns-only FEN placement field; the key is a stable 64-bit hash
of it (pawns-only boards aren't legal positions, so we hash the signature rather
than Zobrist it).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from chess_graph.config import PipelineConfig
from chess_graph.load.falkor import connect


def pawn_signature(placement: str) -> str:
    """Reduce a FEN placement field to pawns only, re-compressing empties."""
    ranks = []
    for rank in placement.split("/"):
        out: list[str] = []
        empty = 0
        for ch in rank:
            if ch in "Pp":
                if empty:
                    out.append(str(empty))
                    empty = 0
                out.append(ch)
            elif ch.isdigit():
                empty += int(ch)
            else:  # any non-pawn piece -> empty square
                empty += 1
        if empty:
            out.append(str(empty))
        ranks.append("".join(out))
    return "/".join(ranks)


def structure_key(signature: str) -> int:
    """Stable signed int64 key for a pawn signature."""
    digest = hashlib.blake2b(signature.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)


@dataclass(slots=True)
class StructureStats:
    positions: int
    structures: int
    edges: int


def _create_schema(graph) -> None:
    graph.create_node_range_index("PawnStructure", "key")
    try:
        graph.create_node_unique_constraint("PawnStructure", "key")
    except Exception:
        pass  # already exists


def enrich_structures(cfg: PipelineConfig, batch: int | None = None) -> StructureStats:
    graph = connect(cfg)
    batch = batch or cfg.batch_rows

    rows = graph.query("MATCH (p:Position) RETURN p.key, p.epd, p.games").result_set

    # aggregate structures and collect position->structure links
    structs: dict[int, dict] = {}
    links: list[dict] = []
    for key, epd, games in rows:
        placement = epd.split(" ", 1)[0]
        sig = pawn_signature(placement)
        skey = structure_key(sig)
        s = structs.get(skey)
        if s is None:
            s = {"key": skey, "signature": sig, "positions": 0, "games": 0}
            structs[skey] = s
        s["positions"] += 1
        s["games"] += games
        links.append({"pk": key, "sk": skey})

    _create_schema(graph)

    struct_rows = list(structs.values())
    create_s = (
        "UNWIND $rows AS r CREATE (:PawnStructure "
        "{key: r.key, signature: r.signature, positions: r.positions, games: r.games})"
    )
    for i in range(0, len(struct_rows), batch):
        graph.query(create_s, {"rows": struct_rows[i : i + batch]})

    create_e = (
        "UNWIND $rows AS r MATCH (p:Position {key: r.pk}), (s:PawnStructure {key: r.sk}) "
        "CREATE (p)-[:HAS_STRUCTURE]->(s)"
    )
    for i in range(0, len(links), batch):
        graph.query(create_e, {"rows": links[i : i + batch]})

    return StructureStats(positions=len(rows), structures=len(structs), edges=len(links))
