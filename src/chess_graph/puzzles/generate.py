"""Generate opening-phase puzzles from the enriched core graph.

The enrichment already ran Stockfish MultiPV=2, so each position carries
``best_uci`` and ``best_gap_cp`` (the gap to the second-best move) — which is the
multi-PV uniqueness signal CLAUDE.md wants, so the clearest puzzles come straight
from the graph:

* ``only_move`` — a position whose best move is far better than any other
  (``best_gap_cp`` large): find the one move that holds.
* ``punish`` — a frequently played blunder (big negative ``eval_delta_cp``) that
  has a clear refutation: from the position after it, find the punishment.

For each puzzle, ``find_rate`` (per bracket) is how often players at that level
actually played the solution — low = hard — and seeds a Glicko rating.
"""

from __future__ import annotations

from dataclasses import dataclass

from chess_graph.config import PipelineConfig
from chess_graph.load.falkor import connect

GENERATOR_VERSION = "v1"
_MATE = 90000  # below this, gaps are material (mate gaps are not puzzle-useful as "cp")


def find_rate(sol_blitz: list[int] | None, reach_blitz: list[int]) -> list[float]:
    """Per-bracket rate at which the solution move was actually played."""
    if not sol_blitz:
        return [0.0] * 6
    out = []
    for played, reached in zip(sol_blitz, reach_blitz, strict=True):
        out.append(round(played / reached, 4) if reached else 0.0)
    return out


def overall_find_rate(sol_blitz: list[int] | None, reach_blitz: list[int]) -> float:
    reached = sum(reach_blitz)
    if not sol_blitz or not reached:
        return 0.0
    return sum(sol_blitz) / reached


def glicko_seed(rate: float) -> int:
    """Seed rating from how findable the solution is: rare = hard = high rating."""
    return int(round(max(800, min(2400, 800 + (1 - rate) * 1600))))


@dataclass(slots=True)
class PuzzleStats:
    only_move: int
    punish: int
    rating_min: int
    rating_median: int
    rating_max: int


def _sum3(w, d, b) -> list[int] | None:
    if w is None:
        return None
    return [w[i] + d[i] + b[i] for i in range(6)]


def _only_move_rows(graph, min_gap: int, min_games: int, created_at: int) -> list[dict]:
    res = graph.query(
        """
        MATCH (p:Position)
        WHERE p.best_uci IS NOT NULL AND p.best_gap_cp >= $gap AND p.best_gap_cp < $mate
              AND p.games >= $games
        OPTIONAL MATCH (p)-[sol:MOVE {uci: p.best_uci}]->()
        RETURN p.key, p.best_uci, p.best_gap_cp, p.games, p.reach_blitz,
               sol.blitz_w, sol.blitz_d, sol.blitz_b
        """,
        {"gap": min_gap, "mate": _MATE, "games": min_games},
    ).result_set
    rows = []
    for key, best, gap, games, reach, sw, sd, sb in res:
        sol = _sum3(sw, sd, sb)
        rows.append(_build("only_move", key, [best], None, gap, games, reach, sol, created_at))
    return rows


def _punish_rows(
    graph, max_delta: int, min_games: int, punish_gap: int, created_at: int
) -> list[dict]:
    res = graph.query(
        """
        MATCH (a:Position)-[m:MOVE]->(b:Position)
        WHERE m.eval_delta_cp <= $delta AND m.games >= $games
              AND b.best_uci IS NOT NULL AND b.best_gap_cp >= $pgap AND b.best_gap_cp < $mate
        OPTIONAL MATCH (b)-[sol:MOVE {uci: b.best_uci}]->()
        RETURN b.key, b.best_uci, b.best_gap_cp, b.games, b.reach_blitz,
               m.uci, m.games, sol.blitz_w, sol.blitz_d, sol.blitz_b
        """,
        {"delta": max_delta, "games": min_games, "pgap": punish_gap, "mate": _MATE},
    ).result_set
    # one puzzle per setup position: keep the most-played blunder leading to it
    best_by_key: dict[int, tuple] = {}
    for row in res:
        key, mgames = row[0], row[6]
        if key not in best_by_key or mgames > best_by_key[key][6]:
            best_by_key[key] = row
    rows = []
    for key, best, gap, games, reach, prev_uci, _mg, sw, sd, sb in best_by_key.values():
        sol = _sum3(sw, sd, sb)
        rows.append(_build("punish", key, [best], prev_uci, gap, games, reach, sol, created_at))
    return rows


def _build(ptype, key, solution, prev_uci, gap, games, reach, sol_blitz, created_at) -> dict:
    fr = find_rate(sol_blitz, reach)
    rating = glicko_seed(overall_find_rate(sol_blitz, reach))
    pid = f"{ptype}:{key}" if prev_uci is None else f"{ptype}:{key}:{prev_uci}"
    return {
        "id": pid, "type": ptype, "solution": solution, "prev_uci": prev_uci,
        "tc": "blitz", "find_rate": fr, "frequency": games,  # raw reach; normalized below
        "eval_gap_cp": gap, "rating": rating, "rating_dev": 350.0, "attempts": 0,
        "generator_version": GENERATOR_VERSION, "created_at": created_at,
        "setup_key": key,
    }


def _create_schema(graph) -> None:
    graph.create_node_range_index("Puzzle", "id")
    graph.create_node_range_index("Puzzle", "type", "rating")
    try:
        graph.create_node_unique_constraint("Puzzle", "id")
    except Exception:
        pass


def generate_puzzles(
    cfg: PipelineConfig,
    created_at: int,
    only_move_gap: int = 150,
    punish_delta: int = -150,
    punish_gap: int = 100,
    min_games: int = 50,
) -> PuzzleStats:
    graph = connect(cfg)
    start_games = graph.query(
        "MATCH (p:Position {min_ply: 0}) RETURN p.games"
    ).result_set[0][0]

    rows = _only_move_rows(graph, only_move_gap, min_games, created_at)
    only_n = len(rows)
    rows += _punish_rows(graph, punish_delta, min_games, punish_gap, created_at)

    # normalize frequency to a share of all games
    for r in rows:
        r["frequency"] = round(r["frequency"] / start_games, 6)

    _create_schema(graph)
    create_z = (
        "UNWIND $rows AS r CREATE (:Puzzle {id: r.id, type: r.type, solution: r.solution, "
        "prev_uci: r.prev_uci, tc: r.tc, find_rate: r.find_rate, frequency: r.frequency, "
        "eval_gap_cp: r.eval_gap_cp, rating: r.rating, rating_dev: r.rating_dev, "
        "attempts: r.attempts, generator_version: r.generator_version, created_at: r.created_at})"
    )
    link = (
        "UNWIND $rows AS r MATCH (z:Puzzle {id: r.id}), (p:Position {key: r.setup_key}) "
        "CREATE (z)-[:SETUP_AT]->(p)"
    )
    for i in range(0, len(rows), cfg.batch_rows):
        chunk = rows[i : i + cfg.batch_rows]
        graph.query(create_z, {"rows": chunk})
        graph.query(link, {"rows": chunk})

    ratings = sorted(r["rating"] for r in rows)
    return PuzzleStats(
        only_move=only_n,
        punish=len(rows) - only_n,
        rating_min=ratings[0] if ratings else 0,
        rating_median=ratings[len(ratings) // 2] if ratings else 0,
        rating_max=ratings[-1] if ratings else 0,
    )
