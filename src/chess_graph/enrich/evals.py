"""Engine evals via local Stockfish (MultiPV=2) over the core positions.

Stores ``eval_cp`` (White's point of view, mate sentinel ±(100000-N)),
``eval_depth``, ``eval_source``, ``best_uci``, and ``best_gap_cp`` (how much
better the best move is than the second-best, mover's view) on positions; then
derives ``eval_delta_cp`` (mover's view; negative = mistake) and ``is_best`` on
edges from the parent/child evals — no extra engine calls.

One Stockfish process per worker; positions are split across workers.
"""

from __future__ import annotations

import shutil
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

from chess_graph.config import PipelineConfig
from chess_graph.load.falkor import connect

_MATE_BASE = 100000
ONLY_MOVE_GAP = 100000  # best_gap_cp sentinel when there's a single legal move


def score_to_cp(score) -> int:
    """chess.engine.Score (already POV-oriented) -> cp with mate sentinel."""
    if score.is_mate():
        m = score.mate()
        return (_MATE_BASE - m) if m > 0 else -(_MATE_BASE + m)
    return score.score()


def _analyse_chunk(
    positions: list[tuple[int, str]], stockfish: str, depth: int | None, movetime_s: float
) -> list[dict]:
    import chess
    import chess.engine

    engine = chess.engine.SimpleEngine.popen_uci(stockfish)
    engine.configure({"Threads": 1})
    limit = chess.engine.Limit(depth=depth) if depth else chess.engine.Limit(time=movetime_s)
    rows: list[dict] = []
    try:
        for key, epd in positions:
            board = chess.Board()
            board.set_epd(epd)
            if board.is_game_over():
                continue  # terminal: no move / eval to record
            info = engine.analyse(board, limit, multipv=2)
            best = info[0]
            best_pv = best.get("pv") or []
            if not best_pv:
                continue
            eval_cp = score_to_cp(best["score"].white())
            best_uci = best_pv[0].uci()
            # gap in the mover's view (both scores relative to side to move)
            cp1 = score_to_cp(best["score"].pov(board.turn))
            if len(info) >= 2:
                cp2 = score_to_cp(info[1]["score"].pov(board.turn))
                gap = cp1 - cp2
            else:
                gap = ONLY_MOVE_GAP
            rows.append({
                "key": key, "eval_cp": eval_cp, "eval_depth": int(best.get("depth", 0)),
                "best_uci": best_uci, "best_gap_cp": int(gap),
            })
    finally:
        engine.quit()
    return rows


@dataclass(slots=True)
class EvalStats:
    positions_evaluated: int
    edges_updated: int
    avg_depth: float


def _chunks(items: list, n: int) -> list[list]:
    size = (len(items) + n - 1) // n
    return [items[i : i + size] for i in range(0, len(items), size)] or [[]]


def enrich_evals(
    cfg: PipelineConfig,
    depth: int | None = None,
    movetime_ms: int = 120,
    jobs: int = 6,
    limit: int | None = None,
    stockfish: str | None = None,
) -> EvalStats:
    stockfish = stockfish or shutil.which("stockfish")
    if not stockfish:
        raise SystemExit("stockfish not found (brew install stockfish)")

    graph = connect(cfg)
    q = "MATCH (p:Position) RETURN p.key, p.epd"
    if limit:
        q += f" LIMIT {limit}"
    positions = [(row[0], row[1]) for row in graph.query(q).result_set]

    movetime_s = movetime_ms / 1000.0
    rows: list[dict] = []
    with ProcessPoolExecutor(max_workers=jobs) as pool:
        futures = [
            pool.submit(_analyse_chunk, chunk, stockfish, depth, movetime_s)
            for chunk in _chunks(positions, jobs)
            if chunk
        ]
        for f in futures:
            rows.extend(f.result())

    set_eval = (
        "UNWIND $rows AS r MATCH (p:Position {key: r.key}) "
        "SET p.eval_cp = r.eval_cp, p.eval_depth = r.eval_depth, "
        "p.eval_source = 'local', p.best_uci = r.best_uci, p.best_gap_cp = r.best_gap_cp"
    )
    for i in range(0, len(rows), cfg.batch_rows):
        graph.query(set_eval, {"rows": rows[i : i + cfg.batch_rows]})

    # Derive edge eval_delta_cp (mover's view) + is_best from parent/child evals.
    edge_res = graph.query(
        """
        MATCH (a:Position)-[m:MOVE]->(b:Position)
        WHERE a.eval_cp IS NOT NULL AND b.eval_cp IS NOT NULL
        SET m.eval_delta_cp = CASE WHEN a.side = 'w'
                                   THEN b.eval_cp - a.eval_cp
                                   ELSE a.eval_cp - b.eval_cp END,
            m.is_best = (m.uci = a.best_uci)
        RETURN count(m)
        """
    )
    avg_depth = sum(r["eval_depth"] for r in rows) / len(rows) if rows else 0.0
    return EvalStats(
        positions_evaluated=len(rows),
        edges_updated=edge_res.result_set[0][0],
        avg_depth=round(avg_depth, 1),
    )
