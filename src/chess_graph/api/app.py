"""Explorer API — a thin, cacheable read layer over the core graph.

Clients send a FEN; the server normalizes it through the identity function and
serves the position's eval/opening/structure, its candidate moves (per-bracket
W/D/L, eval delta, is_best), and the move orders that reach it ("reached via").
Explorer responses are keyed by position and change only on a core rebuild, so
they're safe to cache.
"""

from __future__ import annotations

import chess
from fastapi import FastAPI, HTTPException, Query, Response

from chess_graph.config import PipelineConfig, load_config
from chess_graph.identity import normalize_fen, position_key
from chess_graph.load.falkor import connect

_BRACKETS = ["<1200", "1200-1499", "1500-1799", "1800-2099", "2100-2399", "2400+"]
_CACHE = "public, max-age=3600"


def format_eval(cp: int | None) -> str | None:
    """Human eval string from the signed-cp-with-mate-sentinel value."""
    if cp is None:
        return None
    if abs(cp) >= 90000:
        n = 100000 - abs(cp)
        return f"M{n}" if cp > 0 else f"-M{n}"
    return f"{cp / 100:+.2f}"


def _key_from_fen(fen: str) -> int:
    try:
        return normalize_fen(fen).key
    except (ValueError, IndexError) as exc:
        raise HTTPException(status_code=400, detail=f"invalid FEN: {exc}") from exc


def create_app(cfg: PipelineConfig | None = None) -> FastAPI:
    cfg = cfg or load_config()
    app = FastAPI(title="Transpose — opening explorer API", version="0.1.0")
    app.state.graph = connect(cfg)
    start_key = position_key(chess.Board())

    def g():
        return app.state.graph

    @app.get("/health")
    def health() -> dict:
        n = g().query("MATCH (p:Position) RETURN count(p)").result_set[0][0]
        return {"status": "ok", "graph": cfg.core_graph, "positions": n}

    @app.get("/explorer")
    def explorer(response: Response, fen: str = Query(..., description="position FEN")) -> dict:
        key = _key_from_fen(fen)
        pos = g().query(
            """
            MATCH (p:Position {key: $k})
            OPTIONAL MATCH (p)-[:HAS_STRUCTURE]->(s:PawnStructure)
            RETURN p.epd, p.side, p.eval_cp, p.best_uci, p.best_gap_cp, p.min_ply, p.games,
                   s.signature, s.name
            """,
            {"k": key},
        ).result_set
        if not pos:
            raise HTTPException(404, "position not in the core graph (too rare / past the opening)")
        epd, side, eval_cp, best_uci, best_gap, min_ply, games, ssig, sname = pos[0]
        opening = _resolve_opening(g(), key)

        rows = g().query(
            """
            MATCH (p:Position {key: $k})-[m:MOVE]->(c:Position)
            RETURN m.san, m.uci, m.games, m.is_best, m.eval_delta_cp, c.eval_cp,
                   m.blitz_w, m.blitz_d, m.blitz_b, m.slow_w, m.slow_d, m.slow_b
            ORDER BY m.games DESC
            """,
            {"k": key},
        ).result_set
        moves = [
            {
                "san": r[0], "uci": r[1], "games": r[2], "is_best": r[3],
                "eval_delta_cp": r[4], "child_eval_cp": r[5], "child_eval": format_eval(r[5]),
                "blitz": {"white": r[6], "draw": r[7], "black": r[8]},
                "slow": {"white": r[9], "draw": r[10], "black": r[11]},
            }
            for r in rows
        ]

        response.headers["Cache-Control"] = _CACHE
        return {
            "position": {
                "key": key, "epd": epd, "side": side,
                "eval_cp": eval_cp, "eval": format_eval(eval_cp),
                "best_uci": best_uci, "best_gap_cp": best_gap,
                "min_ply": min_ply, "games": games,
                "opening": opening,
                "structure": {"signature": ssig, "name": sname} if ssig else None,
            },
            "brackets": _BRACKETS,
            "moves": moves,
            "reached_via": _reached_via(g(), key, start_key),
        }

    @app.get("/reached-via")
    def reached_via(response: Response, fen: str = Query(...)) -> dict:
        key = _key_from_fen(fen)
        if not g().query("MATCH (p:Position {key: $k}) RETURN 1", {"k": key}).result_set:
            raise HTTPException(404, "position not in the core graph")
        response.headers["Cache-Control"] = _CACHE
        return {"reached_via": _reached_via(g(), key, start_key)}

    return app


def _resolve_opening(graph, key: int) -> dict | None:
    """Nearest named opening: this position if NAMED, else the deepest NAMED
    ancestor (the most specific opening whose line is a prefix of this one)."""
    res = graph.query(
        """
        MATCH (p:Position {key: $k})<-[:MOVE*0..15]-(anc:Position)-[:NAMED]->(o:Opening)
        RETURN o.name, o.eco ORDER BY anc.min_ply DESC LIMIT 1
        """,
        {"k": key},
    ).result_set
    return {"name": res[0][0], "eco": res[0][1]} if res else None


def _shortest_line(graph, key: int, start_key: int) -> list[str]:
    """SAN sequence from the start to `key`, following min_ply-decreasing parents."""
    sans: list[str] = []
    cur = key
    for _ in range(40):
        if cur == start_key:
            break
        res = graph.query(
            """
            MATCH (a:Position)-[m:MOVE]->(p:Position {key: $k})
            WHERE a.min_ply < p.min_ply
            RETURN a.key, m.san ORDER BY a.min_ply ASC LIMIT 1
            """,
            {"k": cur},
        ).result_set
        if not res:
            break
        cur, san = res[0]
        sans.append(san)
    sans.reverse()
    return sans


def _reached_via(graph, key: int, start_key: int) -> list[dict]:
    parents = graph.query(
        "MATCH (a:Position)-[m:MOVE]->(p:Position {key: $k}) "
        "RETURN a.key, m.san, m.games ORDER BY m.games DESC",
        {"k": key},
    ).result_set
    out = []
    for i, (pkey, san, pgames) in enumerate(parents):
        line = _shortest_line(graph, pkey, start_key) + [san]
        out.append({
            "san": san, "games": pgames, "line": line,
            "transposition": i > 0,  # the first (most-played) is the main line
        })
    return out


app = create_app()
