"""GamesCorpusWriter: DuckDB -> Parquet round-trip and stats."""

from __future__ import annotations

import duckdb

from chess_graph.ingest.fullgame import FullGame
from chess_graph.ingest.games_store import GamesCorpusWriter


def _game(game_id, plies, moves, evals, has_eval, truncated=False, ply_count=None):
    return FullGame(
        game_id=game_id, source="lichess", white_elo=1600, black_elo=1600,
        bracket=2, tc="blitz", result="w", termination="Normal", eco="C41",
        opening="Philidor Defense", played_at=1_700_000_000,
        ply_count=ply_count if ply_count is not None else plies,
        moves_uci=moves, evals_cp=evals, has_eval=has_eval, truncated=truncated,
    )


def test_corpus_round_trip(tmp_path):
    out = str(tmp_path / "games.parquet")
    writer = GamesCorpusWriter(str(tmp_path / "games.duckdb"), out)
    writer.add(_game("g1", 4, ["e2e4", "e7e5", "g1f3", "b8c6"], [20, 19, 15, None], True))
    writer.add(_game("g2", 2, ["d2d4", "d7d5"], [], False))
    writer.add(_game("g3", 0, [], [], False))  # 0-move game: empty lists
    writer.finalize()
    stats = writer.stats()
    writer.close()

    assert stats.games == 3
    assert stats.distinct_ids == 3
    assert stats.total_plies == 6
    assert stats.max_plies == 4
    assert stats.games_with_eval == 1
    assert stats.eval_coverage == round(1 / 3, 4)

    rows = duckdb.connect(":memory:").execute(
        f"SELECT game_id, moves_uci, evals_cp, has_eval FROM read_parquet('{out}') ORDER BY game_id"
    ).fetchall()
    assert rows[0][0] == "g1"
    assert rows[0][1] == ["e2e4", "e7e5", "g1f3", "b8c6"]
    assert rows[0][2] == [20, 19, 15, None]  # NULL preserved in the list
    assert rows[0][3] is True
    assert rows[1][1] == ["d2d4", "d7d5"]
    assert rows[2][1] == []  # 0-move game stored cleanly


def test_corpus_queryable_for_puzzle_mining(tmp_path):
    """The corpus must support the kind of query a puzzle generator runs:
    find games with a big eval swing between consecutive plies."""
    out = str(tmp_path / "g.parquet")
    writer = GamesCorpusWriter(str(tmp_path / "g.duckdb"), out)
    # a blunder: eval jumps from +30 to -250 at ply 3
    writer.add(_game("blunder", 4, ["e2e4", "e7e5", "f1c4", "d8h4"], [30, 20, 25, -250], True))
    writer.add(_game("quiet", 2, ["d2d4", "d7d5"], [10, 12], True))
    writer.finalize()
    writer.close()

    con = duckdb.connect(":memory:")
    # games with a >= 200cp swing between consecutive plies (candidate "punish"
    # puzzles): list_transform(x, i) compares each eval to the previous one.
    swings = con.execute(
        f"""
        SELECT game_id FROM read_parquet('{out}')
        WHERE has_eval AND len(evals_cp) >= 2
          AND list_max(list_transform(evals_cp[2:], lambda x, i: abs(x - evals_cp[i]))) >= 200
        """
    ).fetchall()
    assert ("blunder",) in swings
    assert ("quiet",) not in swings
