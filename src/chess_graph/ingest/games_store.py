"""Write the full-game corpus to an on-disk DuckDB table, exported as Parquet.

The corpus is the offline substrate the phase-4 puzzle generator (+ Stockfish)
will mine across all game phases. It is deliberately NOT loaded into FalkorDB —
that stays the opening-graph serving store; only generated puzzles land there.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from chess_graph.ingest.fullgame import FullGame

_BATCH = 20_000

_CREATE = """
CREATE TABLE games(
  game_id VARCHAR, source VARCHAR,
  white_elo INTEGER, black_elo INTEGER,
  bracket UTINYINT, tc VARCHAR, result VARCHAR,
  termination VARCHAR, eco VARCHAR, opening VARCHAR,
  played_at BIGINT, ply_count INTEGER,
  moves_uci VARCHAR[], evals_cp INTEGER[],
  has_eval BOOLEAN, truncated BOOLEAN
)
"""


@dataclass(slots=True)
class CorpusStats:
    games: int
    total_plies: int
    avg_plies: float
    max_plies: int
    games_with_eval: int
    eval_coverage: float
    truncated: int
    distinct_ids: int


class GamesCorpusWriter:
    def __init__(self, db_path: str, out_parquet: str, memory_limit: str = "6GB") -> None:
        self.out = out_parquet
        self.count = 0
        self.con = duckdb.connect(db_path)
        self.con.execute(f"SET memory_limit = '{memory_limit}'")
        self.con.execute("DROP TABLE IF EXISTS games")
        self.con.execute(_CREATE)
        self._app = self.con.appender("games")
        self._buf: list[tuple] = []

    def add(self, g: FullGame) -> None:
        self._buf.append(
            (
                g.game_id, g.source, g.white_elo, g.black_elo, g.bracket, g.tc, g.result,
                g.termination, g.eco, g.opening, g.played_at, g.ply_count,
                g.moves_uci, g.evals_cp, g.has_eval, g.truncated,
            )
        )
        self.count += 1
        if len(self._buf) >= _BATCH:
            self._flush()

    def _flush(self) -> None:
        if self._buf:
            self._app.append_rows(self._buf)
            self._buf.clear()

    def finalize(self) -> None:
        """Flush and export the Parquet file (ZSTD-compressed)."""
        self._flush()
        self._app.flush()
        self.con.execute(f"COPY games TO '{self.out}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    def stats(self) -> CorpusStats:
        row = self.con.execute(
            """
            SELECT count(*), coalesce(sum(ply_count), 0), coalesce(avg(ply_count), 0),
                   coalesce(max(ply_count), 0),
                   count(*) FILTER (WHERE has_eval),
                   count(*) FILTER (WHERE truncated),
                   count(DISTINCT game_id)
            FROM games
            """
        ).fetchone()
        games = row[0]
        return CorpusStats(
            games=games,
            total_plies=int(row[1]),
            avg_plies=round(float(row[2]), 1),
            max_plies=int(row[3]),
            games_with_eval=row[4],
            eval_coverage=round(row[4] / games, 4) if games else 0.0,
            truncated=row[5],
            distinct_ids=row[6],
        )

    def close(self) -> None:
        self.con.close()


_STATS_SQL = """
SELECT count(*), coalesce(sum(ply_count), 0), coalesce(avg(ply_count), 0),
       coalesce(max(ply_count), 0),
       count(*) FILTER (WHERE has_eval),
       count(*) FILTER (WHERE truncated),
       count(DISTINCT game_id)
FROM {source}
"""


def _stats_from_row(row: tuple) -> CorpusStats:
    games = row[0]
    return CorpusStats(
        games=games,
        total_plies=int(row[1]),
        avg_plies=round(float(row[2]), 1),
        max_plies=int(row[3]),
        games_with_eval=row[4],
        eval_coverage=round(row[4] / games, 4) if games else 0.0,
        truncated=row[5],
        distinct_ids=row[6],
    )


def stats_from_parquet(parquet_glob: str) -> CorpusStats:
    """Compute corpus stats over one or more Parquet files (glob allowed)."""
    con = duckdb.connect(":memory:")
    row = con.execute(_STATS_SQL.format(source=f"read_parquet('{parquet_glob}')")).fetchone()
    con.close()
    return _stats_from_row(row)
