"""DuckDB-backed aggregation for memory-safe large samples.

The in-memory :class:`~chess_graph.ingest.aggregate.Aggregator` holds every
transient (soon-pruned) position — at ~20 positions/game that is tens of
millions of Python objects for a 1M-game sample (measured ~30 GB). This path
instead streams one row per position-visit and edge-visit into an on-disk
DuckDB database and does the aggregation + prune in SQL, so memory stays
bounded (DuckDB spills to its temp dir). It yields the same node/edge row dicts
the loader consumes, so the two aggregators are interchangeable.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import chess
import duckdb

from chess_graph.config import PipelineConfig
from chess_graph.identity import position_key
from chess_graph.ingest.aggregate import iter_san_moves
from chess_graph.ingest.pgn_stream import AcceptedGame

_RESULT_CODE = {"w": 0, "d": 1, "b": 2}
_FLUSH_ROWS = 50_000


def _bracket_sums(prefix: str, cond: str) -> str:
    return ", ".join(
        f"CAST(sum(CASE WHEN {cond} AND bracket = {b} THEN 1 ELSE 0 END) AS BIGINT) AS {prefix}{b}"
        for b in range(6)
    )


@dataclass(slots=True)
class DuckStats:
    positions_before: int = 0
    positions_after: int = 0
    edges_before: int = 0
    edges_after: int = 0


def _positions_sql(source: str, min_games: int) -> str:
    return f"""
        CREATE OR REPLACE TABLE positions AS
        SELECT key, any_value(epd) AS epd, CAST(count(*) AS BIGINT) AS games,
               {_bracket_sums('rb', 'is_blitz')},
               {_bracket_sums('rs', 'NOT is_blitz')}
        FROM {source}
        GROUP BY key
        HAVING count(*) >= {min_games}
    """


def _moves_sql(source: str) -> str:
    return f"""
        CREATE OR REPLACE TABLE moves AS
        SELECT e.frm AS frm, e.t AS t,
               any_value(e.uci) AS uci, any_value(e.san) AS san,
               CAST(count(*) AS BIGINT) AS games,
               {_bracket_sums('bw', 'is_blitz AND result = 0')},
               {_bracket_sums('bd', 'is_blitz AND result = 1')},
               {_bracket_sums('bb', 'is_blitz AND result = 2')},
               {_bracket_sums('sw', 'NOT is_blitz AND result = 0')},
               {_bracket_sums('sd', 'NOT is_blitz AND result = 1')},
               {_bracket_sums('sb', 'NOT is_blitz AND result = 2')}
        FROM {source} e
        SEMI JOIN positions pf ON e.frm = pf.key
        SEMI JOIN positions pt ON e.t = pt.key
        GROUP BY e.frm, e.t
    """


def build_positions_and_moves(con, pos_source: str, edge_source: str, min_games: int) -> DuckStats:
    """Run the GROUP BY + prune over ``pos_source``/``edge_source`` (a table name
    or a ``read_parquet('...')`` expression), creating `positions` and `moves`."""
    con.execute(_positions_sql(pos_source, min_games))
    con.execute(_moves_sql(edge_source))
    stats = DuckStats()
    stats.positions_before = con.execute(
        f"SELECT count(DISTINCT key) FROM {pos_source}"
    ).fetchone()[0]
    stats.edges_before = con.execute(
        f"SELECT count(DISTINCT (frm, t)) FROM {edge_source}"
    ).fetchone()[0]
    stats.positions_after = con.execute("SELECT count(*) FROM positions").fetchone()[0]
    stats.edges_after = con.execute("SELECT count(*) FROM moves").fetchone()[0]
    return stats


def iter_node_rows(con, batch: int = 10_000) -> Iterator[dict]:
    cur = con.execute(
        "SELECT key, epd, games, rb0, rb1, rb2, rb3, rb4, rb5, "
        "rs0, rs1, rs2, rs3, rs4, rs5 FROM positions"
    )
    while rows := cur.fetchmany(batch):
        for r in rows:
            _, side, castling, _ep = r[1].split(" ", 3)
            yield {
                "key": r[0], "epd": r[1], "side": side, "castling": castling,
                "games": r[2],
                "reach_blitz": list(r[3:9]),
                "reach_slow": list(r[9:15]),
            }


def iter_edge_rows(con, first_seen: int, batch: int = 10_000) -> Iterator[dict]:
    cur = con.execute(
        "SELECT frm, t, uci, san, games, "
        "bw0,bw1,bw2,bw3,bw4,bw5, bd0,bd1,bd2,bd3,bd4,bd5, bb0,bb1,bb2,bb3,bb4,bb5, "
        "sw0,sw1,sw2,sw3,sw4,sw5, sd0,sd1,sd2,sd3,sd4,sd5, sb0,sb1,sb2,sb3,sb4,sb5 "
        "FROM moves"
    )
    while rows := cur.fetchmany(batch):
        for r in rows:
            yield {
                "a": r[0], "b": r[1], "uci": r[2], "san": r[3], "games": r[4],
                "bw": list(r[5:11]), "bd": list(r[11:17]), "bb": list(r[17:23]),
                "sw": list(r[23:29]), "sd": list(r[29:35]), "sb": list(r[35:41]),
                "fs": first_seen,
            }


class ShardVisitAggregator:
    """Aggregate pos/edge visit Parquet shards (from parallel workers) into the
    final `positions`/`moves`, using the same SQL as the serial engine."""

    def __init__(
        self,
        cfg: PipelineConfig,
        first_seen: int,
        pos_glob: str,
        edge_glob: str,
        db_path: str,
        memory_limit: str = "8GB",
        temp_dir: str | None = None,
        threads: int = 4,
    ) -> None:
        self.first_seen = first_seen
        self.con = duckdb.connect(db_path)
        self.con.execute(f"SET memory_limit = '{memory_limit}'")
        self.con.execute(f"PRAGMA threads = {threads}")
        if temp_dir:
            self.con.execute(f"SET temp_directory = '{temp_dir}'")
        self.stats = build_positions_and_moves(
            self.con,
            f"read_parquet('{pos_glob}')",
            f"read_parquet('{edge_glob}')",
            cfg.min_games_per_position,
        )

    def node_rows(self, batch: int = 10_000) -> Iterator[dict]:
        return iter_node_rows(self.con, batch)

    def edge_rows(self, batch: int = 10_000) -> Iterator[dict]:
        return iter_edge_rows(self.con, self.first_seen, batch)

    def close(self) -> None:
        self.con.close()


class DuckDBAggregator:
    """Streams position/edge visits into DuckDB; aggregates + prunes in SQL."""

    def __init__(
        self,
        cfg: PipelineConfig,
        first_seen: int,
        db_path: str,
        memory_limit: str = "8GB",
        temp_dir: str | None = None,
        threads: int = 4,
    ) -> None:
        self.cfg = cfg
        self.first_seen = first_seen
        self.games_aggregated = 0
        self.san_errors = 0

        self.con = duckdb.connect(db_path)
        self.con.execute(f"SET memory_limit = '{memory_limit}'")
        self.con.execute(f"PRAGMA threads = {threads}")
        if temp_dir:
            self.con.execute(f"SET temp_directory = '{temp_dir}'")
        self.con.execute("DROP TABLE IF EXISTS pos_visits")
        self.con.execute("DROP TABLE IF EXISTS edge_visits")
        self.con.execute(
            "CREATE TABLE pos_visits("
            "key BIGINT, epd VARCHAR, is_blitz BOOLEAN, bracket UTINYINT)"
        )
        self.con.execute(
            "CREATE TABLE edge_visits("
            "frm BIGINT, t BIGINT, uci VARCHAR, san VARCHAR, "
            "is_blitz BOOLEAN, bracket UTINYINT, result UTINYINT)"
        )
        self._pos_app = self.con.appender("pos_visits")
        self._edge_app = self.con.appender("edge_visits")
        self._pos_buf: list[tuple] = []
        self._edge_buf: list[tuple] = []

    def add_game(self, game: AcceptedGame) -> None:
        board = chess.Board()
        from_key = position_key(board)
        is_blitz = game.tc == "blitz"
        result = _RESULT_CODE[game.result]

        self._pos_buf.append((from_key, board.epd(), is_blitz, game.bracket))
        visited_pos = {from_key}
        visited_edge: set[tuple[int, int]] = set()

        for san in iter_san_moves(game.movetext, self.cfg.depth_cap_plies):
            try:
                move = board.parse_san(san)
            except (ValueError, AssertionError):
                self.san_errors += 1
                break
            uci = move.uci()
            board.push(move)
            to_key = position_key(board)

            if to_key not in visited_pos:
                self._pos_buf.append((to_key, board.epd(), is_blitz, game.bracket))
                visited_pos.add(to_key)

            edge_key = (from_key, to_key)
            if edge_key not in visited_edge:
                self._edge_buf.append((from_key, to_key, uci, san, is_blitz, game.bracket, result))
                visited_edge.add(edge_key)
            from_key = to_key

        self.games_aggregated += 1
        if len(self._pos_buf) >= _FLUSH_ROWS:
            self._flush()

    def _flush(self) -> None:
        if self._pos_buf:
            self._pos_app.append_rows(self._pos_buf)
            self._pos_buf.clear()
        if self._edge_buf:
            self._edge_app.append_rows(self._edge_buf)
            self._edge_buf.clear()

    def finalize(self) -> DuckStats:
        """Flush, then aggregate + prune in SQL into `positions` and `moves`."""
        self._flush()
        self._pos_app.flush()
        self._edge_app.flush()
        return build_positions_and_moves(
            self.con, "pos_visits", "edge_visits", self.cfg.min_games_per_position
        )

    def export_visits(self, pos_path: str, edge_path: str) -> None:
        """Write this shard's raw visit rows to Parquet (for parallel workers)."""
        self._flush()
        self._pos_app.flush()
        self._edge_app.flush()
        self.con.execute(f"COPY pos_visits TO '{pos_path}' (FORMAT PARQUET)")
        self.con.execute(f"COPY edge_visits TO '{edge_path}' (FORMAT PARQUET)")

    def node_rows(self, batch: int = 10_000) -> Iterator[dict]:
        return iter_node_rows(self.con, batch)

    def edge_rows(self, batch: int = 10_000) -> Iterator[dict]:
        return iter_edge_rows(self.con, self.first_seen, batch)

    def close(self) -> None:
        self.con.close()
