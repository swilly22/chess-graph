"""Differential test: C identity kernel vs python-chess over real positions.

Replays games from the corpus with python-chess, collects (FEN, key, epd) at
every ply, feeds the FENs to the C `hash_fen` tool, and asserts the C key and
EPD match python-chess for every position. This is the gate the C producer must
clear before it can replace the Python path.

    make -C native hash_fen
    uv run python native/difftest_identity.py --games 20000
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import chess
import duckdb

from chess_graph.identity import position_key

HERE = Path(__file__).resolve().parent
HASH_FEN = HERE / "hash_fen"


def collect_positions(parquet: str, games: int):
    """Yield (fen, expected_key, expected_epd) for every ply of `games` games."""
    con = duckdb.connect(":memory:")
    rows = con.execute(
        f"SELECT moves_uci FROM read_parquet('{parquet}') LIMIT {games}"
    ).fetchall()
    # en_passant="fen" always emits the ep square on a double push (what the C
    # producer tracks from the move), so the key's pseudo-ep rule has its input.
    # The EPD still uses python-chess's legal-ep display, which C must match.
    for (moves,) in rows:
        board = chess.Board()
        yield board.fen(en_passant="fen"), position_key(board), board.epd()
        for uci in moves:
            board.push(chess.Move.from_uci(uci))
            yield board.fen(en_passant="fen"), position_key(board), board.epd()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default="data/games_2026-10.parquet")
    ap.add_argument("--games", type=int, default=20000)
    args = ap.parse_args()

    if not HASH_FEN.exists():
        print("build first: make -C native hash_fen", file=sys.stderr)
        return 2

    fens: list[str] = []
    exp_keys: list[int] = []
    exp_epds: list[str] = []
    for fen, key, epd in collect_positions(args.parquet, args.games):
        fens.append(fen)
        exp_keys.append(key)
        exp_epds.append(epd)

    proc = subprocess.run(
        [str(HASH_FEN)], input="\n".join(fens), capture_output=True, text=True, check=True
    )
    out = proc.stdout.splitlines()
    assert len(out) == len(fens), f"C emitted {len(out)} lines for {len(fens)} FENs"

    key_mismatch = 0
    epd_mismatch = 0
    first_examples: list[str] = []
    for i, line in enumerate(out):
        c_key_s, _, c_epd = line.partition("\t")
        c_key = int(c_key_s)
        if c_key != exp_keys[i]:
            key_mismatch += 1
            if len(first_examples) < 5:
                first_examples.append(f"KEY  {fens[i]}  C={c_key} py={exp_keys[i]}")
        if c_epd != exp_epds[i]:
            epd_mismatch += 1
            if len(first_examples) < 5:
                first_examples.append(f"EPD  {fens[i]}  C={c_epd!r} py={exp_epds[i]!r}")

    total = len(fens)
    print(f"positions checked: {total:,}")
    print(f"key mismatches:    {key_mismatch}")
    print(f"epd mismatches:    {epd_mismatch}")
    for ex in first_examples:
        print("  ", ex)
    return 0 if (key_mismatch == 0 and epd_mismatch == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
