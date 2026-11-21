"""Differential test: C SAN replay vs python-chess over real games.

Replays corpus games with python-chess to produce canonical SAN movetext plus
the expected per-ply key and UCI sequences, feeds the SAN to the C
`replay_games` tool, and asserts the C keys and UCIs match for every ply of
every game. This proves the C move resolution + make-move + identity.

    make -C native replay_games
    uv run python native/difftest_replay.py --games 20000
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
REPLAY = HERE / "replay_games"


def build_cases(parquet: str, games: int):
    con = duckdb.connect(":memory:")
    rows = con.execute(
        f"SELECT moves_uci FROM read_parquet('{parquet}') LIMIT {games}"
    ).fetchall()
    for (moves,) in rows:
        board = chess.Board()
        sans: list[str] = []
        keys = [position_key(board)]
        ucis: list[str] = []
        for uci in moves:
            move = chess.Move.from_uci(uci)
            sans.append(board.san(move))  # canonical SAN before pushing
            board.push(move)
            keys.append(position_key(board))
            ucis.append(uci)
        yield " ".join(sans), keys, ucis


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default="data/games_2026-10.parquet")
    ap.add_argument("--games", type=int, default=20000)
    args = ap.parse_args()

    if not REPLAY.exists():
        print("build first: make -C native replay_games", file=sys.stderr)
        return 2

    san_lines: list[str] = []
    exp_keys: list[list[int]] = []
    exp_ucis: list[list[str]] = []
    for san_line, keys, ucis in build_cases(args.parquet, args.games):
        san_lines.append(san_line)
        exp_keys.append(keys)
        exp_ucis.append(ucis)

    proc = subprocess.run(
        [str(REPLAY)], input="\n".join(san_lines) + "\n",
        capture_output=True, text=True, check=True,
    )
    out = proc.stdout.split("\n")

    games = len(san_lines)
    key_mismatch = 0
    uci_mismatch = 0
    len_mismatch = 0
    examples: list[str] = []
    for g in range(games):
        c_keys = [int(x) for x in out[2 * g].split()] if out[2 * g] else []
        c_ucis = out[2 * g + 1].split()
        if c_keys != exp_keys[g]:
            if len(c_keys) != len(exp_keys[g]):
                len_mismatch += 1
            key_mismatch += 1
            if len(examples) < 5:
                # find first differing ply
                for i in range(min(len(c_keys), len(exp_keys[g]))):
                    if c_keys[i] != exp_keys[g][i]:
                        examples.append(
                            f"game {g} ply {i}: C={c_keys[i]} py={exp_keys[g][i]} "
                            f"| line: {san_lines[g][:60]}"
                        )
                        break
                else:
                    examples.append(f"game {g}: key-count C={len(c_keys)} py={len(exp_keys[g])}")
        if c_ucis != exp_ucis[g]:
            uci_mismatch += 1

    total_plies = sum(len(k) for k in exp_keys)
    print(f"games: {games:,} | plies(+starts): {total_plies:,}")
    print(f"games with key mismatch: {key_mismatch}")
    print(f"games with uci mismatch: {uci_mismatch}")
    print(f"games with length mismatch: {len_mismatch}")
    for ex in examples:
        print("  ", ex)
    return 0 if (key_mismatch == 0 and uci_mismatch == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
