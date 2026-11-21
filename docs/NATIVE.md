# Native C producer (`native/`)

A C implementation of the opening-graph hot path — stream a `.pgn.zst`, filter,
replay the opening, and emit position/edge visit rows — for when Python +
multiprocessing isn't fast enough (full-month scale). It reuses the
[`swilly22/chess`](https://github.com/swilly22/chess) engine (git submodule at
`native/engine`) for board state, make-move, and attack detection.

Per the project's language preference, the native layer is **C, not Rust**.

## Pieces

| File | Role |
|---|---|
| `chessid.{h,c}` | Position identity: Polyglot Zobrist key + canonical EPD. Pure C, no engine. |
| `polyglot_random.h` | 781 Zobrist constants generated from python-chess (`gen_polyglot_header.py`). |
| `replay.{h,c}` | SAN → legal move + make-move + per-ply identity, on the engine. |
| `producer.c` | Full producer: libzstd stream → PGN filter → replay → CSV visit shards. |
| `hash_fen.c`, `replay_games.c` | Tools driven by the differential tests. |

## Identity contract (the hard part)

The C key/EPD must match python-chess **byte-for-byte** or transpositions break.
Two differential tests enforce this against real games:

- `difftest_identity.py` — static FEN → key/EPD. **1,367,720 positions, 0 mismatches.**
- `difftest_replay.py` — full SAN replay → per-ply key/UCI. **6.86M plies, 0 mismatches.**

Subtleties handled: the Zobrist uses the *pseudo* en-passant rule while the EPD
uses the *legal* (pin-aware) rule; castling rights are tracked independently of
the engine (whose castling bitmask is lossy); castling and en-passant moves are
constructed directly (the engine's generator emits neither, and its pin
parameter is type-inconsistent, so we do our own geometric candidate generation
and use the engine only for make-move + `isSquareAttacked`).

## Build & test

```bash
git submodule update --init          # fetch native/engine
make -C native hash_fen replay_games producer
uv run python native/difftest_identity.py --games 20000
uv run python native/difftest_replay.py  --games 20000
```

## Use it

```bash
uv run python -m chess_graph.cli ingest-sample --engine c --source data/month.pgn.zst
```

`--engine c` runs the native producer, aggregates its CSV shards with the same
DuckDB SQL as the Python path, and loads `chess_core` — output is **identical**
to the Python engine (verified on a 25k sample: pos 338,782→674, edges
343,767→891, accepted 15,396).

## Measured

| | 25k-game sample, no load |
|---|---|
| Python engine (`--engine duckdb`, 1 proc) | 18.1 s |
| **Native C (`--engine c`)** | **0.96 s (~19×)** |
| producer alone (parse+replay+emit) | 0.53 s user (~29k accepted games/s) |

Full-month projection (~55M accepted games): ~25 h (single-process Python) →
~45–60 min (single-threaded C). The producer is embarrassingly parallel, so
sharding the input across cores would bring that to minutes.

## Not yet done

- **HTTP streaming in C** — the producer reads a local `.zst`; streaming the
  dump directly would need libcurl (today: download first, or keep Python
  streaming for the live dump).
- **Parallel producer** — currently single-threaded; shard the input or thread
  the replay for month-in-minutes.
- **Corpus producer** — the full-game corpus (`ingest-games`) is still Python;
  the same C approach applies.
