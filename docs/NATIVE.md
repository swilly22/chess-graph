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

## Parallel replay, parallel download

- **Parallel replay** — one reader thread (decompress + header filter) feeds a
  bounded queue; `<jobs>` worker threads replay and write per-worker CSV shards.
  The engine's lookup tables are read-only after `Chess_Init()` and each worker
  owns its board, so no locking and **no engine changes**.
- **Streaming** — the producer reads compressed bytes from a file or stdin
  (`-`), so it can be fed from a pipe.
- **Parallel download** — the dominant cost at scale. The Lichess CDN throttles
  **per connection** (~3.3 MB/s measured) but scales almost linearly across
  connections (8 → ~25 MB/s). Single-connection, the full 31 GB file would take
  ~2.6 h just to transfer. Two modes (`fetch.py`):
  - **Sample** (`--engine c --jobs N`, a `--limit`): `parallel_fetch` — N range
    connections (Retry-After backoff + resume) pull the estimated prefix to
    disk, then the producer runs on it.
  - **Full file** (`--engine c --jobs N --limit 0`): `run_fused` — N connections
    each download a byte-range and **pipe it straight into a range-mode
    producer**, so each pipe overlaps its own download with its replay. The
    producer frame-syncs to the next pzstd frame boundary, owns whole frames
    (stopping at the frame boundary once its span is consumed, with a download
    margin to finish the straddling frame), and skips its leading partial game —
    so frames are owned by exactly one connection. Verified against the
    single-stream path: **−1 game in 398,425** (one boundary artifact; invisible
    after the min-games prune).

## Use it

```bash
# local file, 8 workers
uv run python -m chess_graph.cli ingest-sample --engine c --jobs 8 --source data/month.pgn.zst
# stream straight from the Lichess dump (no --source; download overlaps replay)
uv run python -m chess_graph.cli ingest-sample --engine c --jobs 8 --month 2026-10 --limit 1000000
```

`--engine c` runs the producer, aggregates its CSV shards with the same DuckDB
SQL as the Python path, and loads `chess_core` — output is **byte-identical** to
the Python engine (verified at 25k and 150k games).

## Measured (11-core machine)

Same 150k-game sample, no load, identical output (accepted 91,597;
pos 1,860,355→4,230; edges 1,894,672→6,104):

| Path | Time | Speedup |
|---|---|---|
| Python engine (`--engine duckdb`, 1 proc) | 154.3 s | 1× |
| Python engine (`--jobs 8`) | ~27 s | ~5.7× |
| **Native C (`--engine c --jobs 8`)** | **2.5 s** | **~61×** |

Producer-only scaling (parse+replay+emit, 150k games): jobs 1 → 4.27 s,
jobs 4 → 1.25 s, jobs 8 → 1.16 s (~79k accepted games/s; plateaus at ~4 as the
serial reader/decompress becomes the limit). Streaming 50k games live from the
dump (download + decompress + 8-way replay + aggregate): ~2.5 s end-to-end.

End-to-end from the live URL (`--engine c --jobs 8`, 200k games, no load):
parallel download (~297 MB, 8 conns) + producer + aggregate in **~29 s**, with a
clean early-abort at exactly 200,000 accepted.

Full-month projection (~55M accepted, 31 GB): single-connection download alone
is ~2.6 h; with 8-way parallel download (~25 MB/s) it's ~20 min, and the
replay (~12 min at `--jobs 8`) overlaps/follows — so **~20–30 min** total,
bounded by download bandwidth.

## Not yet done

- **Corpus producer** — the full-game corpus (`ingest-games`) is still Python;
  the same C approach applies.
- **Exactness at range boundaries** — the fused full-file path loses ~1 game per
  few range boundaries (frame-boundary artifact). Negligible for the aggregated
  graph; the prefix/sample and single-stream paths remain byte-exact.
