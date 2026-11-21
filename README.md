# Chess position graph

A transposition-aware chess opening explorer built on a graph of real Lichess
games, stored in FalkorDB. Positions are nodes; moves are edges; positions
reached by different move orders merge into one node. See `CLAUDE.md` for the
full product and architecture spec, and `docs/SCHEMA.md` for the graph schema.

## Status

**Phase 1 — environment & position identity.** Done:

- `docker-compose.yml` runs FalkorDB (RESP on `6379`, Browser UI on `3000`).
- Python project under `src/chess_graph` with `uv` for env management.
- `chess_graph.identity` — the single source of truth for position identity
  (Zobrist key + canonical EPD), with the required unit tests in `tests/`.
- `config/pipeline.toml` holds every threshold; `chess_graph.config` loads it.

**Phase 2 — sample ingestion.** Done:

- Streams a Lichess `.pgn.zst` dump (`ingest/source.py`), filters + classifies
  games (`ingest/pgn_stream.py`), replays to the ply cap and aggregates
  positions + MOVE edges (`ingest/aggregate.py`), prunes (`ingest/prune.py`),
  and bulk-loads `chess_core` in FalkorDB (`load/falkor.py`).
- Two aggregation engines: `memory` (small samples/tests) and `duckdb`
  (memory-bounded, for the full 1M run). They produce identical rows — see
  `tests/test_aggregate_equivalence.py`.

### Run the sample ingestion

```bash
docker compose up -d                      # FalkorDB must be running
# quick sample, no DB load:
uv run python -m chess_graph.cli ingest-sample --month 2026-10 --limit 20000 --no-load
# full 1M sample, memory-safe engine, load into chess_core:
uv run python -m chess_graph.cli ingest-sample --month 2026-10 --limit 1000000 \
  --engine duckdb --duckdb-path data/aggregate.duckdb --temp-dir data --report report.json
# ...or parallelize the replay across processes (~5.7x on 11 cores):
uv run python -m chess_graph.cli ingest-sample --month 2026-10 --limit 1000000 --jobs 8
```

`--jobs N` (on both `ingest-sample` and `ingest-games`) fans the CPU-bound move
replay across worker processes — the main process streams and filters (keeping
the exact-N cut deterministic), workers replay into DuckDB/Parquet shards, and
the final aggregation runs once over the union. Output is identical to
single-process; measured ~5.7× for the opening graph and ~4.2× for the corpus.

For full-month scale there's also a **native C producer** (`--engine c`, reads a
local `.pgn.zst`) that's ~19× faster than the single-process Python engine with
byte-identical output. See [`docs/NATIVE.md`](docs/NATIVE.md).

The dump is streamed and abandoned once `--limit` accepted games are read, so
only a few hundred MB of the ~29 GB file is transferred.

### Full-game corpus (for puzzle generation)

The opening graph only keeps the first 30 plies. Puzzle generation (mate
patterns, tactics, best-move) needs **whole games across all phases**, so a
separate command records complete games — full UCI move lists plus any Lichess
`[%eval]` annotations — into a Parquet corpus (`data/games_<month>.parquet`).
This corpus is offline (not in FalkorDB); the Phase-4 generator + Stockfish mine
it, and only the resulting puzzles land in the graph.

```bash
uv run python -m chess_graph.cli ingest-games --month 2026-10 --limit 1000000 \
  --out data/games_2026-10.parquet --report games_report.json
```

Add `--jobs N` to fan the move replay across worker processes (the main process
streams + filters; workers replay and write Parquet shards that are combined at
the end). Measured ~4.2× end-to-end at `--jobs 8` on this 11-core machine;
output is identical to single-process. The GIL blocks threads, so this uses
processes — and `ingest/parallel.py` is the reusable driver.

```bash
uv run python -m chess_graph.cli ingest-games --month 2026-10 --limit 1000000 --jobs 8 \
  --out data/games_2026-10.parquet
```

## Setup

```bash
uv venv --python 3.12
uv pip install -e ".[dev]"
```

## Run the tests

```bash
uv run pytest
```

## Start the database

```bash
docker compose up -d        # start FalkorDB
docker compose ps           # check health
docker compose down         # stop (graph persists in the named volume)
```

## Project layout

```
config/pipeline.toml     all tunable thresholds (brackets, filters, ply cap, ...)
docker-compose.yml       FalkorDB service
docs/SCHEMA.md           graph schema — source of truth
src/chess_graph/
  identity.py            position identity: key + EPD normalization (critical)
  config.py              typed loader for pipeline.toml
  cli.py                 `ingest-sample` command
  ingest/
    source.py            streaming .zst line source (HTTP early-abort / local)
    pgn_stream.py        PGN parse + filters + bracket/tc classification
    aggregate.py         in-memory position/edge aggregation + SAN tokenizer
    aggregate_duckdb.py  DuckDB-backed aggregation (bounded memory, for 1M)
    prune.py             min-games prune + edge endpoint survival
    fullgame.py          whole-game parser (all plies, metadata, [%eval])
    games_store.py       full-game Parquet corpus writer (puzzle substrate)
    parallel.py          multiprocessing driver (reader + worker pool, --jobs)
  load/
    falkor.py            schema (indexes/constraints) + batched UNWIND loaders
tests/
  test_identity.py       transposition / en-passant / castling identity tests
  test_config.py         time-control + rating-bracket classification
  test_ingest.py         PGN parse, SAN tokenizer, transposition merge
  test_aggregate_equivalence.py  memory engine == duckdb engine
```
