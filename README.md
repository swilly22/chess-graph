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
```

The dump is streamed and abandoned once `--limit` accepted games are read, so
only a few hundred MB of the ~29 GB file is transferred.

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
  load/
    falkor.py            schema (indexes/constraints) + batched UNWIND loaders
tests/
  test_identity.py       transposition / en-passant / castling identity tests
  test_config.py         time-control + rating-bracket classification
  test_ingest.py         PGN parse, SAN tokenizer, transposition merge
  test_aggregate_equivalence.py  memory engine == duckdb engine
```
