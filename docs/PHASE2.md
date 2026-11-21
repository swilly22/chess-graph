# Phase 2 — sample ingestion measurements

Measured run, not estimates. Raw report: [`phase2_report.json`](phase2_report.json).

## Run

- **Source:** `lichess_db_standard_rated_2026-10.pgn.zst` (29 GB), streamed with
  early-abort — only a few hundred MB transferred.
- **Engine:** `duckdb` (bounded memory). Machine: 36 GB RAM, Apple Silicon.
- **Sample:** 1,000,000 accepted games.

## Headline numbers

| Metric | Value |
|---|---|
| Games scanned to fill the sample | 1,610,941 |
| Games accepted | 1,000,000 (62.1%) |
| Aggregate throughput | 598 games/s (27.9 min total) |
| Positions before prune | 17,883,657 |
| Positions after prune (`games ≥ 20`) | **49,027** |
| Edges before prune | 18,299,779 |
| Edges after prune | **74,443** |
| Peak pipeline RSS (DuckDB engine) | 7.9 GB |
| Node load time | 1.7 s |
| Edge load time | 6.5 s |
| FalkorDB graph size | 84 MB |
| SAN parse errors | 0 |

### Rejections (why 38% of scanned games were dropped)

| Reason | Count |
|---|---|
| Bullet/ultrabullet (time control) | 577,502 |
| Rating difference > 400 | 27,907 |
| Unrated | 5,375 |
| Unfinished (`*`) | 157 |
| Non-standard variant | 0 (file is already standard) |

## Graph sanity checks

- Start position `games` = 1,000,000 (exactly the accepted count).
- First-move distribution matches reality: e4 61.6%, d4 25.2%, Nf3 3.2%,
  c4 2.9%, e3 1.6%, g3 1.1%.
- **20,848 transposition merge-nodes** (positions reached by ≥2 distinct parent
  moves); max in-degree 5.
- Per-bracket `reach_blitz` + `reach_slow` arrays sum to exactly 1,000,000.
- First-move edge games sum to 998,688 → **1,312 accepted games (0.13%) made no
  move** (aborted/forfeited before move 1). Counted at the root only; not an
  error, just a property of the data.

## Decisions these numbers drive

- **Memory / engine.** In-memory aggregation grows ~linearly (~20 positions/
  game, barely saturating): 17.9M positions at 1M games would need ~28–30 GB —
  unsafe here. The DuckDB engine held it to a configurable 8 GB. **Keep DuckDB
  for any run ≥ ~300k games; the in-memory engine is for small samples/tests.**
- **Parser scaling.** Single-process python-chess does ~600 games/s (opening
  graph) / ~1,270 (corpus). `--jobs` fans the replay across processes — measured
  (same 100k, 11 cores): **opening graph 171 s → 30 s (~5.7×)**, **corpus 82 s →
  19 s (~4.2×)**. The opening path gains more because it also hashes/dedups per
  ply. Output is identical to single-process (row-for-row, test-enforced). The
  GIL blocks threads, so this uses processes. A full month (~90M+ games) would
  still take hours even parallelized; if that's needed, move the parse/replay to
  a **C** layer (not Rust) keeping the same identity rules.
- **Frequency floor (`min_games = 20`).** Yields a clean 49k-node graph from 1M
  games (0.27% of pre-prune positions survive). Reasonable to keep provisional;
  revisit against a larger sample and the explorer's depth needs.
- **Ply cap (30).** Sustainable — the pruned graph stays small at 30 plies.
- **`Game` nodes.** N/A for the Lichess sample (aggregated into stats, not
  stored). Revisit only when loading curated model/master games.

## Full-game corpus (same 1M sample)

Separate from the opening graph: complete games recorded to Parquet for puzzle
mining across all phases (`ingest-games`). See `docs/SCHEMA.md` → "Full-game
corpus".

| Metric | Value |
|---|---|
| Games | 1,000,000 |
| Total plies recorded | 66,916,930 (avg **66.9**, max 600) |
| Throughput | **1,272 games/s** (13.1 min) — faster than the graph (no per-ply hashing) |
| Games with free Lichess `[%eval]` | 125,606 (**12.6%**) |
| Truncated (bad SAN) | 0 |
| Parquet size | 112.7 MB |
| Peak RSS | 3.8 GB |

~89% of games extend past the 30-ply opening cap, and ~95% of the eval-carrying
games contain a ≥300cp swing → ~120k candidate "punish" puzzles from free evals
alone, before any Stockfish.

## Reproduce

```bash
docker compose up -d
uv run python -m chess_graph.cli ingest-sample --month 2026-10 --limit 1000000 \
  --engine duckdb --duckdb-path data/aggregate.duckdb --temp-dir data \
  --report docs/phase2_report.json
```
