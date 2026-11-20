# Chess position graph

A chess-improvement app built on a graph of real games. Positions are nodes, moves are edges, and positions reached by different move orders merge into one node, so the opening "tree" becomes a true graph with transpositions. Data comes from the Lichess open database (CC0). The graph lives in FalkorDB.

The full graph schema (labels, relationship types, properties, indexes, constraints) is in `docs/SCHEMA.md`. Treat it as the source of truth and update it whenever the schema changes.

## Product goals

The app helps club players improve by combining engine truth with how people at their rating actually play.

- **Transposition-aware opening explorer:** board, candidate moves with win/draw/loss bars per rating bracket, and a "reached via" panel listing every move order that leads to the current position.
- **Transposition map:** a layered DAG view (one column per ply, edge width = games, merge nodes highlighted), expanded one ply at a time, with an Elo slider that reweights edges.
- **Repertoire overlay and coverage:** import a user's games, show where they leave theory and where they score below the field, and compute what share of their games a repertoire covers.
- **Practical move vs engine move:** at each position, show both the engine's best move and the move that scores best at the user's rating.
- **Pawn-structure layer:** link positions to pawns-only structures so players can learn structures across openings.
- **Transposition-aware spaced-repetition drills** attached to positions, not move sequences.
- **Generated puzzles** (opening phase): `punish`, `avoid_trap`, `only_move`, `move_order`, `guess_peer`, each with real-world frequency and an empirical difficulty curve per rating bracket.
- **Opponent scouting:** the repertoire overlay applied to another player's public games.

## Architecture decisions (already made)

- **One core graph, `chess_core`,** holds positions, moves with per-bracket stats, evals, openings, pawn structures, puzzles, and a small set of model games.
- **One small graph per user, `chess_user_<id>`,** holds that user's games, repertoire, drill state, and puzzle attempts. User graphs reference core positions by Zobrist `key` only. FalkorDB cannot join across graphs, so the API merges them.
- **Rating brackets are not split into separate graphs.** Cross-bracket comparison (rating lens, difficulty curves) must stay a single query.
- **Core graph rebuilds are snapshots:** build the next month's graph alongside the live one, then switch the API over.
- **Front end:** responsive web app, later installable as a PWA for drills. A browser extension may come later and must only work on analysis boards and finished games, never during live play.
- **API:** a thin service in front of FalkorDB (FastAPI preferred). FalkorDB is never exposed to clients. Explorer responses are keyed by position and cacheable, since core data changes only on rebuild.
- **Licensing:** avoid GPL front-end components (no chessground). Use MIT/BSD libraries such as react-chessboard or cm-chessboard and chess.js. Stockfish runs server-side only.

## Position identity rules (critical)

Every position must map to exactly one node. Duplicate nodes silently break transpositions.

- **Key:** `chess.polyglot.zobrist_hash(board)`, stored as a signed int64 (`h - (1 << 64) if h >= (1 << 63) else h`).
- **Display string:** `board.epd()` with default arguments, which drops move counters and keeps the en-passant square only when a legal en-passant capture exists.
- **Same position means:** same piece placement, side to move, castling rights, and legal en-passant possibility. Move counters are never part of identity. Castling rights are. Color-flipped positions are different positions.
- **Normalize every external source** (eval DB, openings, puzzles, user imports) through the same function before matching on keys.
- **One `MOVE` edge per (from, to) pair:** aggregate per-game data before loading. This can't be enforced by a database constraint, so the loader must guarantee it.

Required unit tests for the normalization function:

- `1.d4 Nf6 2.c4 e6` and `1.c4 e6 2.d4 Nf6` produce the same key.
- `1.Nf3 Nf6 2.Ng1 Ng8` returns the starting position's key.
- After `1.e4`, the key equals that of the same placement with no en-passant square (no black pawn can capture).
- A position where a legal en-passant capture exists differs from the same placement without it.
- A king that walks out and back (losing castling rights) produces a different key from the original position.

## Data sources

- **Games:** Lichess standard rated monthly PGN dumps (`.pgn.zst`) from database.lichess.org. These are very large; stream-decompress them and never load a whole file into memory.
- **Evals:** Lichess evaluation database (`lichess_db_eval.jsonl.zst`). Normalize its FENs through the identity function before joining.
- **Opening names:** the `lichess-org/chess-openings` repository (TSV files with `eco`, `name`, `pgn`).

## Ingestion rules

- **Filters:** standard variant only, rated games only, no bullet or ultrabullet. Use Lichess's estimated duration, `base + 40 * increment` in seconds: below 180 is bullet (excluded), below 480 is blitz, and anything longer is `slow` (rapid and classical together).
- **Rating brackets:** six buckets, always indexed in this order: `[<1200, 1200–1499, 1500–1799, 1800–2099, 2100–2399, 2400+]`. A game's bracket is the average of both players' ratings. Skip games where the rating difference exceeds 400. This rule is provisional; keep it configurable.
- **Depth cap:** process the first 30 plies of each game (configurable).
- **Pruning:** after aggregation, keep positions with at least 20 games (configurable) and keep only edges whose endpoints both survive. Apply the ply cap using `min_ply`, the shortest route from the start, never the ply at which one particular game reached the position.
- **Evals:** store as a single `eval_cp` integer from White's point of view. Forced mates use a sentinel: `+(100000 - N)` for White mating in N, `-(100000 - N)` for Black. Any mate outranks any material score, and shorter mates outrank longer ones. Decode to "M3" only for display.
- **Aggregation first, loading second:** aggregate into keyed structures (in memory for samples; DuckDB or Parquet partial aggregates at full scale), then bulk load. Create indexes before loading edges, since edges are matched by position key.
- **Loading:** batched `UNWIND $rows` with `CREATE` (not `MERGE`), since data is already deduplicated. Batches of roughly 10k rows to start.

## Performance expectations

Pure `python-chess` is too slow to process a full month (tens of millions of games times 30 plies). Start with a sample of 1–5 million games to measure throughput, node counts, and memory. If full-month scale is needed, move the parse-and-aggregate step to a faster engine (for example the Rust `shakmaty` and `pgn-reader` crates) while keeping the same identity rules and tests.

## Stack

- **Pipeline:** Python 3.11+, `python-chess`, `zstandard`, the `falkordb` Python client, DuckDB for large aggregations.
- **Database:** FalkorDB via Docker (`falkordb/falkordb`), run with `docker compose`.
- **Engine:** Stockfish, server-side, for positions missing from the eval DB and for puzzle verification (multi-PV).
- **API:** FastAPI (planned).
- **Front end:** web app, framework to be decided. Designs will come from a separate design phase.

## Phase plan

Work in this order and report measurements at the end of each phase before moving on.

1. **Environment:** `docker-compose.yml` for FalkorDB, Python project layout, normalization module with the unit tests above.
2. **Sample ingestion:** stream a sample of one month, aggregate, prune, load `Position` and `MOVE` into `chess_core`, create indexes and constraints. Report: games processed, positions and edges kept, load time, and FalkorDB memory use. These numbers decide the ply cap, the frequency floor, and whether `Game` nodes fit.
3. **Enrichment:** compute `min_ply` with a breadth-first pass from the start position; join evals and compute `best_uci`, `best_gap_cp`, `eval_delta_cp`, `is_best`; load `Opening` nodes and `NAMED` / `SUBVARIATION_OF`; compute pawn structures and `HAS_STRUCTURE`.
4. **Puzzle generation:** queries per puzzle type, Stockfish multi-PV verification (uniqueness threshold roughly a 150 cp gap, or winning vs not), frequency floor, `find_rate` from per-bracket edge stats, Glicko seed rating. Report counts per type.
5. **API:** explorer, reached-via, transposition map expansion, puzzles, user import and repertoire endpoints, with caching.
6. **Front end:** built from the design phase's screens.

## Working agreements

- Ask before downloading full monthly dumps or other multi-GB files. Prefer samples while iterating.
- Keep the pipeline streaming, deterministic, and restartable. Thresholds belong in a config file, not in code.
- Keep `docs/SCHEMA.md` in sync with the actual DDL and loaders.
- Report measured numbers, not estimates, when deciding schema or threshold changes.

## Open decisions

- Rating-bracket rule: average of both players vs the mover's rating.
- Final ply cap and frequency floor (decided from phase 2 measurements).
- Whether `Game` nodes for model games fit in memory.
- Front-end framework.
