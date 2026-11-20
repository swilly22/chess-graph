# Graph schema

Source of truth for the FalkorDB schema. Keep it in sync with DDL and loaders.

## Conventions

- **Rating-bracket arrays** have length 6, in this order: `[<1200, 1200–1499, 1500–1799, 1800–2099, 2100–2399, 2400+]`.
- **Time controls:** `blitz` and `slow` (rapid and classical). Bullet is excluded.
- **Position keys:** polyglot Zobrist hash as signed int64.
- **Evals:** `eval_cp` in centipawns from White's view; forced mate encoded as `±(100000 - N)`.
- **Dates:** epoch seconds as Integer, or `yyyymm` Integer where noted.

## Core graph: `chess_core`

### Node labels

#### `Position`

| Property | Type | Notes |
|---|---|---|
| `key` | Integer | Zobrist hash. Unique, mandatory |
| `epd` | String | `board.epd()`. Mandatory |
| `side` | String | `w` or `b` |
| `castling` | String | e.g. `KQkq`, `-` |
| `min_ply` | Integer | Shortest route from the start position |
| `games` | Integer | Total games reaching the position, after filters |
| `reach_blitz` | Array<Integer>[6] | Games reaching it, per bracket |
| `reach_slow` | Array<Integer>[6] | Same, for rapid and classical |
| `eval_cp` | Integer | With mate sentinel encoding |
| `eval_depth` | Integer | |
| `eval_source` | String | `lichess_db` or `local` |
| `best_uci` | String | Engine's top move |
| `best_gap_cp` | Integer | Gap between first and second engine lines |

#### `Opening`

| Property | Type | Notes |
|---|---|---|
| `name` | String | Full name. Unique, mandatory |
| `eco` | String | Not unique |
| `family` | String | |
| `variation` | String | Nullable |
| `pgn` | String | Canonical line |
| `ply` | Integer | Length of the canonical line |

#### `PawnStructure`

| Property | Type | Notes |
|---|---|---|
| `key` | Integer | Hash of pawn placement only. Unique |
| `signature` | String | Pawns-only FEN placement field |
| `name` | String | Nullable, curated (e.g. `Carlsbad`) |
| `positions` | Integer | |
| `games` | Integer | |

#### `Puzzle`

| Property | Type | Notes |
|---|---|---|
| `id` | String | Unique, mandatory |
| `type` | String | `punish`, `avoid_trap`, `only_move`, `move_order`, `guess_peer`. Mandatory |
| `solution` | Array<String> | UCI moves. Mandatory |
| `prev_uci` | String | Move leading to the setup position (`punish`) |
| `tc` | String | `blitz` or `slow` |
| `find_rate` | Array<Float>[6] | Empirical rate of finding the solution per bracket |
| `frequency` | Float | Share of games at the target bracket reaching the setup position |
| `eval_gap_cp` | Integer | Solution vs next-best move |
| `rating` | Integer | Glicko rating, seeded from `find_rate` |
| `rating_dev` | Float | |
| `attempts` | Integer | |
| `generator_version` | String | |
| `created_at` | Integer | |

#### `Game` (model and master games only)

| Property | Type | Notes |
|---|---|---|
| `id` | String | Source-prefixed, e.g. `lichess:abc123`. Unique |
| `source` | String | |
| `white_elo`, `black_elo` | Integer | |
| `result` | String | `1-0`, `0-1`, `1/2-1/2` |
| `tc` | String | |
| `played_at` | Integer | |
| `url` | String | |

#### `Player`

| Property | Type | Notes |
|---|---|---|
| `id` | String | Source-prefixed username. Unique |
| `name` | String | |
| `peak_rating` | Integer | |

### Relationship types

| Type | Pattern | Properties |
|---|---|---|
| `MOVE` | `(Position)-[:MOVE]->(Position)` | `uci` String (mandatory), `san` String, `games` Integer, `blitz_w`, `blitz_d`, `blitz_b`, `slow_w`, `slow_d`, `slow_b` Array<Integer>[6], `eval_delta_cp` Integer (mover's view; negative = mistake), `is_best` Boolean, `first_seen` Integer (`yyyymm`), `trend` Float |
| `NAMED` | `(Position)-[:NAMED]->(Opening)` | None. Only on the exact final position of the canonical line |
| `SUBVARIATION_OF` | `(Opening)-[:SUBVARIATION_OF]->(Opening)` | None |
| `HAS_STRUCTURE` | `(Position)-[:HAS_STRUCTURE]->(PawnStructure)` | None |
| `SETUP_AT` | `(Puzzle)-[:SETUP_AT]->(Position)` | None |
| `TARGET` | `(Puzzle)-[:TARGET]->(Position)` | None. `move_order` puzzles only |
| `PLAYED` | `(Player)-[:PLAYED]->(Game)` | `color` String |
| `LEFT_BOOK_AT` | `(Game)-[:LEFT_BOOK_AT]->(Position)` | `ply` Integer, `uci` String |
| `ILLUSTRATES` | `(Game)-[:ILLUSTRATES]->(PawnStructure)` | `note` String, nullable |

One `MOVE` edge per (from, to) pair is enforced by the loader, not the database.

### Indexes

Create indexes before loading relationships, and before creating unique constraints (FalkorDB requires an exact-match range index on the constrained property).

```cypher
CREATE INDEX FOR (p:Position) ON (p.key)
CREATE INDEX FOR (p:Position) ON (p.min_ply, p.games)
CREATE INDEX FOR (o:Opening) ON (o.name)
CREATE INDEX FOR (o:Opening) ON (o.eco)
CREATE INDEX FOR (s:PawnStructure) ON (s.key)
CREATE INDEX FOR (s:PawnStructure) ON (s.name)
CREATE INDEX FOR (z:Puzzle) ON (z.id)
CREATE INDEX FOR (z:Puzzle) ON (z.type, z.rating)
CREATE INDEX FOR (g:Game) ON (g.id)
CREATE INDEX FOR (pl:Player) ON (pl.id)

CREATE INDEX FOR ()-[m:MOVE]-() ON (m.eval_delta_cp, m.games)
CREATE INDEX FOR ()-[m:MOVE]-() ON (m.trend)

CALL db.idx.fulltext.createNodeIndex('Opening', 'name')
CALL db.idx.fulltext.createNodeIndex('Player', 'name')
```

### Constraints

These are FalkorDB commands, not Cypher. Run them through the client's raw command interface (`execute_command`). Unique constraints build asynchronously; confirm they're active before relying on them.

```
GRAPH.CONSTRAINT CREATE chess_core UNIQUE NODE Position PROPERTIES 1 key
GRAPH.CONSTRAINT CREATE chess_core UNIQUE NODE Opening PROPERTIES 1 name
GRAPH.CONSTRAINT CREATE chess_core UNIQUE NODE PawnStructure PROPERTIES 1 key
GRAPH.CONSTRAINT CREATE chess_core UNIQUE NODE Puzzle PROPERTIES 1 id
GRAPH.CONSTRAINT CREATE chess_core UNIQUE NODE Game PROPERTIES 1 id
GRAPH.CONSTRAINT CREATE chess_core UNIQUE NODE Player PROPERTIES 1 id

GRAPH.CONSTRAINT CREATE chess_core MANDATORY NODE Position PROPERTIES 2 key epd
GRAPH.CONSTRAINT CREATE chess_core MANDATORY NODE Puzzle PROPERTIES 2 type solution
GRAPH.CONSTRAINT CREATE chess_core MANDATORY RELATIONSHIP MOVE PROPERTIES 1 uci
```

## Per-user graph: `chess_user_<id>`

References core positions by `key` only. Opponent-scouting graphs reuse this schema, with `User` standing for the scouted player.

### Node labels

#### `User`

| Property | Type | Notes |
|---|---|---|
| `id` | String | Unique |
| `username` | String | |
| `rating_blitz`, `rating_slow` | Integer | |
| `imported_at` | Integer | |

#### `UPosition`

| Property | Type | Notes |
|---|---|---|
| `key` | Integer | Same Zobrist key as core. Unique |
| `epd` | String | |
| `games` | Integer | |
| `score` | Float | User's score from here (0–1) |
| `in_repertoire` | Boolean | |
| `last_seen` | Integer | |

#### `UGame`

| Property | Type | Notes |
|---|---|---|
| `id` | String | Unique |
| `color` | String | |
| `result` | String | `win`, `draw`, `loss` (user's view) |
| `tc` | String | |
| `rating`, `opp_rating` | Integer | |
| `played_at` | Integer | |
| `url` | String | |

#### `Attempt`

| Property | Type | Notes |
|---|---|---|
| `puzzle_id` | String | ID of a core `Puzzle` |
| `solved` | Boolean | |
| `time_ms` | Integer | |
| `rating_before`, `rating_after` | Integer | |
| `at` | Integer | |

### Relationship types

| Type | Pattern | Properties |
|---|---|---|
| `PLAYED_MOVE` | `(UPosition)-[:PLAYED_MOVE]->(UPosition)` | `uci`, `san` String, `games`, `w`, `d`, `l` Integer |
| `REP_MOVE` | `(UPosition)-[:REP_MOVE]->(UPosition)` | `uci`, `san` String, `added_at` Integer, `due_at` Integer, `interval_days` Float, `ease` Float, `reps` Integer, `lapses` Integer |
| `PASSED_THROUGH` | `(UGame)-[:PASSED_THROUGH]->(UPosition)` | `ply` Integer (up to ply 30) |
| `LEFT_BOOK_AT` | `(UGame)-[:LEFT_BOOK_AT]->(UPosition)` | `ply` Integer, `uci` String |
| `ATTEMPTED` | `(User)-[:ATTEMPTED]->(Attempt)` | None |

### Indexes

```cypher
CREATE INDEX FOR (u:User) ON (u.id)
CREATE INDEX FOR (p:UPosition) ON (p.key)
CREATE INDEX FOR (p:UPosition) ON (p.in_repertoire)
CREATE INDEX FOR (g:UGame) ON (g.id)
CREATE INDEX FOR (g:UGame) ON (g.played_at)
CREATE INDEX FOR (a:Attempt) ON (a.puzzle_id)
CREATE INDEX FOR ()-[r:REP_MOVE]-() ON (r.due_at)
```

### Constraints

```
GRAPH.CONSTRAINT CREATE chess_user_<id> UNIQUE NODE User PROPERTIES 1 id
GRAPH.CONSTRAINT CREATE chess_user_<id> UNIQUE NODE UPosition PROPERTIES 1 key
GRAPH.CONSTRAINT CREATE chess_user_<id> UNIQUE NODE UGame PROPERTIES 1 id
GRAPH.CONSTRAINT CREATE chess_user_<id> MANDATORY RELATIONSHIP REP_MOVE PROPERTIES 1 uci
```
