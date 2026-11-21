# Phase 3 — enrichment measurements

Enriched the 1M-sample `chess_core` graph (49,027 positions / 74,443 edges).
All steps are CLI subcommands (`enrich-*`) and self-contained except evals.

## min_ply (BFS shortest route from start)

| | |
|---|---|
| reachable positions | 48,357 |
| orphans (pruning artifact, min_ply unset) | 670 |
| max min_ply | 28 (within the 30-ply cap) |

ply 1 = exactly 20 (the surviving first moves); 0 edges violate
`b.min_ply <= a.min_ply + 1`.

## Pawn structures (PawnStructure + HAS_STRUCTURE)

7,279 distinct structures over 49,027 positions (~6.7 positions each). Top
structure (symmetric e4/e5, 646 positions / 955k games) is shared across
Italian/Ruy/Petrov/etc. Every position maps to exactly one structure.

## Openings (Opening + NAMED + SUBVARIATION_OF)

Source: lichess-org/chess-openings TSVs. 3,177 openings; 1,632 NAMED (canonical
final positions that survived pruning); 3,157 SUBVARIATION_OF (move-prefix tree).
1.e4 c5 -> Sicilian Defense (B20).

## Evals (local Stockfish, MultiPV=2)

Chose local Stockfish over the 21 GB Lichess eval DB: full coverage, our chosen
budget, and Stockfish is needed anyway for puzzle verification. 8 engines,
200 ms/position.

| | |
|---|---|
| positions evaluated | 49,016 (11 terminal skipped) |
| edges with eval_delta_cp + is_best | 74,431 |
| avg depth reached | 13.0 |
| wall time | ~27 min |

Validation: start eval `+0.31`, best move e4; **30% of played moves are the
engine-best**; biggest popular mistakes surfaced (Ng5 -184cp / 2004 games, f5,
g5, Bc5). The eval DB stays reserved for full-month-scale graphs.
