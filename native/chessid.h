// Position identity in C: Polyglot Zobrist key + canonical EPD, matching
// python-chess byte-for-byte. This is the identity contract the whole C
// producer must honor (see docs/SCHEMA.md "Position identity rules").
#pragma once

#include <stdint.h>

// A position, in our own representation (independent of the move-gen engine, so
// castling/ep identity can never be corrupted by the engine's internal state).
//   board[sq]: 0 empty; +1..+6 white P,N,B,R,Q,K; -1..-6 black (python-chess
//              piece-type order). Square 0 = a1, 63 = h8 (rank-major).
//   side:      0 = white to move, 1 = black.
//   cr[4]:     castling rights WK, WQ, BK, BQ (0/1).
//   ep:        en-passant target square 0..63, or -1 if none.
typedef struct {
    int8_t board[64];
    int side;
    int cr[4];
    int ep;
} CidPos;

// Parse a FEN (or EPD — trailing fields are ignored) into `pos`.
// Returns 0 on success, non-zero on malformed input.
int cid_from_fen(const char *fen, CidPos *pos);

// Unsigned 64-bit Polyglot Zobrist hash.
uint64_t cid_zobrist_u64(const CidPos *pos);

// Signed int64 key (the value stored in FalkorDB): the Zobrist hash folded into
// signed range, identical to python-chess's `h - 2**64 if h >= 2**63 else h`.
int64_t cid_key(const CidPos *pos);

// Write the canonical EPD (placement side castling ep) into `out`
// (>= 90 bytes). Matches board.epd() with default args.
void cid_epd(const CidPos *pos, char *out);
