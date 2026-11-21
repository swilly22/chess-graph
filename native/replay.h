// SAN game replay on top of the swilly22/chess engine, producing per-ply
// position identity (key + EPD) that matches python-chess.
//
// The engine supplies move generation + make-move; this layer adds SAN parsing
// and tracks castling rights itself (the engine's castling bitmask is lossy for
// independent rook moves, and its generator emits neither castles-after-rook-move
// nor en-passant captures — both of which we construct directly).
#pragma once

#include <stdint.h>

#include "chess.h"

typedef struct {
    Chess_Board board;
    int cr[4];  // castling rights: WK, WQ, BK, BQ (0/1), tracked independently
} ReplayState;

// Initialise to the standard start position. Returns 0 on success.
int replay_init(ReplayState *st);
void replay_free(ReplayState *st);

// Resolve `san` to a legal move, write its UCI into `uci_out` (>= 6 bytes),
// apply it, and update castling rights. Returns 0 on success, non-zero on
// parse failure / no legal match.
int replay_san(ReplayState *st, const char *san, char uci_out[6]);

// Identity of the current position (matches python-chess).
int64_t replay_key(const ReplayState *st);
void replay_epd(const ReplayState *st, char *out);  // out >= 96 bytes
