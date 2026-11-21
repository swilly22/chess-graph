# Build the C identity tools / producer.
#   make hash_fen   # Zobrist/EPD parity tool (no engine dependency)
#
# The move-gen engine lives in native/engine (git submodule). Milestone 1
# (hash_fen) is pure and needs no engine; later targets link the engine Source.
CC ?= cc
CFLAGS ?= -O2 -std=c11 -Wall -Wextra

ENGINE_SRC := $(wildcard engine/Source/*.c)
ENGINE_INC := -Iengine/Include -Iengine/Source

hash_fen: hash_fen.c chessid.c chessid.h polyglot_random.h
	$(CC) $(CFLAGS) -o hash_fen hash_fen.c chessid.c

replay_games: replay_games.c replay.c replay.h chessid.c chessid.h polyglot_random.h $(ENGINE_SRC)
	$(CC) $(CFLAGS) $(ENGINE_INC) -I. -o replay_games replay_games.c replay.c chessid.c $(ENGINE_SRC)

clean:
	rm -f hash_fen replay_games

.PHONY: clean
