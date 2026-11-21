# Build the C identity tools / producer.
#   make hash_fen   # Zobrist/EPD parity tool (no engine dependency)
#
# The move-gen engine lives in native/engine (git submodule). Milestone 1
# (hash_fen) is pure and needs no engine; later targets link the engine Source.
CC ?= cc
CFLAGS ?= -O2 -std=c11 -Wall -Wextra

hash_fen: hash_fen.c chessid.c chessid.h polyglot_random.h
	$(CC) $(CFLAGS) -o hash_fen hash_fen.c chessid.c

clean:
	rm -f hash_fen

.PHONY: clean
