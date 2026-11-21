#include "chessid.h"

#include <string.h>

#include "polyglot_random.h"

// piece-type (1..6) -> FEN letter, python-chess order: P N B R Q K
static const char PIECE_LETTER[7] = {' ', 'P', 'N', 'B', 'R', 'Q', 'K'};

static int letter_to_piece(char c) {
    switch (c) {
        case 'P': return 1;  case 'N': return 2;  case 'B': return 3;
        case 'R': return 4;  case 'Q': return 5;  case 'K': return 6;
        case 'p': return -1; case 'n': return -2; case 'b': return -3;
        case 'r': return -4; case 'q': return -5; case 'k': return -6;
        default: return 0;
    }
}

int cid_from_fen(const char *fen, CidPos *pos) {
    memset(pos, 0, sizeof(*pos));
    pos->ep = -1;

    // Field 1: piece placement, ranks 8 -> 1.
    int rank = 7, file = 0;
    const char *p = fen;
    for (; *p && *p != ' '; p++) {
        char c = *p;
        if (c == '/') {
            if (file != 8) return 1;
            rank--;
            file = 0;
        } else if (c >= '1' && c <= '8') {
            file += c - '0';
        } else {
            int piece = letter_to_piece(c);
            if (piece == 0 || file > 7 || rank < 0) return 1;
            pos->board[rank * 8 + file] = (int8_t)piece;
            file++;
        }
    }
    if (rank != 0 || file != 8) return 1;

    while (*p == ' ') p++;
    // Field 2: side to move.
    if (*p == 'w') pos->side = 0;
    else if (*p == 'b') pos->side = 1;
    else return 1;
    p++;
    while (*p == ' ') p++;

    // Field 3: castling rights.
    if (*p == '-') {
        p++;
    } else {
        for (; *p && *p != ' '; p++) {
            switch (*p) {
                case 'K': pos->cr[0] = 1; break;
                case 'Q': pos->cr[1] = 1; break;
                case 'k': pos->cr[2] = 1; break;
                case 'q': pos->cr[3] = 1; break;
                default: return 1;
            }
        }
    }
    while (*p == ' ') p++;

    // Field 4: en-passant target (optional; EPD/FEN both carry it).
    if (*p == '-') {
        pos->ep = -1;
    } else if (p[0] >= 'a' && p[0] <= 'h' && p[1] >= '1' && p[1] <= '8') {
        pos->ep = (p[1] - '1') * 8 + (p[0] - 'a');
    } else if (*p == '\0') {
        pos->ep = -1;  // placement-only string
    } else {
        return 1;
    }
    // Remaining fields (move counters) are irrelevant to identity: ignored.
    return 0;
}

// Is the en-passant square actually capturable by a pawn of the side to move?
// This is the Polyglot rule (pseudo-legal: pin legality is irrelevant).
static int ep_capturable(const CidPos *pos) {
    if (pos->ep < 0) return 0;
    int epfile = pos->ep & 7;
    if (pos->side == 0) {  // white to move: capturer sits a rank below ep
        int base = pos->ep - 8;
        if (base < 0) return 0;
        if (epfile > 0 && pos->board[base - 1] == 1) return 1;
        if (epfile < 7 && pos->board[base + 1] == 1) return 1;
    } else {  // black to move: capturer sits a rank above ep
        int base = pos->ep + 8;
        if (base > 63) return 0;
        if (epfile > 0 && pos->board[base - 1] == -1) return 1;
        if (epfile < 7 && pos->board[base + 1] == -1) return 1;
    }
    return 0;
}

// Is `sq` attacked by the given side? Used only for the EPD's legal-ep rule.
// Piece codes: ±1 pawn, ±2 knight, ±3 bishop, ±4 rook, ±5 queen, ±6 king.
static int attacked(const int8_t b[64], int sq, int by_white) {
    int s = by_white ? 1 : -1;
    int f = sq & 7, r = sq >> 3;

    if (by_white) {
        if (f < 7 && sq - 7 >= 0 && b[sq - 7] == 1) return 1;
        if (f > 0 && sq - 9 >= 0 && b[sq - 9] == 1) return 1;
    } else {
        if (f > 0 && sq + 7 < 64 && b[sq + 7] == -1) return 1;
        if (f < 7 && sq + 9 < 64 && b[sq + 9] == -1) return 1;
    }

    static const int kn[8][2] = {
        {-1, -2}, {1, -2}, {-2, -1}, {2, -1}, {-2, 1}, {2, 1}, {-1, 2}, {1, 2}};
    for (int i = 0; i < 8; i++) {
        int nf = f + kn[i][0], nr = r + kn[i][1];
        if (nf >= 0 && nf < 8 && nr >= 0 && nr < 8 && b[nr * 8 + nf] == 2 * s) return 1;
    }

    for (int dr = -1; dr <= 1; dr++)
        for (int dc = -1; dc <= 1; dc++) {
            if (!dr && !dc) continue;
            int nf = f + dc, nr = r + dr;
            if (nf >= 0 && nf < 8 && nr >= 0 && nr < 8 && b[nr * 8 + nf] == 6 * s) return 1;
        }

    static const int diag[4][2] = {{-1, -1}, {1, -1}, {-1, 1}, {1, 1}};
    for (int i = 0; i < 4; i++) {
        int nf = f, nr = r;
        for (;;) {
            nf += diag[i][0];
            nr += diag[i][1];
            if (nf < 0 || nf > 7 || nr < 0 || nr > 7) break;
            int pc = b[nr * 8 + nf];
            if (pc == 0) continue;
            if (pc == 3 * s || pc == 5 * s) return 1;  // bishop or queen
            break;
        }
    }

    static const int orth[4][2] = {{0, -1}, {0, 1}, {-1, 0}, {1, 0}};
    for (int i = 0; i < 4; i++) {
        int nf = f, nr = r;
        for (;;) {
            nf += orth[i][0];
            nr += orth[i][1];
            if (nf < 0 || nf > 7 || nr < 0 || nr > 7) break;
            int pc = b[nr * 8 + nf];
            if (pc == 0) continue;
            if (pc == 4 * s || pc == 5 * s) return 1;  // rook or queen
            break;
        }
    }
    return 0;
}

static int find_king(const int8_t b[64], int white) {
    int target = white ? 6 : -6;
    for (int i = 0; i < 64; i++)
        if (b[i] == target) return i;
    return -1;
}

// Does a *legal* en-passant capture exist (pseudo-capturable AND not leaving the
// mover's king in check)? This is python-chess's board.epd() rule.
static int ep_legal(const CidPos *pos) {
    if (pos->ep < 0) return 0;
    int ep = pos->ep, f = ep & 7;
    int8_t tmp[64];

    if (pos->side == 0) {  // white captures; black pawn removed from ep-8
        int captured = ep - 8;
        int caps[2] = {f > 0 ? ep - 9 : -1, f < 7 ? ep - 7 : -1};
        for (int k = 0; k < 2; k++) {
            int cap = caps[k];
            if (cap < 0 || cap > 63 || pos->board[cap] != 1) continue;
            memcpy(tmp, pos->board, 64);
            tmp[cap] = 0; tmp[captured] = 0; tmp[ep] = 1;
            int ksq = find_king(tmp, 1);
            if (ksq >= 0 && !attacked(tmp, ksq, 0)) return 1;
        }
    } else {  // black captures; white pawn removed from ep+8
        int captured = ep + 8;
        int caps[2] = {f > 0 ? ep + 7 : -1, f < 7 ? ep + 9 : -1};
        for (int k = 0; k < 2; k++) {
            int cap = caps[k];
            if (cap < 0 || cap > 63 || pos->board[cap] != -1) continue;
            memcpy(tmp, pos->board, 64);
            tmp[cap] = 0; tmp[captured] = 0; tmp[ep] = -1;
            int ksq = find_king(tmp, 0);
            if (ksq >= 0 && !attacked(tmp, ksq, 1)) return 1;
        }
    }
    return 0;
}

uint64_t cid_zobrist_u64(const CidPos *pos) {
    uint64_t h = 0;

    for (int sq = 0; sq < 64; sq++) {
        int piece = pos->board[sq];
        if (piece == 0) continue;
        int type = piece < 0 ? -piece : piece;  // 1..6 (python-chess order)
        // python-chess enumerates occupied_co = [BLACK, WHITE], so pivot is
        // 1 for white and 0 for black (matches standard Polyglot kind encoding).
        int pivot = piece > 0 ? 1 : 0;
        int piece_index = (type - 1) * 2 + pivot;
        h ^= POLYGLOT_RANDOM[64 * piece_index + sq];
    }

    if (pos->cr[0]) h ^= POLYGLOT_RANDOM[768];
    if (pos->cr[1]) h ^= POLYGLOT_RANDOM[769];
    if (pos->cr[2]) h ^= POLYGLOT_RANDOM[770];
    if (pos->cr[3]) h ^= POLYGLOT_RANDOM[771];

    if (ep_capturable(pos)) h ^= POLYGLOT_RANDOM[772 + (pos->ep & 7)];

    if (pos->side == 0) h ^= POLYGLOT_RANDOM[780];  // white to move
    return h;
}

int64_t cid_key(const CidPos *pos) {
    return (int64_t)cid_zobrist_u64(pos);  // two's-complement reinterpret == signed fold
}

void cid_epd(const CidPos *pos, char *out) {
    char *o = out;

    // Placement, ranks 8 -> 1.
    for (int rank = 7; rank >= 0; rank--) {
        int empties = 0;
        for (int file = 0; file < 8; file++) {
            int piece = pos->board[rank * 8 + file];
            if (piece == 0) {
                empties++;
                continue;
            }
            if (empties) { *o++ = (char)('0' + empties); empties = 0; }
            int type = piece < 0 ? -piece : piece;
            char letter = PIECE_LETTER[type];
            *o++ = piece < 0 ? (char)(letter - 'A' + 'a') : letter;
        }
        if (empties) *o++ = (char)('0' + empties);
        if (rank) *o++ = '/';
    }

    *o++ = ' ';
    *o++ = pos->side == 0 ? 'w' : 'b';
    *o++ = ' ';

    if (pos->cr[0] || pos->cr[1] || pos->cr[2] || pos->cr[3]) {
        if (pos->cr[0]) *o++ = 'K';
        if (pos->cr[1]) *o++ = 'Q';
        if (pos->cr[2]) *o++ = 'k';
        if (pos->cr[3]) *o++ = 'q';
    } else {
        *o++ = '-';
    }

    *o++ = ' ';
    // EPD shows the ep square only when a *legal* capture exists (pin-aware);
    // the Zobrist key, by contrast, uses the pseudo rule (ep_capturable).
    if (ep_legal(pos)) {
        *o++ = (char)('a' + (pos->ep & 7));
        *o++ = (char)('1' + (pos->ep >> 3));
    } else {
        *o++ = '-';
    }
    *o = '\0';
}
