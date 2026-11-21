#include "replay.h"

#include <stdbool.h>
#include <string.h>

#include "chessid.h"

#define START_FEN "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"

// engine piece type (1=P,2=R,3=N,4=B,5=Q,6=K) -> python-chess type order.
static const int ENG2PC[7] = {0, 1, 4, 2, 3, 5, 6};

int replay_init(ReplayState *st) {
    st->cr[0] = st->cr[1] = st->cr[2] = st->cr[3] = 1;
    st->board = NULL;
    return Chess_Board_fromFEN(&st->board, START_FEN) != CHESS_SUCCESS;
}

void replay_free(ReplayState *st) {
    if (st->board) Chess_Board_free(&st->board);
    st->board = NULL;
}

static void build_cidpos(const ReplayState *st, CidPos *p) {
    memset(p, 0, sizeof *p);
    p->ep = -1;
    for (int sq = 0; sq < 64; sq++) {
        uint8_t c = st->board->cells[sq];
        if (c == EMPTY) continue;
        int pct = ENG2PC[c & 0x07];
        int white = (c & PIECE_COLOR_MASK) == COLOR_WHITE;
        p->board[sq] = (int8_t)(white ? pct : -pct);
    }
    p->side = (st->board->turn == COLOR_WHITE) ? 0 : 1;
    p->cr[0] = st->cr[0]; p->cr[1] = st->cr[1];
    p->cr[2] = st->cr[2]; p->cr[3] = st->cr[3];
    uint8_t ep = st->board->en_passant_square;
    p->ep = (ep == EMPTY) ? -1 : (int)ep;  // ep is never a1, so 0 == "none"
}

int64_t replay_key(const ReplayState *st) {
    CidPos p;
    build_cidpos(st, &p);
    return cid_key(&p);
}

void replay_epd(const ReplayState *st, char *out) {
    CidPos p;
    build_cidpos(st, &p);
    cid_epd(&p, out);
}

// --- SAN parsing ------------------------------------------------------------

typedef struct {
    int ptype;   // engine piece type
    int dest;    // 0..63
    int ffile;   // disambiguation / capture file, -1 if none
    int frank;   // disambiguation rank, -1 if none
    int promo;   // engine promotion type, 0 if none
    int castle;  // 0 none, 1 kingside, 2 queenside
} San;

static int letter_eng_type(char c) {
    switch (c) {
        case 'N': return 3; case 'B': return 4; case 'R': return 2;
        case 'Q': return 5; case 'K': return 6; default: return 0;
    }
}

static int parse_san(const char *raw, San *p) {
    char s[24];
    int n = 0;
    for (const char *q = raw; *q && n < 23; q++) {
        char ch = *q;
        if (ch == '+' || ch == '#' || ch == '!' || ch == '?') continue;
        s[n++] = ch;
    }
    s[n] = '\0';

    p->ptype = 1; p->dest = -1; p->ffile = -1; p->frank = -1; p->promo = 0; p->castle = 0;
    if (!strcmp(s, "O-O") || !strcmp(s, "0-0")) { p->castle = 1; p->ptype = 6; return 0; }
    if (!strcmp(s, "O-O-O") || !strcmp(s, "0-0-0")) { p->castle = 2; p->ptype = 6; return 0; }

    char *eq = strchr(s, '=');
    if (eq) { p->promo = letter_eng_type(eq[1]); *eq = '\0'; n = (int)strlen(s); }

    int i = 0;
    if (letter_eng_type(s[0])) { p->ptype = letter_eng_type(s[0]); i = 1; }

    char rem[24];
    int rn = 0;
    for (; i < n; i++)
        if (s[i] != 'x') rem[rn++] = s[i];
    rem[rn] = '\0';
    if (rn < 2) return 1;

    char df = rem[rn - 2], dr = rem[rn - 1];
    if (df < 'a' || df > 'h' || dr < '1' || dr > '8') return 1;
    p->dest = (dr - '1') * 8 + (df - 'a');
    for (int k = 0; k < rn - 2; k++) {
        char ch = rem[k];
        if (ch >= 'a' && ch <= 'h') p->ffile = ch - 'a';
        else if (ch >= '1' && ch <= '8') p->frank = ch - '1';
    }
    return 0;
}

// --- geometric candidate generation -----------------------------------------
// The engine's move generator has a broken pin parameter, so we find candidate
// source squares ourselves on the mailbox and let move_legal (engine make-move
// + isSquareAttacked) enforce real legality.

static int abs_i(int x) { return x < 0 ? -x : x; }

static int path_clear(const uint8_t *cells, int from, int to) {
    int ff = from & 7, fr = from >> 3, tf = to & 7, tr = to >> 3;
    int sf = (tf > ff) - (tf < ff);
    int sr = (tr > fr) - (tr < fr);
    int cf = ff + sf, cr = fr + sr;
    while (cf != tf || cr != tr) {
        if (cells[cr * 8 + cf] != EMPTY) return 0;
        cf += sf;
        cr += sr;
    }
    return 1;
}

static int reaches(const uint8_t *cells, int from, int to, int ptype) {
    int df = (to & 7) - (from & 7);
    int dr = (to >> 3) - (from >> 3);
    switch (ptype) {
        case 3: return (abs_i(df) == 1 && abs_i(dr) == 2) || (abs_i(df) == 2 && abs_i(dr) == 1);
        case 6: return abs_i(df) <= 1 && abs_i(dr) <= 1 && (df || dr);
        case 4: return abs_i(df) == abs_i(dr) && df != 0 && path_clear(cells, from, to);
        case 2: return ((df == 0) != (dr == 0)) && path_clear(cells, from, to);
        case 5:
            return (((abs_i(df) == abs_i(dr) && df != 0) || ((df == 0) != (dr == 0))) &&
                    path_clear(cells, from, to));
    }
    return 0;
}

// engine promo type -> promotion flag (optionally the capture variant)
static int promo_to_flag(int promo, int capture) {
    static const int idx[6] = {0, 0, 2, 0, 1, 3};  // by engine type: rook->2, knight->0, bishop->1, queen->3
    int base = capture ? CHESS_MOVE_PROMO_CAPTURE_KNIGHT : CHESS_MOVE_PROMOTION_KNIGHT;
    return base + idx[promo];
}

static Chess_Move mk_move(int from, int to, int flag) {
    return (Chess_Move)(from | (to << 6) | (flag << 12));
}

// --- UCI + legality ---------------------------------------------------------

static void move_to_uci(Chess_Move m, char *out) {
    int from = CHESS_MOVE_FROM(m), to = CHESS_MOVE_TO(m), fl = CHESS_MOVE_FLAGS(m);
    out[0] = (char)('a' + (from & 7));
    out[1] = (char)('1' + (from >> 3));
    out[2] = (char)('a' + (to & 7));
    out[3] = (char)('1' + (to >> 3));
    int k = 4;
    if (fl >= CHESS_MOVE_PROMOTION_KNIGHT) {
        static const char pc[4] = {'n', 'b', 'r', 'q'};
        out[k++] = pc[fl & 3];
    }
    out[k] = '\0';
}

static bool move_legal(const ReplayState *st, Chess_Move m) {
    struct Chess_Board_opaque tmp = *st->board;  // POD struct copy, no heap pointers
    uint8_t mover = st->board->turn;
    if (Chess_Board_movePiece(&tmp, m) != CHESS_SUCCESS) return false;
    uint8_t king = (uint8_t)(KING | mover);
    int ksq = -1;
    for (int sq = 0; sq < 64; sq++)
        if (tmp.cells[sq] == king) { ksq = sq; break; }
    if (ksq < 0) return false;
    uint8_t enemy = (mover == COLOR_WHITE) ? COLOR_BLACK : COLOR_WHITE;
    bool attacked = false;
    Chess_Board_isSquareAttacked(&attacked, &tmp, (uint8_t)ksq, enemy);
    return !attacked;
}

static void update_castling(ReplayState *st, int from, int to, int movedtype, uint8_t mover) {
    if (movedtype == 6) {  // king
        if (mover == COLOR_WHITE) { st->cr[0] = 0; st->cr[1] = 0; }
        else { st->cr[2] = 0; st->cr[3] = 0; }
    }
    if (movedtype == 2) {  // rook leaving a home square
        if (from == 7) st->cr[0] = 0;
        if (from == 0) st->cr[1] = 0;
        if (from == 63) st->cr[2] = 0;
        if (from == 56) st->cr[3] = 0;
    }
    if (to == 7) st->cr[0] = 0;   // capture of a rook on its home square
    if (to == 0) st->cr[1] = 0;
    if (to == 63) st->cr[2] = 0;
    if (to == 56) st->cr[3] = 0;
}

int replay_san(ReplayState *st, const char *san, char uci_out[6]) {
    San p;
    if (parse_san(san, &p)) return 1;
    uint8_t turn = st->board->turn;
    const uint8_t *cells = st->board->cells;
    int ep_sq = (st->board->en_passant_square == EMPTY) ? -1 : (int)st->board->en_passant_square;

    Chess_Move cands[8];
    int nc = 0;

    if (p.castle) {
        int rank = (turn == COLOR_WHITE) ? 0 : 7;
        int kfrom = rank * 8 + 4;
        int kto = (p.castle == 1) ? rank * 8 + 6 : rank * 8 + 2;
        int flag = (p.castle == 1) ? CHESS_MOVE_KING_SIDE_CASTLE : CHESS_MOVE_QUEEN_SIDE_CASTLE;
        cands[nc++] = mk_move(kfrom, kto, flag);
    } else if (p.ptype == 1) {
        int dest = p.dest, drank = dest >> 3;
        uint8_t pawn = (uint8_t)(PAWN | turn);
        int dir = (turn == COLOR_WHITE) ? 8 : -8;
        int promorank = (turn == COLOR_WHITE) ? 7 : 0;
        int dbl_drank = (turn == COLOR_WHITE) ? 3 : 4;

        if (p.ffile < 0) {  // push
            int from1 = dest - dir, mid = dest - dir, from2 = dest - 2 * dir;
            if (cells[dest] == EMPTY && from1 >= 0 && from1 < 64 && cells[from1] == pawn) {
                int flag = (drank == promorank) ? promo_to_flag(p.promo, 0) : CHESS_MOVE_QUIET;
                cands[nc++] = mk_move(from1, dest, flag);
            } else if (drank == dbl_drank && cells[dest] == EMPTY && cells[mid] == EMPTY &&
                       from2 >= 0 && from2 < 64 && cells[from2] == pawn) {
                cands[nc++] = mk_move(from2, dest, CHESS_MOVE_DOUBLE_PAWN_PUSH);
            }
        } else if (dest == ep_sq && cells[dest] == EMPTY) {  // en passant
            int from = (drank - (turn == COLOR_WHITE ? 1 : -1)) * 8 + p.ffile;
            if (from >= 0 && from < 64 && cells[from] == pawn)
                cands[nc++] = mk_move(from, dest, CHESS_MOVE_EN_PASSANT);
        } else {  // normal capture
            int from = (drank - (turn == COLOR_WHITE ? 1 : -1)) * 8 + p.ffile;
            if (from >= 0 && from < 64 && cells[from] == pawn && cells[dest] != EMPTY &&
                (cells[dest] & PIECE_COLOR_MASK) != turn) {
                int flag = (drank == promorank) ? promo_to_flag(p.promo, 1) : CHESS_MOVE_CAPTURE;
                cands[nc++] = mk_move(from, dest, flag);
            }
        }
    } else {  // N, B, R, Q, K (non-castle)
        if (!(cells[p.dest] != EMPTY && (cells[p.dest] & PIECE_COLOR_MASK) == turn)) {
            int flag = (cells[p.dest] == EMPTY) ? CHESS_MOVE_QUIET : CHESS_MOVE_CAPTURE;
            for (int sq = 0; sq < 64 && nc < 8; sq++) {
                uint8_t c = cells[sq];
                if (c == EMPTY || (c & PIECE_COLOR_MASK) != turn || (c & 0x07) != p.ptype) continue;
                if (!reaches(cells, sq, p.dest, p.ptype)) continue;
                if (p.ffile >= 0 && (sq & 7) != p.ffile) continue;
                if (p.frank >= 0 && (sq >> 3) != p.frank) continue;
                cands[nc++] = mk_move(sq, p.dest, flag);
            }
        }
    }

    if (nc == 0) return 2;
    Chess_Move chosen = 0;
    int found = 0;
    for (int i = 0; i < nc; i++)
        if (move_legal(st, cands[i])) { chosen = cands[i]; found = 1; break; }
    if (!found) return 3;

    int from = CHESS_MOVE_FROM(chosen), to = CHESS_MOVE_TO(chosen);
    int movedtype = cells[from] & 0x07;
    move_to_uci(chosen, uci_out);
    update_castling(st, from, to, movedtype, turn);
    if (Chess_Board_movePiece(st->board, chosen) != CHESS_SUCCESS) return 4;
    return 0;
}
