// Native opening-graph producer: stream a Lichess .pgn.zst, filter games with
// the same rules as the Python pipeline, replay the first N plies, and emit
// position/edge visit rows as CSV shards for DuckDB to aggregate.
//
//   producer <input.pgn.zst> <pos_visits.csv> <edge_visits.csv> <limit>
//
// Output columns (consumed by the Python orchestrator via read_csv):
//   pos_visits:  key,epd,is_blitz,bracket
//   edge_visits: frm,t,uci,san,is_blitz,bracket,result
#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "chessid.h"
#include "replay.h"
#include "zstd.h"

// --- config (mirrors config/pipeline.toml) ----------------------------------
#define DEPTH_CAP 30
#define MAX_RATING_DIFF 400
#define TC_INC_MULT 40
#define TC_BULLET_MAX 180
#define TC_BLITZ_MAX 480
static const int BR_EDGES[5] = {1200, 1500, 1800, 2100, 2400};

static int bracket_of(double avg) {
    int b = 0;
    for (int i = 0; i < 5; i++) {
        if (avg < BR_EDGES[i]) break;
        b++;
    }
    return b;
}

// --- per-game header accumulator --------------------------------------------
typedef struct {
    int white_elo, black_elo;  // -1 if missing/unknown
    int tc_base, tc_inc;       // -1 base if missing
    int result;                // 0=w,1=d,2=b, -1 none
    int rated;                 // 0/1
    int variant_ok;            // 1 standard (default), 0 otherwise
    char movetext[32768];
    int mt_len;
} GameHdr;

static void game_reset(GameHdr *g) {
    g->white_elo = g->black_elo = -1;
    g->tc_base = -1;
    g->tc_inc = 0;
    g->result = -1;
    g->rated = 0;
    g->variant_ok = 1;
    g->mt_len = 0;
    g->movetext[0] = '\0';
}

static int parse_int_or_neg1(const char *v) {
    if (v[0] < '0' || v[0] > '9') return -1;
    return atoi(v);
}

static void handle_header(GameHdr *g, const char *key, const char *val) {
    if (!strcmp(key, "WhiteElo")) {
        g->white_elo = parse_int_or_neg1(val);
    } else if (!strcmp(key, "BlackElo")) {
        g->black_elo = parse_int_or_neg1(val);
    } else if (!strcmp(key, "TimeControl")) {
        if (val[0] == '-') {
            g->tc_base = 1000000;
            g->tc_inc = 0;
        } else {
            g->tc_base = atoi(val);
            const char *plus = strchr(val, '+');
            g->tc_inc = plus ? atoi(plus + 1) : 0;
        }
    } else if (!strcmp(key, "Result")) {
        if (!strcmp(val, "1-0")) g->result = 0;
        else if (!strcmp(val, "1/2-1/2")) g->result = 1;
        else if (!strcmp(val, "0-1")) g->result = 2;
        else g->result = -1;
    } else if (!strcmp(key, "Event")) {
        char low[128];
        int i = 0;
        for (; val[i] && i < 127; i++) low[i] = (char)tolower((unsigned char)val[i]);
        low[i] = '\0';
        g->rated = strstr(low, "rated") != NULL;
    } else if (!strcmp(key, "Variant")) {
        g->variant_ok = !strcmp(val, "Standard");
    }
}

// Returns 1 if accepted (and sets *is_blitz, *bracket), 0 if filtered.
static int classify(const GameHdr *g, int *is_blitz, int *bracket) {
    if (!g->variant_ok) return 0;
    if (!g->rated) return 0;
    if (g->white_elo < 0 || g->black_elo < 0) return 0;
    if (abs(g->white_elo - g->black_elo) > MAX_RATING_DIFF) return 0;
    if (g->tc_base < 0) return 0;
    int est = g->tc_base + TC_INC_MULT * g->tc_inc;
    if (est < TC_BULLET_MAX) return 0;  // bullet
    if (g->result < 0) return 0;
    *is_blitz = est < TC_BLITZ_MAX;
    *bracket = bracket_of((g->white_elo + g->black_elo) / 2.0);
    return 1;
}

// --- SAN tokenization -------------------------------------------------------
// Blank out {...} comments and (...) variations in place.
static void strip_annotations(char *s) {
    int brace = 0, paren = 0;
    for (char *p = s; *p; p++) {
        if (brace) {
            if (*p == '}') brace = 0;
            *p = ' ';
        } else if (paren) {
            if (*p == '(') paren++;
            else if (*p == ')') paren--;
            *p = ' ';
        } else if (*p == '{') {
            brace = 1;
            *p = ' ';
        } else if (*p == '(') {
            paren = 1;
            *p = ' ';
        }
    }
}

static int is_result_token(const char *t) {
    return !strcmp(t, "1-0") || !strcmp(t, "0-1") || !strcmp(t, "1/2-1/2") || !strcmp(t, "*");
}

// --- visit emission ---------------------------------------------------------
typedef struct {
    FILE *pos_fp;
    FILE *edge_fp;
    long pos_rows;
    long edge_rows;
} Emitter;

static void emit_pos(Emitter *e, int64_t key, const char *epd, int is_blitz, int bracket) {
    fprintf(e->pos_fp, "%lld,%s,%s,%d\n", (long long)key, epd, is_blitz ? "true" : "false", bracket);
    e->pos_rows++;
}

static void emit_edge(Emitter *e, int64_t frm, int64_t to, const char *uci, const char *san,
                      int is_blitz, int bracket, int result) {
    fprintf(e->edge_fp, "%lld,%lld,%s,%s,%s,%d,%d\n", (long long)frm, (long long)to, uci, san,
            is_blitz ? "true" : "false", bracket, result);
    e->edge_rows++;
}

// Replay the first DEPTH_CAP plies and emit deduped position/edge visits.
static void emit_game(Emitter *e, GameHdr *g, int is_blitz, int bracket) {
    ReplayState st;
    if (replay_init(&st)) return;

    int64_t visited_pos[DEPTH_CAP + 1];
    int64_t edge_from[DEPTH_CAP], edge_to[DEPTH_CAP];
    int nvp = 0, nve = 0;
    char epd[96];

    int64_t from_key = replay_key(&st);
    replay_epd(&st, epd);
    emit_pos(e, from_key, epd, is_blitz, bracket);
    visited_pos[nvp++] = from_key;

    strip_annotations(g->movetext);
    int plies = 0;
    char *save = NULL;
    for (char *tok = strtok_r(g->movetext, " \t\n", &save); tok && plies < DEPTH_CAP;
         tok = strtok_r(NULL, " \t\n", &save)) {
        if (is_result_token(tok)) break;
        if (tok[0] == '$') continue;
        // strip a leading move number ("12." / "12..." / glued "12.e4")
        while (*tok >= '0' && *tok <= '9') tok++;
        while (*tok == '.') tok++;
        if (*tok == '\0') continue;

        // cleaned SAN for emission: drop trailing !?
        char san[16];
        int sl = 0;
        for (const char *c = tok; *c && sl < 15; c++) san[sl++] = *c;
        while (sl > 0 && (san[sl - 1] == '!' || san[sl - 1] == '?')) sl--;
        san[sl] = '\0';

        char uci[6];
        if (replay_san(&st, tok, uci) != 0) break;  // keep valid prefix
        int64_t to_key = replay_key(&st);

        int seen = 0;
        for (int i = 0; i < nvp; i++)
            if (visited_pos[i] == to_key) { seen = 1; break; }
        if (!seen) {
            replay_epd(&st, epd);
            emit_pos(e, to_key, epd, is_blitz, bracket);
            visited_pos[nvp++] = to_key;
        }

        int edge_seen = 0;
        for (int i = 0; i < nve; i++)
            if (edge_from[i] == from_key && edge_to[i] == to_key) { edge_seen = 1; break; }
        if (!edge_seen) {
            emit_edge(e, from_key, to_key, uci, san, is_blitz, bracket, g->result);
            edge_from[nve] = from_key;
            edge_to[nve] = to_key;
            nve++;
        }
        from_key = to_key;
        plies++;
    }
    replay_free(&st);
}

// --- PGN line state machine over the decompressed stream --------------------
typedef struct {
    GameHdr g;
    int in_movetext;
    long scanned, accepted;
    long limit;
    Emitter *e;
    int done;
} Producer;

static void handle_line(Producer *pr, char *line) {
    if (pr->done) return;

    if (line[0] == '[') {
        const char *sp = strchr(line, ' ');
        const char *q1 = strchr(line, '"');
        const char *q2 = q1 ? strchr(q1 + 1, '"') : NULL;
        if (sp && q1 && q2 && sp < q1) {
            char key[32], val[256];
            int kl = (int)(sp - (line + 1));
            if (kl > 31) kl = 31;
            memcpy(key, line + 1, kl);
            key[kl] = '\0';
            int vl = (int)(q2 - (q1 + 1));
            if (vl > 255) vl = 255;
            memcpy(val, q1 + 1, vl);
            val[vl] = '\0';
            handle_header(&pr->g, key, val);
        }
        return;
    }

    if (line[0] == '\0') {  // blank line
        if (!pr->in_movetext) {
            // blank after headers starts movetext (only if we saw any header)
            if (pr->g.tc_base != -1 || pr->g.white_elo != -1 || pr->g.result != -1)
                pr->in_movetext = 1;
            return;
        }
        // blank after movetext ends the game
        pr->scanned++;
        int is_blitz, bracket;
        if (classify(&pr->g, &is_blitz, &bracket)) {
            pr->accepted++;
            emit_game(pr->e, &pr->g, is_blitz, bracket);
            if (pr->limit > 0 && pr->accepted >= pr->limit) pr->done = 1;
        }
        game_reset(&pr->g);
        pr->in_movetext = 0;
        return;
    }

    // non-blank, non-header: movetext
    pr->in_movetext = 1;
    int len = (int)strlen(line);
    if (pr->g.mt_len + len + 1 < (int)sizeof(pr->g.movetext)) {
        if (pr->g.mt_len) pr->g.movetext[pr->g.mt_len++] = ' ';
        memcpy(pr->g.movetext + pr->g.mt_len, line, len);
        pr->g.mt_len += len;
        pr->g.movetext[pr->g.mt_len] = '\0';
    }
}

// --- zstd streaming + line splitting ----------------------------------------
int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr, "usage: %s <input.pgn.zst> <pos.csv> <edge.csv> <limit>\n", argv[0]);
        return 2;
    }
    Chess_Init();

    FILE *in = fopen(argv[1], "rb");
    if (!in) { perror("open input"); return 1; }
    Emitter em = {fopen(argv[2], "w"), fopen(argv[3], "w"), 0, 0};
    if (!em.pos_fp || !em.edge_fp) { perror("open output"); return 1; }
    long limit = atol(argv[4]);

    Producer pr;
    game_reset(&pr.g);
    pr.in_movetext = 0;
    pr.scanned = pr.accepted = 0;
    pr.limit = limit;
    pr.e = &em;
    pr.done = 0;

    size_t in_cap = ZSTD_DStreamInSize();
    size_t out_cap = ZSTD_DStreamOutSize();
    char *in_buf = malloc(in_cap);
    char *out_buf = malloc(out_cap);
    ZSTD_DCtx *dctx = ZSTD_createDCtx();

    // Line accumulator across decompressed chunks.
    size_t line_cap = 1 << 16;
    char *line = malloc(line_cap);
    size_t line_len = 0;

    size_t read;
    while (!pr.done && (read = fread(in_buf, 1, in_cap, in)) > 0) {
        ZSTD_inBuffer input = {in_buf, read, 0};
        while (input.pos < input.size && !pr.done) {
            ZSTD_outBuffer output = {out_buf, out_cap, 0};
            size_t ret = ZSTD_decompressStream(dctx, &output, &input);
            if (ZSTD_isError(ret)) {
                fprintf(stderr, "zstd error: %s\n", ZSTD_getErrorName(ret));
                pr.done = 1;
                break;
            }
            for (size_t i = 0; i < output.pos; i++) {
                char ch = out_buf[i];
                if (ch == '\n') {
                    if (line_len && line[line_len - 1] == '\r') line_len--;
                    line[line_len] = '\0';
                    handle_line(&pr, line);
                    line_len = 0;
                } else if (line_len + 1 < line_cap) {
                    line[line_len++] = ch;
                }
            }
        }
    }

    ZSTD_freeDCtx(dctx);
    free(in_buf); free(out_buf); free(line);
    fclose(in); fclose(em.pos_fp); fclose(em.edge_fp);

    fprintf(stderr, "scanned=%ld accepted=%ld pos_rows=%ld edge_rows=%ld\n", pr.scanned,
            pr.accepted, em.pos_rows, em.edge_rows);
    return 0;
}
