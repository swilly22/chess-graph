// Parallel + streaming native opening-graph producer.
//
//   producer <input|-> <out_dir> <limit> <jobs>
//
// A single reader thread (this thread) streams a .pgn.zst from a file or stdin
// ("-", so `curl <url> | producer -` overlaps download with CPU), decompresses
// (libzstd), splits into games, applies the cheap header filter, and enqueues
// accepted games. A pool of <jobs> worker threads does the expensive replay +
// identity and writes per-worker CSV shards (out_dir/pos_<w>.csv,
// out_dir/edge_<w>.csv) which DuckDB later aggregates.
//
// The chess engine is used unmodified: its lookup tables are read-only after
// Chess_Init(), and every worker owns its own board, so no locking is needed.
#include <ctype.h>
#include <pthread.h>
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
#define MT_COPY_CAP 12288  // movetext bytes kept per game (>= first 30 plies)
#define QUEUE_CAP 2048
static const int BR_EDGES[5] = {1200, 1500, 1800, 2100, 2400};

static int bracket_of(double avg) {
    int b = 0;
    for (int i = 0; i < 5; i++) {
        if (avg < BR_EDGES[i]) break;
        b++;
    }
    return b;
}

// --- work queue (bounded, blocking) -----------------------------------------
typedef struct {
    int is_blitz, bracket, result;
    char *movetext;  // owned; worker frees
} Rec;

typedef struct {
    Rec buf[QUEUE_CAP];
    int head, tail, count, closed;
    pthread_mutex_t m;
    pthread_cond_t not_empty, not_full;
} Queue;

static void q_init(Queue *q) {
    q->head = q->tail = q->count = q->closed = 0;
    pthread_mutex_init(&q->m, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

static void q_push(Queue *q, Rec r) {
    pthread_mutex_lock(&q->m);
    while (q->count == QUEUE_CAP && !q->closed) pthread_cond_wait(&q->not_full, &q->m);
    if (q->closed) {
        pthread_mutex_unlock(&q->m);
        free(r.movetext);
        return;
    }
    q->buf[q->tail] = r;
    q->tail = (q->tail + 1) % QUEUE_CAP;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->m);
}

static int q_pop(Queue *q, Rec *out) {
    pthread_mutex_lock(&q->m);
    while (q->count == 0 && !q->closed) pthread_cond_wait(&q->not_empty, &q->m);
    if (q->count == 0 && q->closed) {
        pthread_mutex_unlock(&q->m);
        return 0;
    }
    *out = q->buf[q->head];
    q->head = (q->head + 1) % QUEUE_CAP;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->m);
    return 1;
}

static void q_close(Queue *q) {
    pthread_mutex_lock(&q->m);
    q->closed = 1;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->m);
}

// --- SAN tokenization (worker side) -----------------------------------------
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
            brace = 1; *p = ' ';
        } else if (*p == '(') {
            paren = 1; *p = ' ';
        }
    }
}

static int is_result_token(const char *t) {
    return !strcmp(t, "1-0") || !strcmp(t, "0-1") || !strcmp(t, "1/2-1/2") || !strcmp(t, "*");
}

// --- worker -----------------------------------------------------------------
typedef struct {
    Queue *q;
    FILE *pos_fp, *edge_fp;
    long pos_rows, edge_rows;
} Worker;

static void emit_rec(Worker *w, Rec *r) {
    ReplayState st;
    if (replay_init(&st)) return;

    int64_t visited_pos[DEPTH_CAP + 1];
    int64_t edge_from[DEPTH_CAP], edge_to[DEPTH_CAP];
    int nvp = 0, nve = 0;
    char epd[96];
    const char *bl = r->is_blitz ? "true" : "false";

    int64_t from_key = replay_key(&st);
    replay_epd(&st, epd);
    fprintf(w->pos_fp, "%lld,%s,%s,%d\n", (long long)from_key, epd, bl, r->bracket);
    w->pos_rows++;
    visited_pos[nvp++] = from_key;

    strip_annotations(r->movetext);
    int plies = 0;
    char *save = NULL;
    for (char *tok = strtok_r(r->movetext, " \t\n", &save); tok && plies < DEPTH_CAP;
         tok = strtok_r(NULL, " \t\n", &save)) {
        if (is_result_token(tok)) break;
        if (tok[0] == '$') continue;
        while (*tok >= '0' && *tok <= '9') tok++;
        while (*tok == '.') tok++;
        if (*tok == '\0') continue;

        char san[16];
        int sl = 0;
        for (const char *c = tok; *c && sl < 15; c++) san[sl++] = *c;
        while (sl > 0 && (san[sl - 1] == '!' || san[sl - 1] == '?')) sl--;
        san[sl] = '\0';

        char uci[6];
        if (replay_san(&st, tok, uci) != 0) break;
        int64_t to_key = replay_key(&st);

        int seen = 0;
        for (int i = 0; i < nvp; i++)
            if (visited_pos[i] == to_key) { seen = 1; break; }
        if (!seen) {
            replay_epd(&st, epd);
            fprintf(w->pos_fp, "%lld,%s,%s,%d\n", (long long)to_key, epd, bl, r->bracket);
            w->pos_rows++;
            visited_pos[nvp++] = to_key;
        }

        int edge_seen = 0;
        for (int i = 0; i < nve; i++)
            if (edge_from[i] == from_key && edge_to[i] == to_key) { edge_seen = 1; break; }
        if (!edge_seen) {
            fprintf(w->edge_fp, "%lld,%lld,%s,%s,%s,%d,%d\n", (long long)from_key,
                    (long long)to_key, uci, san, bl, r->bracket, r->result);
            w->edge_rows++;
            edge_from[nve] = from_key;
            edge_to[nve] = to_key;
            nve++;
        }
        from_key = to_key;
        plies++;
    }
    replay_free(&st);
}

static void *worker_main(void *arg) {
    Worker *w = arg;
    Rec r;
    while (q_pop(w->q, &r)) {
        emit_rec(w, &r);
        free(r.movetext);
    }
    return NULL;
}

// --- reader (header parse + filter) -----------------------------------------
typedef struct {
    int white_elo, black_elo, tc_base, tc_inc, result, rated, variant_ok, mt_len;
    char movetext[32768];
} GameHdr;

static void game_reset(GameHdr *g) {
    g->white_elo = g->black_elo = -1;
    g->tc_base = -1; g->tc_inc = 0; g->result = -1;
    g->rated = 0; g->variant_ok = 1; g->mt_len = 0;
    g->movetext[0] = '\0';
}

static int parse_int_or_neg1(const char *v) {
    if (v[0] < '0' || v[0] > '9') return -1;
    return atoi(v);
}

static void handle_header(GameHdr *g, const char *key, const char *val) {
    if (!strcmp(key, "WhiteElo")) g->white_elo = parse_int_or_neg1(val);
    else if (!strcmp(key, "BlackElo")) g->black_elo = parse_int_or_neg1(val);
    else if (!strcmp(key, "TimeControl")) {
        if (val[0] == '-') { g->tc_base = 1000000; g->tc_inc = 0; }
        else {
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

static int classify(const GameHdr *g, int *is_blitz, int *bracket) {
    if (!g->variant_ok || !g->rated) return 0;
    if (g->white_elo < 0 || g->black_elo < 0) return 0;
    if (abs(g->white_elo - g->black_elo) > MAX_RATING_DIFF) return 0;
    if (g->tc_base < 0) return 0;
    int est = g->tc_base + TC_INC_MULT * g->tc_inc;
    if (est < TC_BULLET_MAX) return 0;
    if (g->result < 0) return 0;
    *is_blitz = est < TC_BLITZ_MAX;
    *bracket = bracket_of((g->white_elo + g->black_elo) / 2.0);
    return 1;
}

typedef struct {
    GameHdr g;
    int in_movetext;
    long scanned, accepted, limit;
    Queue *q;
    int done;
} Reader;

static void finish_game(Reader *rd) {
    rd->scanned++;
    int is_blitz, bracket;
    if (classify(&rd->g, &is_blitz, &bracket)) {
        rd->accepted++;
        int len = rd->g.mt_len;
        if (len > MT_COPY_CAP) len = MT_COPY_CAP;
        char *mt = malloc(len + 1);
        memcpy(mt, rd->g.movetext, len);
        mt[len] = '\0';
        Rec r = {is_blitz, bracket, rd->g.result, mt};
        q_push(rd->q, r);
        if (rd->limit > 0 && rd->accepted >= rd->limit) rd->done = 1;
    }
    game_reset(&rd->g);
    rd->in_movetext = 0;
}

static void handle_line(Reader *rd, char *line) {
    if (rd->done) return;
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
            handle_header(&rd->g, key, val);
        }
        return;
    }
    if (line[0] == '\0') {
        if (!rd->in_movetext) {
            if (rd->g.tc_base != -1 || rd->g.white_elo != -1 || rd->g.result != -1)
                rd->in_movetext = 1;
            return;
        }
        finish_game(rd);
        return;
    }
    rd->in_movetext = 1;
    int len = (int)strlen(line);
    if (rd->g.mt_len + len + 1 < (int)sizeof(rd->g.movetext)) {
        if (rd->g.mt_len) rd->g.movetext[rd->g.mt_len++] = ' ';
        memcpy(rd->g.movetext + rd->g.mt_len, line, len);
        rd->g.mt_len += len;
        rd->g.movetext[rd->g.mt_len] = '\0';
    }
}

// --- main -------------------------------------------------------------------
int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr, "usage: %s <input|-> <out_dir> <limit> <jobs>\n", argv[0]);
        return 2;
    }
    Chess_Init();  // read-only tables, shared by all workers

    FILE *in = strcmp(argv[1], "-") == 0 ? stdin : fopen(argv[1], "rb");
    if (!in) { perror("open input"); return 1; }
    const char *out_dir = argv[2];
    long limit = atol(argv[3]);
    int jobs = atoi(argv[4]);
    if (jobs < 1) jobs = 1;

    Queue q;
    q_init(&q);

    Worker *workers = calloc(jobs, sizeof(Worker));
    pthread_t *threads = calloc(jobs, sizeof(pthread_t));
    for (int i = 0; i < jobs; i++) {
        char path[512];
        snprintf(path, sizeof path, "%s/pos_%d.csv", out_dir, i);
        workers[i].pos_fp = fopen(path, "w");
        snprintf(path, sizeof path, "%s/edge_%d.csv", out_dir, i);
        workers[i].edge_fp = fopen(path, "w");
        workers[i].q = &q;
        if (!workers[i].pos_fp || !workers[i].edge_fp) { perror("open shard"); return 1; }
        pthread_create(&threads[i], NULL, worker_main, &workers[i]);
    }

    Reader rd = {.in_movetext = 0, .scanned = 0, .accepted = 0, .limit = limit, .q = &q, .done = 0};
    game_reset(&rd.g);

    size_t in_cap = ZSTD_DStreamInSize();
    size_t out_cap = ZSTD_DStreamOutSize();
    char *in_buf = malloc(in_cap);
    char *out_buf = malloc(out_cap);
    ZSTD_DCtx *dctx = ZSTD_createDCtx();
    size_t line_cap = 1 << 16;
    char *line = malloc(line_cap);
    size_t line_len = 0;

    size_t read;
    while (!rd.done && (read = fread(in_buf, 1, in_cap, in)) > 0) {
        ZSTD_inBuffer input = {in_buf, read, 0};
        while (input.pos < input.size && !rd.done) {
            ZSTD_outBuffer output = {out_buf, out_cap, 0};
            size_t ret = ZSTD_decompressStream(dctx, &output, &input);
            if (ZSTD_isError(ret)) {
                fprintf(stderr, "zstd error: %s\n", ZSTD_getErrorName(ret));
                rd.done = 1;
                break;
            }
            for (size_t i = 0; i < output.pos; i++) {
                char ch = out_buf[i];
                if (ch == '\n') {
                    if (line_len && line[line_len - 1] == '\r') line_len--;
                    line[line_len] = '\0';
                    handle_line(&rd, line);
                    line_len = 0;
                } else if (line_len + 1 < line_cap) {
                    line[line_len++] = ch;
                }
            }
        }
    }

    q_close(&q);
    for (int i = 0; i < jobs; i++) pthread_join(threads[i], NULL);

    long pos_rows = 0, edge_rows = 0;
    for (int i = 0; i < jobs; i++) {
        fclose(workers[i].pos_fp);
        fclose(workers[i].edge_fp);
        pos_rows += workers[i].pos_rows;
        edge_rows += workers[i].edge_rows;
    }

    ZSTD_freeDCtx(dctx);
    free(in_buf); free(out_buf); free(line); free(workers); free(threads);
    if (in != stdin) fclose(in);

    fprintf(stderr, "scanned=%ld accepted=%ld pos_rows=%ld edge_rows=%ld jobs=%d\n", rd.scanned,
            rd.accepted, pos_rows, edge_rows, jobs);
    return 0;
}
