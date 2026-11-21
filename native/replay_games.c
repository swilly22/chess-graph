// Reads one game per stdin line (space-separated SAN tokens, no move numbers)
// and prints two lines per game:
//   line 1: position keys  (start, then after each move), space-separated
//   line 2: UCI moves, space-separated
// Used by the SAN-replay differential test against python-chess.
#include <stdio.h>
#include <string.h>

#include "replay.h"

int main(void) {
    Chess_Init();

    char line[16384];
    while (fgets(line, sizeof line, stdin)) {
        size_t n = strlen(line);
        while (n && (line[n - 1] == '\n' || line[n - 1] == '\r')) line[--n] = '\0';

        ReplayState st;
        replay_init(&st);
        printf("%lld", (long long)replay_key(&st));

        char ucis[16384];
        int up = 0;
        ucis[0] = '\0';
        char uci[6];
        char *save = NULL;
        for (char *tok = strtok_r(line, " ", &save); tok; tok = strtok_r(NULL, " ", &save)) {
            int rc = replay_san(&st, tok, uci);
            if (rc != 0) {
                fprintf(stderr, "replay fail rc=%d tok=%s\n", rc, tok);
                break;
            }
            printf(" %lld", (long long)replay_key(&st));
            up += snprintf(ucis + up, sizeof(ucis) - up, "%s%s", up ? " " : "", uci);
        }
        printf("\n%s\n", ucis);
        replay_free(&st);
    }
    return 0;
}
