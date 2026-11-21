// Reads FEN/EPD lines from stdin, prints "<signed_key>\t<epd>" per line.
// Used by the differential test to check Zobrist/EPD parity with python-chess.
#include <stdio.h>
#include <string.h>

#include "chessid.h"

int main(void) {
    char line[256];
    while (fgets(line, sizeof line, stdin)) {
        size_t n = strlen(line);
        while (n && (line[n - 1] == '\n' || line[n - 1] == '\r')) line[--n] = '\0';
        if (n == 0) continue;

        CidPos pos;
        if (cid_from_fen(line, &pos) != 0) {
            printf("ERR\n");
            continue;
        }
        char epd[96];
        cid_epd(&pos, epd);
        printf("%lld\t%s\n", (long long)cid_key(&pos), epd);
    }
    return 0;
}
