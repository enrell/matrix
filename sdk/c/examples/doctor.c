/* matrix-doctor: diagnose the Matrix operator environment.
 * Usage: matrix-doctor [--binary <path>]. Prints JSON (no secrets). */
#define _POSIX_C_SOURCE 200809L

#include "mx_component.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
    const char *binary = NULL;
    for (int i = 1; i + 1 < argc; i++) {
        if (!strcmp(argv[i], "--binary"))
            binary = argv[++i];
    }
    char *rep = mx_op_doctor(binary);
    if (!rep) {
        fprintf(stderr, "doctor failed\n");
        return 1;
    }
    printf("%s\n", rep);
    int ok = strstr(rep, "\"cli_shape_ok\":true") != NULL;
    free(rep);
    return ok ? 0 : 1;
}
