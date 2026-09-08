/* Matrix JSON subset: parse + print (ML1 C SDK, no dependencies).
 *
 * Covers exactly what the component protocol needs: objects with
 * string keys, strings with escapes (strict UTF-8), numbers as raw
 * tokens, booleans, null, nesting (depth cap). Unknown fields are
 * preserved in the DOM; callers pass input/output through as values.
 * Duplicate object keys: last wins (the host validates wire shape).
 */
#ifndef MX_JSON_H
#define MX_JSON_H

#include <stddef.h>

typedef enum {
    MXJ_NULL,
    MXJ_BOOL,
    MXJ_NUM, /* raw token, never converted (no precision loss) */
    MXJ_STR,
    MXJ_ARR,
    MXJ_OBJ
} mxj_type_t;

typedef struct mxj mxj_t;
typedef struct mxj_kv {
    char *key;
    mxj_t *val;
    struct mxj_kv *next;
} mxj_kv_t;
typedef struct mxj_item {
    mxj_t *val;
    struct mxj_item *next;
} mxj_item_t;
struct mxj {
    mxj_type_t type;
    int boolean;      /* MXJ_BOOL */
    char *str;        /* MXJ_NUM (raw) or MXJ_STR (unescaped) */
    mxj_kv_t *obj;    /* MXJ_OBJ */
    mxj_item_t *arr;  /* MXJ_ARR */
};

/* Parses a NUL-terminated document. Returns NULL on any error (strict:
 * trailing garbage, depth over MXJ_MAX_DEPTH, bad escapes, invalid
 * UTF-8 in strings all refuse). */
mxj_t *mxj_parse(const char *doc);
/* Frees a DOM (NULL-safe). */
void mxj_free(mxj_t *v);
/* Object field lookup (NULL when absent/wrong type). */
const mxj_t *mxj_field(const mxj_t *obj, const char *key);
/* String value (NULL unless MXJ_STR). */
const char *mxj_str(const mxj_t *v);
/* Serializes back to JSON (malloc'd, caller frees; NULL on OOM). */
char *mxj_print(const mxj_t *v);
/* Escapes a string as a JSON string literal including quotes (malloc'd). */
char *mxj_quote(const char *s, size_t n);
/* Strict UTF-8 validation (rejects overlongs, surrogates, >U+10FFFF). */
int mxj_valid_utf8(const char *s, size_t n);
/* Decimal u64 parse (full range; 0 + false on garbage/overflow). */
unsigned long long mxj_parse_u64(const char *s, int *ok);

#define MXJ_MAX_DEPTH 64

#endif
