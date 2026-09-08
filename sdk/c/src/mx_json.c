/* Minimal strict JSON parser/printer for the Matrix C SDK. */
#include "mx_json.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *p;
    const char *end;
    int depth;
    int fail;
} parser_t;

static void skip_ws(parser_t *c) {
    while (c->p < c->end && (*c->p == ' ' || *c->p == '\t' || *c->p == '\n' || *c->p == '\r'))
        c->p++;
}

static mxj_t *parse_value(parser_t *c);

static mxj_t *fresh(mxj_type_t t) {
    mxj_t *v = (mxj_t *)calloc(1, sizeof(*v));
    if (v)
        v->type = t;
    return v;
}

/* Appends one UTF-8 sequence for codepoint cp (validated upstream). */
static int utf8_emit(char **dst, size_t *len, size_t *cap, unsigned long cp) {
    char tmp[4];
    int n = 0;
    if (cp < 0x80) {
        tmp[0] = (char)cp;
        n = 1;
    } else if (cp < 0x800) {
        tmp[0] = (char)(0xC0 | (cp >> 6));
        tmp[1] = (char)(0x80 | (cp & 0x3F));
        n = 2;
    } else if (cp < 0x10000) {
        tmp[0] = (char)(0xE0 | (cp >> 12));
        tmp[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        tmp[2] = (char)(0x80 | (cp & 0x3F));
        n = 3;
    } else {
        tmp[0] = (char)(0xF0 | (cp >> 18));
        tmp[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
        tmp[2] = (char)(0x80 | ((cp >> 6) & 0x3F));
        tmp[3] = (char)(0x80 | (cp & 0x3F));
        n = 4;
    }
    if (*len + (size_t)n + 1 > *cap) {
        size_t ncap = (*cap ? *cap * 2 : 64);
        while (ncap < *len + (size_t)n + 1)
            ncap *= 2;
        char *nb = (char *)realloc(*dst, ncap);
        if (!nb)
            return -1;
        *dst = nb;
        *cap = ncap;
    }
    memcpy(*dst + *len, tmp, (size_t)n);
    *len += (size_t)n;
    (*dst)[*len] = '\0';
    return 0;
}

static int hexval(char c) {
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

/* Parses a JSON string (opening quote already checked). Strict UTF-8:
 * raw bytes must be valid; \u escapes validated (surrogates paired). */
static char *parse_string(parser_t *c) {
    /* c->p at opening quote */
    c->p++;
    char *out = NULL;
    size_t len = 0, cap = 0;
    int ok = 1;
    for (;;) {
        if (c->p >= c->end) {
            ok = 0;
            break;
        }
        unsigned char ch = (unsigned char)*c->p;
        if (ch == '"') {
            c->p++;
            break;
        }
        if (ch < 0x20) {
            ok = 0; /* unescaped control */
            break;
        }
        if (ch == '\\') {
            c->p++;
            if (c->p >= c->end) {
                ok = 0;
                break;
            }
            char e = *c->p++;
            switch (e) {
            case '"':
            case '\\':
            case '/':
                if (utf8_emit(&out, &len, &cap, (unsigned long)(unsigned char)e))
                    ok = 0;
                break;
            case 'b':
                if (utf8_emit(&out, &len, &cap, 8))
                    ok = 0;
                break;
            case 'f':
                if (utf8_emit(&out, &len, &cap, 12))
                    ok = 0;
                break;
            case 'n':
                if (utf8_emit(&out, &len, &cap, 10))
                    ok = 0;
                break;
            case 'r':
                if (utf8_emit(&out, &len, &cap, 13))
                    ok = 0;
                break;
            case 't':
                if (utf8_emit(&out, &len, &cap, 9))
                    ok = 0;
                break;
            case 'u': {
                unsigned long cp = 0;
                for (int i = 0; i < 4; i++) {
                    if (c->p >= c->end) {
                        ok = 0;
                        break;
                    }
                    int h = hexval(*c->p++);
                    if (h < 0) {
                        ok = 0;
                        break;
                    }
                    cp = (cp << 4) | (unsigned long)h;
                }
                if (!ok)
                    break;
                if (cp >= 0xD800 && cp <= 0xDBFF) {
                    /* high surrogate: needs \uDC00-\uDFFF */
                    if (c->end - c->p < 6 || c->p[0] != '\\' || c->p[1] != 'u') {
                        ok = 0;
                        break;
                    }
                    c->p += 2;
                    unsigned long lo = 0;
                    for (int i = 0; i < 4; i++) {
                        int h = hexval(*c->p++);
                        if (h < 0) {
                            ok = 0;
                            break;
                        }
                        lo = (lo << 4) | (unsigned long)h;
                    }
                    if (!ok || lo < 0xDC00 || lo > 0xDFFF) {
                        ok = 0;
                        break;
                    }
                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                } else if (cp >= 0xDC00 && cp <= 0xDFFF) {
                    ok = 0; /* lone low surrogate */
                    break;
                }
                if (utf8_emit(&out, &len, &cap, cp))
                    ok = 0;
                break;
            }
            default:
                ok = 0;
                break;
            }
            if (!ok)
                break;
        } else if (ch < 0x80) {
            if (utf8_emit(&out, &len, &cap, ch))
                ok = 0;
            c->p++;
        } else {
            /* Raw UTF-8 run: validate strictly, then copy verbatim. */
            size_t seqlen;
            unsigned long cp;
            unsigned char b0 = ch;
            if ((b0 & 0xE0) == 0xC0) {
                seqlen = 2;
                cp = b0 & 0x1F;
            } else if ((b0 & 0xF0) == 0xE0) {
                seqlen = 3;
                cp = b0 & 0x0F;
            } else if ((b0 & 0xF8) == 0xF0) {
                seqlen = 4;
                cp = b0 & 0x07;
            } else {
                ok = 0;
                break;
            }
            if (c->p + (long)seqlen > c->end) {
                ok = 0;
                break;
            }
            for (size_t i = 1; i < seqlen; i++) {
                unsigned char bx = (unsigned char)c->p[i];
                if ((bx & 0xC0) != 0x80) {
                    ok = 0;
                    break;
                }
                cp = (cp << 6) | (bx & 0x3F);
            }
            if (!ok)
                break;
            /* reject overlongs, surrogates, >U+10FFFF */
            if ((seqlen == 2 && cp < 0x80) || (seqlen == 3 && cp < 0x800) ||
                (seqlen == 4 && cp < 0x10000) || (cp >= 0xD800 && cp <= 0xDFFF) || cp > 0x10FFFF) {
                ok = 0;
                break;
            }
            if (len + seqlen + 1 > cap) {
                size_t ncap = cap ? cap * 2 : 64;
                while (ncap < len + seqlen + 1)
                    ncap *= 2;
                char *nb = (char *)realloc(out, ncap);
                if (!nb) {
                    ok = 0;
                    break;
                }
                out = nb;
                cap = ncap;
            }
            memcpy(out + len, c->p, seqlen);
            len += seqlen;
            out[len] = '\0';
            c->p += (long)seqlen;
        }
    }
    if (!ok) {
        free(out);
        c->fail = 1;
        return NULL;
    }
    if (!out) {
        out = (char *)malloc(1);
        if (out)
            out[0] = '\0';
    }
    return out;
}

static mxj_t *parse_value(parser_t *c) {
    skip_ws(c);
    if (c->p >= c->end || c->fail)
        return NULL;
    if (c->depth >= MXJ_MAX_DEPTH) {
        c->fail = 1;
        return NULL;
    }
    char ch = *c->p;
    if (ch == '{') {
        c->p++;
        c->depth++;
        mxj_t *v = fresh(MXJ_OBJ);
        mxj_kv_t **tail = v ? &v->obj : NULL;
        skip_ws(c);
        if (c->p < c->end && *c->p == '}') {
            c->p++;
            c->depth--;
            return v;
        }
        while (!c->fail) {
            skip_ws(c);
            if (c->p >= c->end || *c->p != '"') {
                c->fail = 1;
                break;
            }
            char *key = parse_string(c);
            if (!key)
                break;
            skip_ws(c);
            if (c->p >= c->end || *c->p != ':') {
                free(key);
                c->fail = 1;
                break;
            }
            c->p++;
            mxj_t *val = parse_value(c);
            if (!val) {
                free(key);
                break;
            }
            mxj_kv_t *kv = (mxj_kv_t *)calloc(1, sizeof(*kv));
            if (!kv) {
                free(key);
                mxj_free(val);
                c->fail = 1;
                break;
            }
            kv->key = key;
            kv->val = val;
            if (tail) {
                *tail = kv;
                tail = &kv->next;
            }
            skip_ws(c);
            if (c->p < c->end && *c->p == ',') {
                c->p++;
                continue;
            }
            if (c->p < c->end && *c->p == '}') {
                c->p++;
                break;
            }
            c->fail = 1;
            break;
        }
        c->depth--;
        if (c->fail) {
            mxj_free(v);
            return NULL;
        }
        return v;
    }
    if (ch == '[') {
        c->p++;
        c->depth++;
        mxj_t *v = fresh(MXJ_ARR);
        mxj_item_t **tail = v ? &v->arr : NULL;
        skip_ws(c);
        if (c->p < c->end && *c->p == ']') {
            c->p++;
            c->depth--;
            return v;
        }
        while (!c->fail) {
            mxj_t *val = parse_value(c);
            if (!val)
                break;
            mxj_item_t *it = (mxj_item_t *)calloc(1, sizeof(*it));
            if (!it) {
                mxj_free(val);
                c->fail = 1;
                break;
            }
            it->val = val;
            if (tail) {
                *tail = it;
                tail = &it->next;
            }
            skip_ws(c);
            if (c->p < c->end && *c->p == ',') {
                c->p++;
                continue;
            }
            if (c->p < c->end && *c->p == ']') {
                c->p++;
                break;
            }
            c->fail = 1;
            break;
        }
        c->depth--;
        if (c->fail) {
            mxj_free(v);
            return NULL;
        }
        return v;
    }
    if (ch == '"') {
        char *s = parse_string(c);
        if (!s)
            return NULL;
        mxj_t *v = fresh(MXJ_STR);
        if (!v) {
            free(s);
            return NULL;
        }
        v->str = s;
        return v;
    }
    if ((ch >= '0' && ch <= '9') || ch == '-') {
        const char *start = c->p;
        if (ch == '-')
            c->p++;
        int digits = 0;
        while (c->p < c->end && isdigit((unsigned char)*c->p)) {
            c->p++;
            digits++;
        }
        if (c->p < c->end && *c->p == '.') {
            c->p++;
            while (c->p < c->end && isdigit((unsigned char)*c->p)) {
                c->p++;
                digits++;
            }
        }
        if (c->p < c->end && (*c->p == 'e' || *c->p == 'E')) {
            c->p++;
            if (c->p < c->end && (*c->p == '+' || *c->p == '-'))
                c->p++;
            int ed = 0;
            while (c->p < c->end && isdigit((unsigned char)*c->p)) {
                c->p++;
                ed++;
            }
            if (!ed)
                digits = 0;
        }
        if (!digits) {
            c->fail = 1;
            return NULL;
        }
        size_t n = (size_t)(c->p - start);
        mxj_t *v = fresh(MXJ_NUM);
        if (!v)
            return NULL;
        v->str = (char *)malloc(n + 1);
        if (!v->str) {
            mxj_free(v);
            return NULL;
        }
        memcpy(v->str, start, n);
        v->str[n] = '\0';
        return v;
    }
    if ((size_t)(c->end - c->p) >= 4 && !memcmp(c->p, "true", 4)) {
        c->p += 4;
        mxj_t *v = fresh(MXJ_BOOL);
        if (v)
            v->boolean = 1;
        return v;
    }
    if ((size_t)(c->end - c->p) >= 5 && !memcmp(c->p, "false", 5)) {
        c->p += 5;
        return fresh(MXJ_BOOL);
    }
    if ((size_t)(c->end - c->p) >= 4 && !memcmp(c->p, "null", 4)) {
        c->p += 4;
        return fresh(MXJ_NULL);
    }
    c->fail = 1;
    return NULL;
}

mxj_t *mxj_parse(const char *doc) {
    if (!doc)
        return NULL;
    parser_t c;
    c.p = doc;
    c.end = doc + strlen(doc);
    c.depth = 0;
    c.fail = 0;
    mxj_t *v = parse_value(&c);
    if (!v)
        return NULL;
    skip_ws(&c);
    if (c.fail || c.p != c.end) {
        mxj_free(v);
        return NULL;
    }
    return v;
}

void mxj_free(mxj_t *v) {
    if (!v)
        return;
    free(v->str);
    mxj_kv_t *kv = v->obj;
    while (kv) {
        mxj_kv_t *nx = kv->next;
        free(kv->key);
        mxj_free(kv->val);
        free(kv);
        kv = nx;
    }
    mxj_item_t *it = v->arr;
    while (it) {
        mxj_item_t *nx = it->next;
        mxj_free(it->val);
        free(it);
        it = nx;
    }
    free(v);
}

const mxj_t *mxj_field(const mxj_t *obj, const char *key) {
    if (!obj || obj->type != MXJ_OBJ || !key)
        return NULL;
    /* last wins on duplicates */
    const mxj_t *found = NULL;
    for (mxj_kv_t *kv = obj->obj; kv; kv = kv->next)
        if (!strcmp(kv->key, key))
            found = kv->val;
    return found;
}

const char *mxj_str(const mxj_t *v) {
    if (v && v->type == MXJ_STR)
        return v->str;
    return NULL;
}

typedef struct {
    char *buf;
    size_t len, cap;
    int fail;
} printer_t;

static void emit(printer_t *o, const char *s, size_t n) {
    if (o->fail)
        return;
    if (o->len + n + 1 > o->cap) {
        size_t ncap = o->cap ? o->cap * 2 : 128;
        while (ncap < o->len + n + 1)
            ncap *= 2;
        char *nb = (char *)realloc(o->buf, ncap);
        if (!nb) {
            o->fail = 1;
            return;
        }
        o->buf = nb;
        o->cap = ncap;
    }
    memcpy(o->buf + o->len, s, n);
    o->len += n;
    o->buf[o->len] = '\0';
}

static void print_value(printer_t *o, const mxj_t *v);

static void print_string(printer_t *o, const char *s) {
    char *q = mxj_quote(s, strlen(s));
    if (!q) {
        o->fail = 1;
        return;
    }
    emit(o, q, strlen(q));
    free(q);
}

static void print_value(printer_t *o, const mxj_t *v) {
    if (!v) {
        emit(o, "null", 4);
        return;
    }
    switch (v->type) {
    case MXJ_NULL:
        emit(o, "null", 4);
        break;
    case MXJ_BOOL:
        if (v->boolean)
            emit(o, "true", 4);
        else
            emit(o, "false", 5);
        break;
    case MXJ_NUM:
    case MXJ_STR:
        if (v->type == MXJ_NUM)
            emit(o, v->str ? v->str : "0", v->str ? strlen(v->str) : 1);
        else
            print_string(o, v->str ? v->str : "");
        break;
    case MXJ_ARR: {
        emit(o, "[", 1);
        int first = 1;
        for (mxj_item_t *it = v->arr; it; it = it->next) {
            if (!first)
                emit(o, ",", 1);
            first = 0;
            print_value(o, it->val);
        }
        emit(o, "]", 1);
        break;
    }
    case MXJ_OBJ: {
        emit(o, "{", 1);
        int first = 1;
        for (mxj_kv_t *kv = v->obj; kv; kv = kv->next) {
            if (!first)
                emit(o, ",", 1);
            first = 0;
            print_string(o, kv->key);
            emit(o, ":", 1);
            print_value(o, kv->val);
        }
        emit(o, "}", 1);
        break;
    }
    }
}

char *mxj_print(const mxj_t *v) {
    printer_t o;
    o.buf = NULL;
    o.len = 0;
    o.cap = 0;
    o.fail = 0;
    print_value(&o, v);
    if (o.fail) {
        free(o.buf);
        return NULL;
    }
    if (!o.buf) {
        o.buf = (char *)malloc(1);
        if (o.buf)
            o.buf[0] = '\0';
    }
    return o.buf;
}

char *mxj_quote(const char *s, size_t n) {
    if (!s)
        return NULL;
    /* worst case: 6x (every byte -> \u00XX) + quotes + NUL */
    size_t cap = n * 6 + 3;
    char *out = (char *)malloc(cap);
    static const char *hex = "0123456789abcdef";
    size_t w = 0;
    if (!out)
        return NULL;
    out[w++] = '"';
    for (size_t i = 0; i < n; i++) {
        unsigned char ch = (unsigned char)s[i];
        switch (ch) {
        case '"':
            out[w++] = '\\';
            out[w++] = '"';
            break;
        case '\\':
            out[w++] = '\\';
            out[w++] = '\\';
            break;
        case '\b':
            out[w++] = '\\';
            out[w++] = 'b';
            break;
        case '\f':
            out[w++] = '\\';
            out[w++] = 'f';
            break;
        case '\n':
            out[w++] = '\\';
            out[w++] = 'n';
            break;
        case '\r':
            out[w++] = '\\';
            out[w++] = 'r';
            break;
        case '\t':
            out[w++] = '\\';
            out[w++] = 't';
            break;
        default:
            if (ch < 0x20) {
                out[w++] = '\\';
                out[w++] = 'u';
                out[w++] = '0';
                out[w++] = '0';
                out[w++] = hex[ch >> 4];
                out[w++] = hex[ch & 15];
            } else {
                out[w++] = (char)ch;
            }
            break;
        }
    }
    out[w++] = '"';
    out[w] = '\0';
    return out;
}

int mxj_valid_utf8(const char *s, size_t n) {
    size_t i = 0;
    while (i < n) {
        unsigned char b0 = (unsigned char)s[i];
        size_t seqlen;
        unsigned long cp;
        if (b0 < 0x80) {
            i++;
            continue;
        } else if ((b0 & 0xE0) == 0xC0) {
            seqlen = 2;
            cp = b0 & 0x1F;
        } else if ((b0 & 0xF0) == 0xE0) {
            seqlen = 3;
            cp = b0 & 0x0F;
        } else if ((b0 & 0xF8) == 0xF0) {
            seqlen = 4;
            cp = b0 & 0x07;
        } else {
            return 0;
        }
        if (i + seqlen > n)
            return 0;
        for (size_t k = 1; k < seqlen; k++) {
            unsigned char bx = (unsigned char)s[i + k];
            if ((bx & 0xC0) != 0x80)
                return 0;
            cp = (cp << 6) | (bx & 0x3F);
        }
        if ((seqlen == 2 && cp < 0x80) || (seqlen == 3 && cp < 0x800) ||
            (seqlen == 4 && cp < 0x10000) || (cp >= 0xD800 && cp <= 0xDFFF) || cp > 0x10FFFF)
            return 0;
        i += seqlen;
    }
    return 1;
}

unsigned long long mxj_parse_u64(const char *s, int *ok) {
    unsigned long long v = 0;
    if (ok)
        *ok = 0;
    if (!s || !*s)
        return 0;
    for (const char *p = s; *p; p++) {
        if (*p < '0' || *p > '9')
            return 0;
        unsigned d = (unsigned)(*p - '0');
        if (v > (0xFFFFFFFFFFFFFFFFull - d) / 10ull)
            return 0; /* overflow: refuse, never wrap */
        v = v * 10ull + d;
    }
    if (ok)
        *ok = 1;
    return v;
}
