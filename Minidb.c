#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <ctype.h>
#include <errno.h>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
typedef SOCKET socket_t;
#define CLOSESOCKET closesocket
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
typedef int socket_t;
#define CLOSESOCKET close
#endif

#define PAGE_SIZE 4096
#define HDR_SIZE 12
#define INTERIOR 1
#define LEAF 2
#define LEAF_OVERHEAD 10
#define INT_CELL 12
#define MAX_TABLES 8
#define MAX_COLS 8
#define NAME_LEN 24
#define DIM 384
#define VEC_BYTES (DIM * sizeof(float))
#define MAX_PAYLOAD 2300
#define EMBED_HOST "127.0.0.1"
#define EMBED_PORT 8765
#define CHUNK_BYTES 480
#define FILE_NAME_MAX 120
#define TOPK 5
#define T_INT 0
#define T_TEXT 1

typedef struct {
    char name[NAME_LEN];
    uint8_t type;
} Col;

typedef struct {
    char name[NAME_LEN];
    uint32_t root;
    int64_t next_rowid;
    uint32_t ncols;
    Col cols[MAX_COLS];
} Table;

typedef struct {
    char magic[8];
    uint32_t page_count;
    uint32_t free_head;
    uint32_t ntables;
    Table tables[MAX_TABLES];
} Header;

typedef struct {
    FILE *f;
    char path[256];
    uint8_t **pages;
    uint8_t *dirty;
    uint32_t cap;
    uint32_t disk_pages;
    Header *hdr;
    int unsaved;
} DB;

typedef struct {
    int split;
    int64_t key;
    uint32_t right;
} Split;

typedef void (*RowFn)(int64_t key, const uint8_t *payload, int len, void *ctx);

static uint16_t r16(const uint8_t *p) { uint16_t v; memcpy(&v, p, 2); return v; }
static uint32_t r32(const uint8_t *p) { uint32_t v; memcpy(&v, p, 4); return v; }
static int64_t r64(const uint8_t *p) { int64_t v; memcpy(&v, p, 8); return v; }
static void w16(uint8_t *p, uint16_t v) { memcpy(p, &v, 2); }
static void w32(uint8_t *p, uint32_t v) { memcpy(p, &v, 4); }
static void w64(uint8_t *p, int64_t v) { memcpy(p, &v, 8); }

static int ncells(const uint8_t *p) { return r16(p + 2); }
static void set_ncells(uint8_t *p, int n) { w16(p + 2, n); }
static int cstart(const uint8_t *p) { return r16(p + 4); }
static void set_cstart(uint8_t *p, int v) { w16(p + 4, v); }
static uint32_t rptr(const uint8_t *p) { return r32(p + 8); }
static void set_rptr(uint8_t *p, uint32_t v) { w32(p + 8, v); }
static int coff(const uint8_t *p, int i) { return r16(p + HDR_SIZE + 2 * i); }
static void set_coff(uint8_t *p, int i, int off) { w16(p + HDR_SIZE + 2 * i, off); }
static int csize(const uint8_t *p, int off) { return p[0] == LEAF ? LEAF_OVERHEAD + r16(p + off) : INT_CELL; }
static int64_t ckey(const uint8_t *p, int i) { return r64(p + coff(p, i) + (p[0] == LEAF ? 2 : 4)); }
static uint32_t cchild(const uint8_t *p, int i) { return r32(p + coff(p, i)); }

static void init_page(uint8_t *p, int type) {
    memset(p, 0, PAGE_SIZE);
    p[0] = type;
    set_cstart(p, PAGE_SIZE);
}

static int free_total(const uint8_t *p) {
    int used = HDR_SIZE + 2 * ncells(p);
    for (int i = 0; i < ncells(p); i++) used += csize(p, coff(p, i));
    return PAGE_SIZE - used;
}

static void defrag(uint8_t *p) {
    uint8_t tmp[PAGE_SIZE];
    memcpy(tmp, p, PAGE_SIZE);
    int top = PAGE_SIZE;
    for (int i = 0; i < ncells(p); i++) {
        int off = coff(tmp, i), sz = csize(tmp, off);
        top -= sz;
        memcpy(p + top, tmp + off, sz);
        set_coff(p, i, top);
    }
    set_cstart(p, top);
}

static int cell_insert(uint8_t *p, int idx, const uint8_t *cell, int size) {
    int n = ncells(p);
    if (free_total(p) < size + 2) return 0;
    if (cstart(p) - (HDR_SIZE + 2 * n) < size + 2) defrag(p);
    int off = cstart(p) - size;
    memcpy(p + off, cell, size);
    memmove(p + HDR_SIZE + 2 * (idx + 1), p + HDR_SIZE + 2 * idx, 2 * (n - idx));
    set_coff(p, idx, off);
    set_cstart(p, off);
    set_ncells(p, n + 1);
    return 1;
}

static void cell_remove(uint8_t *p, int idx) {
    int n = ncells(p);
    memmove(p + HDR_SIZE + 2 * idx, p + HDR_SIZE + 2 * (idx + 1), 2 * (n - idx - 1));
    set_ncells(p, n - 1);
    if (n == 1) set_cstart(p, PAGE_SIZE);
}

static int lower_bound(const uint8_t *p, int64_t key) {
    int lo = 0, hi = ncells(p);
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (ckey(p, mid) < key) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

static int make_leaf_cell(uint8_t *buf, int64_t key, const uint8_t *payload, int len) {
    w16(buf, len);
    w64(buf + 2, key);
    memcpy(buf + LEAF_OVERHEAD, payload, len);
    return LEAF_OVERHEAD + len;
}

static void make_int_cell(uint8_t *buf, uint32_t child, int64_t key) {
    w32(buf, child);
    w64(buf + 4, key);
}

static uint8_t *get_page(DB *db, uint32_t n) {
    if (n >= db->cap) {
        uint32_t nc = db->cap * 2;
        while (nc <= n) nc *= 2;
        db->pages = realloc(db->pages, nc * sizeof(uint8_t *));
        db->dirty = realloc(db->dirty, nc);
        memset(db->pages + db->cap, 0, (nc - db->cap) * sizeof(uint8_t *));
        memset(db->dirty + db->cap, 0, nc - db->cap);
        db->cap = nc;
    }
    if (!db->pages[n]) {
        db->pages[n] = calloc(1, PAGE_SIZE);
        if (db->f && n < db->disk_pages) {
            fseek(db->f, (long)n * PAGE_SIZE, SEEK_SET);
            if (fread(db->pages[n], PAGE_SIZE, 1, db->f) != 1) memset(db->pages[n], 0, PAGE_SIZE);
        }
    }
    return db->pages[n];
}

static void mark(DB *db, uint32_t n) {
    db->dirty[n] = 1;
    db->unsaved = 1;
}

static uint32_t alloc_page(DB *db) {
    Header *h = db->hdr;
    uint32_t n;
    if (h->free_head) {
        n = h->free_head;
        uint8_t *p = get_page(db, n);
        h->free_head = r32(p);
        memset(p, 0, PAGE_SIZE);
    } else {
        n = h->page_count++;
        get_page(db, n);
    }
    mark(db, n);
    mark(db, 0);
    return n;
}

static void free_page(DB *db, uint32_t n) {
    uint8_t *p = get_page(db, n);
    memset(p, 0, PAGE_SIZE);
    w32(p, db->hdr->free_head);
    db->hdr->free_head = n;
    mark(db, n);
    mark(db, 0);
}

static void split_leaf(DB *db, uint32_t pg, int idx, const uint8_t *cell, int size, Split *s) {
    uint8_t *p = get_page(db, pg);
    uint8_t buf[2 * PAGE_SIZE];
    const uint8_t *cp[64];
    int sz[64], n = ncells(p), m = n + 1, total = 0, pos = 0;
    for (int i = 0; i < m; i++) {
        const uint8_t *src;
        int l;
        if (i == idx) {
            src = cell;
            l = size;
        } else {
            int off = coff(p, i < idx ? i : i - 1);
            src = p + off;
            l = csize(p, off);
        }
        memcpy(buf + pos, src, l);
        cp[i] = buf + pos;
        sz[i] = l;
        pos += l;
        total += l;
    }
    int k, acc = 0;
    for (k = 0; k < m - 1; k++) {
        acc += sz[k];
        if (acc * 2 >= total) break;
    }
    if (k > m - 2) k = m - 2;
    uint32_t rpg = alloc_page(db);
    uint8_t *r = get_page(db, rpg);
    init_page(p, LEAF);
    init_page(r, LEAF);
    for (int i = 0; i <= k; i++) cell_insert(p, i, cp[i], sz[i]);
    for (int i = k + 1; i < m; i++) cell_insert(r, i - k - 1, cp[i], sz[i]);
    mark(db, pg);
    mark(db, rpg);
    s->split = 1;
    s->key = r64(cp[k] + 2);
    s->right = rpg;
}

static void split_interior(DB *db, uint32_t pg, int at, const uint8_t *cell, Split *s) {
    uint8_t *p = get_page(db, pg);
    uint32_t ch[512];
    int64_t ky[512];
    int n = ncells(p), m = n + 1;
    uint32_t old_right = rptr(p);
    for (int i = 0; i < m; i++) {
        if (i == at) {
            ch[i] = r32(cell);
            ky[i] = r64(cell + 4);
        } else {
            int j = i < at ? i : i - 1;
            ch[i] = cchild(p, j);
            ky[i] = ckey(p, j);
        }
    }
    int mid = m / 2;
    uint32_t rpg = alloc_page(db);
    uint8_t *r = get_page(db, rpg);
    uint8_t c[INT_CELL];
    init_page(p, INTERIOR);
    init_page(r, INTERIOR);
    for (int i = 0; i < mid; i++) {
        make_int_cell(c, ch[i], ky[i]);
        cell_insert(p, i, c, INT_CELL);
    }
    set_rptr(p, ch[mid]);
    for (int i = mid + 1; i < m; i++) {
        make_int_cell(c, ch[i], ky[i]);
        cell_insert(r, i - mid - 1, c, INT_CELL);
    }
    set_rptr(r, old_right);
    mark(db, pg);
    mark(db, rpg);
    s->split = 1;
    s->key = ky[mid];
    s->right = rpg;
}

static void insert_rec(DB *db, uint32_t pg, int64_t key, const uint8_t *payload, int len, Split *s) {
    uint8_t *p = get_page(db, pg);
    s->split = 0;
    if (p[0] == LEAF) {
        uint8_t cell[LEAF_OVERHEAD + MAX_PAYLOAD];
        int size = make_leaf_cell(cell, key, payload, len);
        int idx = lower_bound(p, key);
        mark(db, pg);
        if (!cell_insert(p, idx, cell, size)) split_leaf(db, pg, idx, cell, size, s);
        return;
    }
    int n = ncells(p);
    int idx = lower_bound(p, key);
    uint32_t child = idx < n ? cchild(p, idx) : rptr(p);
    Split cs;
    insert_rec(db, child, key, payload, len, &cs);
    if (!cs.split) return;
    uint8_t cell[INT_CELL];
    int at;
    mark(db, pg);
    if (idx < n) {
        int64_t old = ckey(p, idx);
        w64(p + coff(p, idx) + 4, cs.key);
        make_int_cell(cell, cs.right, old);
        at = idx + 1;
    } else {
        set_rptr(p, cs.right);
        make_int_cell(cell, child, cs.key);
        at = n;
    }
    if (!cell_insert(p, at, cell, INT_CELL)) split_interior(db, pg, at, cell, s);
}

static void bt_insert(DB *db, uint32_t root, int64_t key, const uint8_t *payload, int len) {
    Split s;
    insert_rec(db, root, key, payload, len, &s);
    if (!s.split) return;
    uint32_t left = alloc_page(db);
    uint8_t *r = get_page(db, root), *l = get_page(db, left);
    uint8_t c[INT_CELL];
    memcpy(l, r, PAGE_SIZE);
    init_page(r, INTERIOR);
    make_int_cell(c, left, s.key);
    cell_insert(r, 0, c, INT_CELL);
    set_rptr(r, s.right);
    mark(db, root);
    mark(db, left);
}

static int delete_rec(DB *db, uint32_t pg, int64_t key, int *empty) {
    uint8_t *p = get_page(db, pg);
    int n = ncells(p), idx = lower_bound(p, key);
    *empty = 0;
    if (p[0] == LEAF) {
        if (idx >= n || ckey(p, idx) != key) return 0;
        cell_remove(p, idx);
        mark(db, pg);
        *empty = n == 1;
        return 1;
    }
    uint32_t child = idx < n ? cchild(p, idx) : rptr(p);
    int child_empty;
    if (!delete_rec(db, child, key, &child_empty)) return 0;
    if (!child_empty) return 1;
    free_page(db, child);
    mark(db, pg);
    if (idx < n) {
        cell_remove(p, idx);
    } else if (n == 0) {
        *empty = 1;
    } else {
        set_rptr(p, cchild(p, n - 1));
        cell_remove(p, n - 1);
    }
    return 1;
}

static int bt_delete(DB *db, uint32_t root, int64_t key) {
    int empty;
    int ok = delete_rec(db, root, key, &empty);
    uint8_t *r = get_page(db, root);
    if (ok && r[0] == INTERIOR) {
        if (empty) init_page(r, LEAF);
        while (r[0] == INTERIOR && ncells(r) == 0) {
            uint32_t c = rptr(r);
            memcpy(r, get_page(db, c), PAGE_SIZE);
            free_page(db, c);
        }
        mark(db, root);
    }
    return ok;
}

static const uint8_t *bt_find(DB *db, uint32_t pg, int64_t key, int *len) {
    uint8_t *p = get_page(db, pg);
    int n = ncells(p), idx = lower_bound(p, key);
    if (p[0] == LEAF) {
        if (idx >= n || ckey(p, idx) != key) return NULL;
        int off = coff(p, idx);
        *len = r16(p + off);
        return p + off + LEAF_OVERHEAD;
    }
    return bt_find(db, idx < n ? cchild(p, idx) : rptr(p), key, len);
}

static void bt_scan(DB *db, uint32_t pg, RowFn fn, void *ctx) {
    uint8_t *p = get_page(db, pg);
    int n = ncells(p);
    if (p[0] == LEAF) {
        for (int i = 0; i < n; i++) {
            int off = coff(p, i);
            fn(r64(p + off + 2), p + off + LEAF_OVERHEAD, r16(p + off), ctx);
        }
        return;
    }
    for (int i = 0; i < n; i++) bt_scan(db, cchild(p, i), fn, ctx);
    bt_scan(db, rptr(p), fn, ctx);
}

static int socket_send_all(socket_t sock, const char *data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
#ifdef _WIN32
        int n = send(sock, data + sent, (int)((len - sent) > 0x7fffffff ? 0x7fffffff : len - sent), 0);
#else
        ssize_t n = send(sock, data + sent, len - sent, 0);
#endif
        if (n <= 0) return 0;
        sent += (size_t)n;
    }
    return 1;
}

static char *json_escape_text(const char *text) {
    size_t n = strlen(text);
    if (n > (SIZE_MAX - 3) / 6) return NULL;
    char *out = malloc(n * 6 + 3);
    if (!out) return NULL;
    char *p = out;
    for (const unsigned char *s = (const unsigned char *)text; *s; s++) {
        switch (*s) {
            case '"': *p++ = '\\'; *p++ = '"'; break;
            case '\\': *p++ = '\\'; *p++ = '\\'; break;
            case '\b': *p++ = '\\'; *p++ = 'b'; break;
            case '\f': *p++ = '\\'; *p++ = 'f'; break;
            case '\n': *p++ = '\\'; *p++ = 'n'; break;
            case '\r': *p++ = '\\'; *p++ = 'r'; break;
            case '\t': *p++ = '\\'; *p++ = 't'; break;
            default:
                if (*s < 0x20) {
                    static const char hex[] = "0123456789abcdef";
                    *p++ = '\\'; *p++ = 'u'; *p++ = '0'; *p++ = '0';
                    *p++ = hex[*s >> 4]; *p++ = hex[*s & 15];
                } else *p++ = (char)*s;
        }
    }
    *p = 0;
    return out;
}

static int embed(const char *text, float *v) {
#ifdef _WIN32
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        fprintf(stderr, "embedding server: WSAStartup failed\n");
        return 0;
    }
#endif
    int ok = 0;
    char *escaped = json_escape_text(text);
    if (!escaped) goto done;
    size_t body_cap = strlen(escaped) + 32;
    char *body = malloc(body_cap);
    if (!body) { free(escaped); goto done; }
    snprintf(body, body_cap, "{\"text\":\"%s\"}", escaped);
    free(escaped);

    socket_t sock = socket(AF_INET, SOCK_STREAM, 0);
#ifdef _WIN32
    if (sock == INVALID_SOCKET) { free(body); goto done; }
#else
    if (sock < 0) { free(body); goto done; }
#endif
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons(EMBED_PORT);
    if (inet_pton(AF_INET, EMBED_HOST, &addr.sin_addr) != 1 ||
        connect(sock, (struct sockaddr *)&addr, sizeof addr) != 0) {
        fprintf(stderr, "cannot connect to embedding server at http://%s:%d/embed; start your Python server first\n", EMBED_HOST, EMBED_PORT);
        CLOSESOCKET(sock); free(body); goto done;
    }
    size_t req_cap = strlen(body) + 256;
    char *req = malloc(req_cap);
    if (!req) { CLOSESOCKET(sock); free(body); goto done; }
    int req_len = snprintf(req, req_cap,
        "POST /embed HTTP/1.1\r\nHost: %s:%d\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n%s",
        EMBED_HOST, EMBED_PORT, strlen(body), body);
    free(body);
    if (req_len < 0 || (size_t)req_len >= req_cap || !socket_send_all(sock, req, (size_t)req_len)) {
        fprintf(stderr, "failed sending request to embedding server\n");
        free(req); CLOSESOCKET(sock); goto done;
    }
    free(req);

    size_t cap = 8192, used = 0;
    char *resp = malloc(cap + 1);
    if (!resp) { CLOSESOCKET(sock); goto done; }
    size_t header_end = 0, expected = 0;
    for (;;) {
        if (used == cap) {
            if (cap > 16 * 1024 * 1024) break;
            cap *= 2;
            char *nr = realloc(resp, cap + 1);
            if (!nr) break;
            resp = nr;
        }
#ifdef _WIN32
        int got = recv(sock, resp + used, (int)(cap - used), 0);
#else
        ssize_t got = recv(sock, resp + used, cap - used, 0);
#endif
        if (got <= 0) break;
        used += (size_t)got;
        resp[used] = 0;
        if (!header_end) {
            char *sep = strstr(resp, "\r\n\r\n");
            if (sep) {
                header_end = (size_t)(sep - resp) + 4;
                char *cl = strstr(resp, "Content-Length:");
                if (!cl || cl > sep) cl = strstr(resp, "content-length:");
                if (cl && cl < sep) expected = (size_t)strtoul(cl + 15, NULL, 10);
                else { fprintf(stderr, "embedding server response has no Content-Length\n"); break; }
                if (expected > 16 * 1024 * 1024 || header_end + expected > 16 * 1024 * 1024) break;
            }
        }
        if (header_end && used >= header_end + expected) break;
    }
    CLOSESOCKET(sock);
    if (!header_end || used < header_end + expected) {
        fprintf(stderr, "incomplete response from embedding server\n");
        free(resp); goto done;
    }
    if (strncmp(resp, "HTTP/1.1 200", 12) && strncmp(resp, "HTTP/1.0 200", 12)) {
        fprintf(stderr, "embedding server error: %.*s\n", (int)expected, resp + header_end);
        free(resp); goto done;
    }
    char *emb = strstr(resp + header_end, "\"embedding\"");
    char *cur = emb ? strchr(emb, '[') : NULL;
    if (!cur) { fprintf(stderr, "embedding missing in server response\n"); free(resp); goto done; }
    cur++;
    for (int i = 0; i < DIM; i++) {
        while (*cur && (isspace((unsigned char)*cur) || *cur == ',')) cur++;
        char *endp;
        v[i] = strtof(cur, &endp);
        if (endp == cur || !isfinite(v[i])) {
            fprintf(stderr, "invalid embedding vector returned by server at value %d\n", i);
            free(resp); goto done;
        }
        cur = endp;
    }
    while (*cur && isspace((unsigned char)*cur)) cur++;
    if (*cur != ']') {
        fprintf(stderr, "embedding server returned fewer or more than %d dimensions\n", DIM);
        free(resp); goto done;
    }
    free(resp);
    ok = 1;
done:
#ifdef _WIN32
    WSACleanup();
#endif
    return ok;
}

static Table *find_table(DB *db, const char *name) {
    for (uint32_t i = 0; i < db->hdr->ntables; i++)
        if (!strcmp(db->hdr->tables[i].name, name)) return &db->hdr->tables[i];
    return NULL;
}

static Table *add_table(DB *db, const char *name, int ncols, const Col *cols) {
    Header *h = db->hdr;
    Table *t = &h->tables[h->ntables++];
    memset(t, 0, sizeof *t);
    snprintf(t->name, NAME_LEN, "%s", name);
    t->ncols = ncols;
    memcpy(t->cols, cols, ncols * sizeof(Col));
    t->root = alloc_page(db);
    init_page(get_page(db, t->root), LEAF);
    t->next_rowid = 1;
    mark(db, 0);
    return t;
}

static void db_close(DB *db) {
    for (uint32_t i = 0; i < db->cap; i++) free(db->pages[i]);
    free(db->pages);
    free(db->dirty);
    if (db->f) fclose(db->f);
    memset(db, 0, sizeof *db);
}

static int db_open(DB *db, const char *path, int must_exist) {
    memset(db, 0, sizeof *db);
    db->cap = 16;
    db->pages = calloc(db->cap, sizeof(uint8_t *));
    db->dirty = calloc(db->cap, 1);
    snprintf(db->path, sizeof db->path, "%s", path);
    FILE *f = path[0] ? fopen(path, "r+b") : NULL;
    if (f) {
        uint8_t first[PAGE_SIZE];
        if (fread(first, PAGE_SIZE, 1, f) != 1 || memcmp(first, "MINIDB02", 8)) {
            fclose(f);
            db_close(db);
            return -1;
        }
        db->f = f;
        db->disk_pages = ((Header *)first)->page_count;
        db->hdr = (Header *)get_page(db, 0);
        return 0;
    }
    if (must_exist || (path[0] && errno != ENOENT)) {
        db_close(db);
        return -1;
    }
    db->hdr = (Header *)get_page(db, 0);
    memcpy(db->hdr->magic, "MINIDB02", 8);
    db->hdr->page_count = 1;
    mark(db, 0);
    Col docs[3] = {{"file", T_TEXT}, {"chunk", T_INT}, {"text", T_TEXT}};
    add_table(db, "docs", 3, docs);
    db->unsaved = 0;
    return 0;
}

static int db_save(DB *db, const char *path) {
    FILE *f;
    if (db->f && !strcmp(path, db->path)) {
        f = db->f;
    } else {
        for (uint32_t i = 0; i < db->hdr->page_count; i++) {
            get_page(db, i);
            db->dirty[i] = 1;
        }
        f = fopen(path, "w+b");
        if (!f) return -1;
        if (db->f) fclose(db->f);
        db->f = f;
        snprintf(db->path, sizeof db->path, "%s", path);
    }
    for (uint32_t i = 0; i < db->hdr->page_count; i++) {
        if (!db->dirty[i]) continue;
        fseek(f, (long)i * PAGE_SIZE, SEEK_SET);
        if (fwrite(db->pages[i], PAGE_SIZE, 1, f) != 1) return -1;
        db->dirty[i] = 0;
    }
    fflush(f);
    db->disk_pages = db->hdr->page_count;
    db->unsaved = 0;
    return 0;
}

static void print_text(const uint8_t *s, int len, int limit) {
    int show = limit > 0 && len > limit ? limit : len;
    if (show < len) while (show > 0 && (s[show] & 0xC0) == 0x80) show--;
    for (int i = 0; i < show; i++) putchar(isspace(s[i]) ? ' ' : s[i]);
    if (show < len) printf("...");
}

static void print_cols(const Table *t, const uint8_t *pl, int limit) {
    const uint8_t *p = pl + VEC_BYTES;
    for (uint32_t c = 0; c < t->ncols; c++) {
        printf(" | ");
        if (t->cols[c].type == T_INT) {
            printf("%lld", (long long)r64(p));
            p += 8;
        } else {
            int l = r16(p);
            print_text(p + 2, l, limit);
            p += 2 + l;
        }
    }
    putchar('\n');
}

static int64_t insert_row(DB *db, Table *t, char **vals, const char *src) {
    int64_t iv[MAX_COLS];
    size_t size = VEC_BYTES;
    for (uint32_t c = 0; c < t->ncols; c++) {
        if (t->cols[c].type == T_INT) {
            char *end;
            iv[c] = strtoll(vals[c], &end, 10);
            if (end == vals[c] || *end) {
                printf("column %s expects an integer\n", t->cols[c].name);
                return -1;
            }
            size += 8;
        } else {
            size += 2 + strlen(vals[c]);
        }
    }
    if (size > MAX_PAYLOAD) {
        printf("row too large (%zu > %d bytes including the %d byte vector)\n", size, MAX_PAYLOAD, (int)VEC_BYTES);
        return -1;
    }
    uint8_t buf[MAX_PAYLOAD];
    float v[DIM];
    if (!embed(src, v)) return -1;
    memcpy(buf, v, VEC_BYTES);
    size_t pos = VEC_BYTES;
    for (uint32_t c = 0; c < t->ncols; c++) {
        if (t->cols[c].type == T_INT) {
            w64(buf + pos, iv[c]);
            pos += 8;
        } else {
            uint16_t l = strlen(vals[c]);
            w16(buf + pos, l);
            memcpy(buf + pos + 2, vals[c], l);
            pos += 2 + l;
        }
    }
    int64_t key = t->next_rowid++;
    bt_insert(db, t->root, key, buf, (int)pos);
    mark(db, 0);
    return key;
}

static char *tok(char **s) {
    while (isspace((uint8_t)**s)) (*s)++;
    if (!**s) return NULL;
    char *t = *s;
    while (**s && !isspace((uint8_t)**s)) (*s)++;
    if (**s) *(*s)++ = 0;
    return t;
}

static char *trim(char *s) {
    while (isspace((uint8_t)*s)) s++;
    char *e = s + strlen(s);
    while (e > s && isspace((uint8_t)e[-1])) *--e = 0;
    return s;
}

static void cmd_create(DB *db, char *rest) {
    char *name = tok(&rest);
    if (!name || !*rest) { puts("usage: create <table> <col[:int|text]> ..."); return; }
    if (db->hdr->ntables >= MAX_TABLES) { puts("table limit reached"); return; }
    if (strlen(name) >= NAME_LEN || find_table(db, name)) { puts("invalid or duplicate table name"); return; }
    Col cols[MAX_COLS];
    int n = 0;
    char *spec;
    memset(cols, 0, sizeof cols);
    while ((spec = tok(&rest))) {
        if (n >= MAX_COLS) { puts("too many columns"); return; }
        char *colon = strchr(spec, ':');
        int type = T_TEXT;
        if (colon) {
            *colon = 0;
            if (!strcmp(colon + 1, "int")) type = T_INT;
            else if (strcmp(colon + 1, "text")) { puts("types are int or text"); return; }
        }
        if (!*spec || strlen(spec) >= NAME_LEN) { puts("invalid column name"); return; }
        snprintf(cols[n].name, NAME_LEN, "%s", spec);
        cols[n++].type = type;
    }
    add_table(db, name, n, cols);
    printf("created %s\n", name);
}

static void cmd_insert(DB *db, char *rest) {
    char *name = tok(&rest);
    Table *t = name ? find_table(db, name) : NULL;
    if (!t) { puts("usage: insert <table> v1 | v2 | ..."); return; }
    char *vals[MAX_COLS + 1];
    int n = 0;
    char *s = rest;
    while (n <= MAX_COLS) {
        char *bar = strchr(s, '|');
        if (bar) *bar = 0;
        vals[n++] = trim(s);
        if (!bar) break;
        s = bar + 1;
    }
    if (n != (int)t->ncols) { printf("%s expects %u values separated by |\n", t->name, t->ncols); return; }
    static char joined[9000];
    joined[0] = 0;
    for (int i = 0; i < n; i++) {
        strcat(joined, vals[i]);
        strcat(joined, " ");
    }
    int64_t key = insert_row(db, t, vals, joined);
    if (key > 0) printf("inserted %s #%lld\n", t->name, (long long)key);
}

static void cmd_upload(DB *db, char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) { puts("cannot open file"); return; }
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc(n + 1);
    n = (long)fread(buf, 1, n, f);
    buf[n] = 0;
    fclose(f);
    const char *base = path;
    for (const char *c = path; *c; c++) if (*c == '/' || *c == '\\') base = c + 1;
    char fname[FILE_NAME_MAX + 1];
    snprintf(fname, sizeof fname, "%s", base);
    Table *t = find_table(db, "docs");
    long pos = 0;
    int count = 0;
    while (pos < n) {
        while (pos < n && isspace((uint8_t)buf[pos])) pos++;
        if (pos >= n) break;
        long end = pos + CHUNK_BYTES;
        if (end >= n) {
            end = n;
        } else {
            long e = end;
            while (e > pos + CHUNK_BYTES / 2 && !isspace((uint8_t)buf[e])) e--;
            if (e > pos + CHUNK_BYTES / 2) end = e;
            while (end > pos + 1 && ((uint8_t)buf[end] & 0xC0) == 0x80) end--;
        }
        char saved = buf[end];
        buf[end] = 0;
        char *chunk = trim(buf + pos);
        char idx[24];
        snprintf(idx, sizeof idx, "%d", count);
        char *vals[3] = {fname, idx, chunk};
        if (*chunk && insert_row(db, t, vals, chunk) > 0) count++;
        buf[end] = saved;
        pos = end;
    }
    free(buf);
    printf("uploaded %s: %d chunks embedded\n", fname, count);
}

typedef struct { const Table *t; } PrintCtx;

static void print_cb(int64_t key, const uint8_t *pl, int len, void *ctx) {
    (void)len;
    printf("%lld", (long long)key);
    print_cols(((PrintCtx *)ctx)->t, pl, 120);
}

static void count_cb(int64_t key, const uint8_t *pl, int len, void *ctx) {
    (void)key; (void)pl; (void)len;
    (*(long *)ctx)++;
}

static void cmd_select(DB *db, char *rest) {
    char *name = tok(&rest);
    Table *t = name ? find_table(db, name) : NULL;
    if (!t) { puts("usage: select <table>"); return; }
    printf("rowid");
    for (uint32_t c = 0; c < t->ncols; c++) printf(" | %s", t->cols[c].name);
    putchar('\n');
    PrintCtx ctx = {t};
    bt_scan(db, t->root, print_cb, &ctx);
}

static void cmd_get(DB *db, char *rest) {
    char *name = tok(&rest);
    char *id = tok(&rest);
    Table *t = name ? find_table(db, name) : NULL;
    if (!t || !id) { puts("usage: get <table> <rowid>"); return; }
    int len;
    long long key = strtoll(id, NULL, 10);
    const uint8_t *pl = bt_find(db, t->root, key, &len);
    if (!pl) { puts("not found"); return; }
    printf("%lld", key);
    print_cols(t, pl, 0);
}

static void cmd_tables(DB *db) {
    for (uint32_t i = 0; i < db->hdr->ntables; i++) {
        Table *t = &db->hdr->tables[i];
        long rows = 0;
        bt_scan(db, t->root, count_cb, &rows);
        printf("%s (", t->name);
        for (uint32_t c = 0; c < t->ncols; c++)
            printf("%s%s:%s", c ? ", " : "", t->cols[c].name, t->cols[c].type == T_INT ? "int" : "text");
        printf(") - %ld rows\n", rows);
    }
}

static void cmd_delete(DB *db, char *rest) {
    char *name = tok(&rest);
    char *id = tok(&rest);
    Table *t = name ? find_table(db, name) : NULL;
    if (!t || !id) { puts("usage: delete <table> <rowid>"); return; }
    puts(bt_delete(db, t->root, strtoll(id, NULL, 10)) ? "deleted" : "not found");
}

typedef struct {
    const char *name;
    int64_t *keys;
    size_t n, cap;
} FileCtx;

static void file_cb(int64_t key, const uint8_t *pl, int len, void *ctx) {
    (void)len;
    FileCtx *c = ctx;
    const uint8_t *p = pl + VEC_BYTES;
    int l = r16(p);
    if (l != (int)strlen(c->name) || memcmp(p + 2, c->name, l)) return;
    if (c->n == c->cap) {
        c->cap = c->cap ? c->cap * 2 : 16;
        c->keys = realloc(c->keys, c->cap * sizeof(int64_t));
    }
    c->keys[c->n++] = key;
}

static void cmd_deletefile(DB *db, char *rest) {
    if (!*rest) { puts("usage: deletefile <name>"); return; }
    char name[FILE_NAME_MAX + 1];
    snprintf(name, sizeof name, "%s", rest);
    Table *t = find_table(db, "docs");
    FileCtx ctx = {name, NULL, 0, 0};
    bt_scan(db, t->root, file_cb, &ctx);
    for (size_t i = 0; i < ctx.n; i++) bt_delete(db, t->root, ctx.keys[i]);
    printf("deleted %zu chunks\n", ctx.n);
    free(ctx.keys);
}

typedef struct {
    float score;
    int ti;
    int64_t key;
    int len;
    uint8_t pl[MAX_PAYLOAD];
} Hit;

typedef struct {
    float q[DIM];
    Hit top[TOPK];
    int n;
    int ti;
} SearchCtx;

static void search_cb(int64_t key, const uint8_t *pl, int len, void *ctx) {
    SearchCtx *c = ctx;
    float v[DIM];
    memcpy(v, pl, VEC_BYTES);
    double dot = 0;
    for (int i = 0; i < DIM; i++) dot += (double)c->q[i] * v[i];
    float score = (float)dot;
    if (score <= 0 || (c->n == TOPK && score <= c->top[TOPK - 1].score)) return;
    int pos = c->n < TOPK ? c->n : TOPK - 1;
    while (pos > 0 && c->top[pos - 1].score < score) {
        if (pos < TOPK) c->top[pos] = c->top[pos - 1];
        pos--;
    }
    c->top[pos].score = score;
    c->top[pos].ti = c->ti;
    c->top[pos].key = key;
    c->top[pos].len = len;
    memcpy(c->top[pos].pl, pl, len);
    if (c->n < TOPK) c->n++;
}

static void cmd_search(DB *db, char *rest) {
    Table *only = NULL;
    if (!strncmp(rest, "in ", 3)) {
        rest += 3;
        char *name = tok(&rest);
        only = name ? find_table(db, name) : NULL;
        if (!only) { puts("unknown table"); return; }
    }
    rest = trim(rest);
    if (!*rest) { puts("usage: search [in <table>] <query>"); return; }
    SearchCtx *c = calloc(1, sizeof *c);
    if (!c) { puts("out of memory"); return; }
    if (!embed(rest, c->q)) { free(c); return; }
    for (uint32_t i = 0; i < db->hdr->ntables; i++) {
        if (only && only != &db->hdr->tables[i]) continue;
        c->ti = i;
        bt_scan(db, db->hdr->tables[i].root, search_cb, c);
    }
    if (!c->n) puts("no matches");
    for (int i = 0; i < c->n; i++) {
        Table *t = &db->hdr->tables[c->top[i].ti];
        printf("[%.3f] %s #%lld", c->top[i].score, t->name, (long long)c->top[i].key);
        print_cols(t, c->top[i].pl, 160);
    }
    free(c);
}

static void cmd_save(DB *db, char *rest) {
    const char *path = *rest ? rest : db->path;
    if (!*path) { puts("usage: save <path>"); return; }
    puts(db_save(db, path) ? "save failed" : "saved");
}

static void cmd_load(DB *db, char *rest, int force) {
    if (!*rest) { puts("usage: load <path>"); return; }
    if (db->unsaved && !force) { puts("unsaved changes: 'save' first or use 'load!' to discard"); return; }
    DB nd;
    if (db_open(&nd, rest, 1)) { puts("cannot load: missing or not a minidb file"); return; }
    db_close(db);
    *db = nd;
    printf("loaded %s\n", rest);
}

static void help(void) {
    puts("create <table> <col[:int|text]> ...   create a table");
    puts("tables                                list tables and row counts");
    puts("insert <table> v1 | v2 | ...          insert a row (MiniLM embedding via Python server)");
    puts("upload <file>                         chunk and embed a text file into docs");
    puts("embedding requires the Python server on 127.0.0.1:8765");
    puts("search [in <table>] <query>           semantic search over rows and file chunks");
    puts("select <table>                        list rows");
    puts("get <table> <rowid>                   fetch one row");
    puts("delete <table> <rowid>                delete a row");
    puts("deletefile <name>                     delete all chunks of an uploaded file");
    puts("save [path] / load <path>             persist or open a database file");
    puts("exit                                  quit (exit! discards unsaved changes)");
}

int main(int argc, char **argv) {
    DB db;
    if (db_open(&db, argc > 1 ? argv[1] : "", 0)) {
        puts("cannot open database file");
        return 1;
    }
    char line[8192];
    for (;;) {
        printf("minidb> ");
        fflush(stdout);
        if (!fgets(line, sizeof line, stdin)) break;
        char *rest = line;
        char *cmd = tok(&rest);
        if (!cmd) continue;
        rest = trim(rest);
        if (!strcmp(cmd, "create")) cmd_create(&db, rest);
        else if (!strcmp(cmd, "tables")) cmd_tables(&db);
        else if (!strcmp(cmd, "insert")) cmd_insert(&db, rest);
        else if (!strcmp(cmd, "upload")) cmd_upload(&db, rest);
        else if (!strcmp(cmd, "search")) cmd_search(&db, rest);
        else if (!strcmp(cmd, "select")) cmd_select(&db, rest);
        else if (!strcmp(cmd, "get")) cmd_get(&db, rest);
        else if (!strcmp(cmd, "delete")) cmd_delete(&db, rest);
        else if (!strcmp(cmd, "deletefile")) cmd_deletefile(&db, rest);
        else if (!strcmp(cmd, "save")) cmd_save(&db, rest);
        else if (!strcmp(cmd, "load")) cmd_load(&db, rest, 0);
        else if (!strcmp(cmd, "load!")) cmd_load(&db, rest, 1);
        else if (!strcmp(cmd, "help")) help();
        else if (!strcmp(cmd, "exit") || !strcmp(cmd, "quit")) {
            if (db.unsaved) { puts("unsaved changes: 'save' first or use 'exit!' to discard"); continue; }
            break;
        }
        else if (!strcmp(cmd, "exit!")) break;
        else puts("unknown command, type 'help'");
    }
    db_close(&db);
    return 0;
}