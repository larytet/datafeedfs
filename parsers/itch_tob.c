/* itch_tob: read a classic pcap on stdin carrying MoldUDP64 / Nasdaq TotalView-ITCH 5.0,
 * rebuild the order book for the chosen symbols and write one record per top-of-book change or trade.
 * Usage: xz -dc day.pcap.xz | itch_tob (--symbols SPY,QQQ | --symbols-file list.txt) --out day.bin|- [--port N] [--table-bits 22]
 *                                      [--stop-at SECONDS_SINCE_MIDNIGHT] [--stats per_symbol.csv] [--depth N | --events]
 * Without --depth a record is the top of book (Record, 43 bytes) and a Q row is written when the best bid or ask changes.
 * With --depth N (1..64) a record is a DepthHeader (27 bytes) followed by 4*N uint32: N bid levels then N ask levels, best first, each as (price, size),
 * zero when the level does not exist; a Q row is written when any of the top N levels changes.
 * With --events a record is an EventRecord (44 bytes): every order message of the chosen symbols as it came, no book (see EventRecord for the fields).
 *        xz -dc day.pcap.xz | itch_tob --list-symbols      prints the symbols of the day's stock directory, one per line, then stops
 * Duplicate packets (A/B feeds, retransmits) are dropped by MoldUDP64 sequence number.
 * --protocol pitch decodes Cboe US equities PITCH (EDGA, EDGX, BYX, BZX) instead: little-endian Sequenced Unit Headers, several units each with its own
 * sequence number, which is dropped when a packet repeats. The record's seq is (unit << 32 | unit sequence). PITCH has no symbol directory: --list-symbols reads add
 * orders until the end of the pcap (or --stop-at) and prints the symbols it saw, sorted. Order Executed at Price/Size is event 'C', Modify is 'U' with the
 * same order id on both sides, Trade (non-displayed) is 'P'. Auction summaries ('Q' / cross rows) are not decoded.
 */
#define _GNU_SOURCE
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#pragma pack(push, 1)
typedef struct {
    uint64_t itch_ns;   /* nanoseconds since midnight (exchange time) */
    uint64_t seq;       /* MoldUDP64 sequence number of the message */
    uint32_t bid_px, bid_sz, ask_px, ask_sz;  /* prices in 1/10000 dollar, state before a trade / after a book change */
    uint32_t trade_px, trade_sz;
    uint16_t sym;       /* index in the --symbols list */
    uint8_t event;      /* 'Q' top of book changed, 'T' trade, 'X' cross */
} Record;
typedef struct {    /* --depth: followed by 4*depth uint32, bid (px, sz) pairs best first, then ask pairs */
    uint64_t itch_ns, seq;
    uint32_t trade_px, trade_sz;
    uint16_t sym;
    uint8_t event;
} DepthHeader;
typedef struct {    /* --events: one order message */
    uint64_t itch_ns, seq;
    uint64_t ref;       /* order reference number; the old one for 'U'; 0 for 'P' and 'Q' */
    uint64_t new_ref;   /* 'U' only: the order reference of the replacement */
    uint32_t px;        /* 1/10000 dollar: the order's price ('A', 'E', 'X', 'D', 'U' new price), the execution price for 'C', 'P', 'Q' */
    uint32_t shares;    /* 'A' added, 'E' executed, 'C' executed, 'X' cancelled, 'D' remaining before the delete, 'U' new size, 'P' and 'Q' traded */
    uint16_t sym;
    uint8_t type;       /* 'A' add, 'E' execute at the order price, 'C' execute at another price, 'X' cancel, 'D' delete, 'U' replace, 'P' non-displayed trade, 'Q' cross */
    uint8_t side;       /* 'B', 'S', or 0 for 'Q' */
} EventRecord;
#pragma pack(pop)

#define MAX_DEPTH 64

typedef struct { uint32_t px; uint32_t qty; } Level;
typedef struct { Level *levels; int count, capacity; } Side;
typedef struct {
    char name[9];
    Side bid, ask;  /* bid ascending by price, ask descending, so the best level is always the last one */
    uint32_t last_bid_px, last_bid_sz, last_ask_px, last_ask_sz;
    int have_last;
    uint32_t *last_depth;   /* --depth: the levels of the last Q row, 4*depth values */
} Book;
typedef struct { uint64_t ref; uint32_t px, shares; uint16_t sym; uint8_t side, used; } Order;

#define MAX_SYMBOLS 4096
static Book books[MAX_SYMBOLS];
static int symbol_count = 0;
static int16_t locate_to_symbol[65536];
static Order *table;
static uint64_t table_mask, table_count, table_capacity;
static int table_bits = 22;
static int events_mode;     /* --events: EventRecord per order message, no book rows */
static int depth;           /* 0: top-of-book Record; otherwise the number of levels per side */
static FILE *out;
static int list_mode, stop_requested;
static uint64_t cur_itch_ns, cur_seq, rows_written;
static uint64_t rows_by_event[MAX_SYMBOLS][3];  /* Q, T, X */
static uint64_t anomalies_missing_level;
static int pitch;           /* --protocol pitch */
static uint64_t cur_base_ns;            /* PITCH: seconds since midnight of the last Time message, in ns */
static uint64_t sym_keys[1 << 14];      /* PITCH: packed symbol -> index in books + 1 (open addressing) */
static uint16_t sym_vals[1 << 14];
static uint64_t list_keys[1 << 16];     /* PITCH --list-symbols: every packed symbol seen */

static void normalise(char *name) {  /* ITCH writes class shares as 'BRK B', Polygon as 'BRK.B' or 'BRK-B' */
    for (; *name; name++) if (*name == ' ' || *name == '-') *name = '.';
}

static inline uint16_t be16(const uint8_t *p) { return (uint16_t)(p[0] << 8 | p[1]); }
static inline uint32_t be32(const uint8_t *p) { return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 | (uint32_t)p[2] << 8 | p[3]; }
static inline uint64_t be48(const uint8_t *p) { return (uint64_t)be16(p) << 32 | be32(p + 2); }
static inline uint64_t be64(const uint8_t *p) { return (uint64_t)be32(p) << 32 | be32(p + 4); }
static inline uint16_t le16(const uint8_t *p) { return (uint16_t)(p[0] | p[1] << 8); }
static inline uint32_t le32(const uint8_t *p) { return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }
static inline uint64_t le64(const uint8_t *p) { return (uint64_t)le32(p + 4) << 32 | le32(p); }

static inline uint64_t hash_ref(uint64_t ref) { return (ref * 0x9E3779B97F4A7C15ULL) >> (64 - table_bits); }

static Order *order_find(uint64_t ref) {
    for (uint64_t i = hash_ref(ref);; i = (i + 1) & table_mask) {
        if (!table[i].used) return NULL;
        if (table[i].ref == ref) return &table[i];
    }
}

static void order_insert(uint64_t ref, uint16_t sym, uint8_t side, uint32_t px, uint32_t shares) {
    if (table_count * 10 > table_capacity * 6) { fprintf(stderr, "order table too full, raise --table-bits\n"); exit(2); }
    uint64_t i = hash_ref(ref);
    while (table[i].used && table[i].ref != ref) i = (i + 1) & table_mask;
    if (!table[i].used) table_count++;
    table[i] = (Order){ref, px, shares, sym, side, 1};
}

static void order_erase(Order *order) {  /* linear probing with backward shift deletion, no tombstones */
    uint64_t i = (uint64_t)(order - table), j = i;
    for (;;) {
        j = (j + 1) & table_mask;
        if (!table[j].used) break;
        uint64_t home = hash_ref(table[j].ref);
        if (((j - home) & table_mask) >= ((j - i) & table_mask)) { table[i] = table[j]; i = j; }
    }
    table[i].used = 0;
    table_count--;
}

/* binary search; ascending=1 sorts by price ascending, 0 by price descending. Returns index of first level not before px. */
static int side_search(const Side *side, uint32_t px, int ascending) {
    int low = 0, high = side->count;
    while (low < high) {
        int mid = (low + high) / 2;
        int before = ascending ? side->levels[mid].px < px : side->levels[mid].px > px;
        if (before) low = mid + 1; else high = mid;
    }
    return low;
}

static void side_add(Side *side, uint32_t px, uint32_t qty, int ascending) {
    int i = side_search(side, px, ascending);
    if (i < side->count && side->levels[i].px == px) { side->levels[i].qty += qty; return; }
    if (side->count == side->capacity) {
        side->capacity = side->capacity ? side->capacity * 2 : 256;
        side->levels = realloc(side->levels, (size_t)side->capacity * sizeof(Level));
        if (!side->levels) { perror("realloc"); exit(2); }
    }
    memmove(&side->levels[i + 1], &side->levels[i], (size_t)(side->count - i) * sizeof(Level));
    side->levels[i] = (Level){px, qty};
    side->count++;
}

static void side_sub(Side *side, uint32_t px, uint32_t qty, int ascending) {
    int i = side_search(side, px, ascending);
    if (i >= side->count || side->levels[i].px != px) { anomalies_missing_level++; return; }
    if (side->levels[i].qty <= qty) {
        memmove(&side->levels[i], &side->levels[i + 1], (size_t)(side->count - i - 1) * sizeof(Level));
        side->count--;
    } else {
        side->levels[i].qty -= qty;
    }
}

static inline Side *book_side(Book *book, uint8_t side) { return side == 'B' ? &book->bid : &book->ask; }
static inline int side_ascending(uint8_t side) { return side == 'B'; }

/* the top `depth` levels of both sides, best first: bid (px, qty) pairs, then ask pairs; zero where a level does not exist */
static void fill_depth(const Book *book, uint32_t *destination) {
    for (int k = 0; k < depth; k++) {
        const Level *bid = k < book->bid.count ? &book->bid.levels[book->bid.count - 1 - k] : NULL;
        const Level *ask = k < book->ask.count ? &book->ask.levels[book->ask.count - 1 - k] : NULL;
        destination[2 * k] = bid ? bid->px : 0;
        destination[2 * k + 1] = bid ? bid->qty : 0;
        destination[2 * depth + 2 * k] = ask ? ask->px : 0;
        destination[2 * depth + 2 * k + 1] = ask ? ask->qty : 0;
    }
}

static void emit_event(uint8_t type, uint16_t sym, uint64_t ref, uint64_t new_ref, uint8_t side, uint32_t px, uint32_t shares) {
    if (!events_mode) return;
    EventRecord record = {cur_itch_ns, cur_seq, ref, new_ref, px, shares, sym, type, side};
    fwrite(&record, sizeof record, 1, out);
    rows_written++;
}

static void emit(uint8_t event, uint16_t sym, uint32_t trade_px, uint32_t trade_sz) {
    if (events_mode) return;
    const Book *book = &books[sym];
    rows_by_event[sym][event == 'Q' ? 0 : event == 'T' ? 1 : 2]++;
    if (depth) {
        uint32_t levels[4 * MAX_DEPTH];
        DepthHeader header = {cur_itch_ns, cur_seq, trade_px, trade_sz, sym, event};
        fill_depth(book, levels);
        fwrite(&header, sizeof header, 1, out);
        fwrite(levels, sizeof levels[0], (size_t)4 * depth, out);
        rows_written++;
        return;
    }
    Record record = {cur_itch_ns, cur_seq, 0, 0, 0, 0, trade_px, trade_sz, sym, event};
    if (book->bid.count) { record.bid_px = book->bid.levels[book->bid.count - 1].px; record.bid_sz = book->bid.levels[book->bid.count - 1].qty; }
    if (book->ask.count) { record.ask_px = book->ask.levels[book->ask.count - 1].px; record.ask_sz = book->ask.levels[book->ask.count - 1].qty; }
    fwrite(&record, sizeof record, 1, out);
    rows_written++;
}

static void check_quote(uint16_t sym) {
    if (events_mode) return;
    Book *book = &books[sym];
    if (depth) {
        uint32_t now[4 * MAX_DEPTH];
        fill_depth(book, now);
        if (book->last_depth && !memcmp(now, book->last_depth, sizeof now[0] * (size_t)4 * depth)) return;
        if (!book->last_depth) book->last_depth = malloc(sizeof now[0] * (size_t)4 * depth);
        if (!book->last_depth) { perror("malloc"); exit(2); }
        memcpy(book->last_depth, now, sizeof now[0] * (size_t)4 * depth);
        emit('Q', sym, 0, 0);
        return;
    }
    uint32_t bid_px = 0, bid_sz = 0, ask_px = 0, ask_sz = 0;
    if (book->bid.count) { bid_px = book->bid.levels[book->bid.count - 1].px; bid_sz = book->bid.levels[book->bid.count - 1].qty; }
    if (book->ask.count) { ask_px = book->ask.levels[book->ask.count - 1].px; ask_sz = book->ask.levels[book->ask.count - 1].qty; }
    if (book->have_last && bid_px == book->last_bid_px && bid_sz == book->last_bid_sz && ask_px == book->last_ask_px && ask_sz == book->last_ask_sz) return;
    book->have_last = 1;
    book->last_bid_px = bid_px; book->last_bid_sz = bid_sz; book->last_ask_px = ask_px; book->last_ask_sz = ask_sz;
    emit('Q', sym, 0, 0);
}

static void reduce_order(Order *order, uint32_t shares) {
    Book *book = &books[order->sym];
    if (shares > order->shares) shares = order->shares;
    side_sub(book_side(book, order->side), order->px, shares, side_ascending(order->side));
    order->shares -= shares;
    uint16_t sym = order->sym;
    if (order->shares == 0) order_erase(order);
    check_quote(sym);
}

static void handle_message(const uint8_t *m, int length) {
    if (length < 11) return;
    cur_itch_ns = be48(m + 5);
    if (list_mode) {
        if (m[0] == 'R' && length >= 19) {
            char name[9]; memcpy(name, m + 11, 8); name[8] = 0;
            for (int i = 7; i >= 0 && name[i] == ' '; i--) name[i] = 0;
            normalise(name);
            puts(name);
        } else if (m[0] == 'A' || m[0] == 'F') {
            stop_requested = 1;  /* the directory is complete once orders start */
        }
        return;
    }
    switch (m[0]) {
    case 'R': {
        if (length < 19) return;
        char name[9]; memcpy(name, m + 11, 8); name[8] = 0;
        for (int i = 7; i >= 0 && name[i] == ' '; i--) name[i] = 0;
        normalise(name);
        for (int s = 0; s < symbol_count; s++)
            if (strcmp(books[s].name, name) == 0) locate_to_symbol[be16(m + 1)] = (int16_t)s;
        break;
    }
    case 'A': case 'F': {
        int sym = locate_to_symbol[be16(m + 1)];
        if (sym < 0 || length < 36) return;
        uint8_t side = m[19];
        uint32_t shares = be32(m + 20), px = be32(m + 32);
        order_insert(be64(m + 11), (uint16_t)sym, side, px, shares);
        emit_event('A', (uint16_t)sym, be64(m + 11), 0, side, px, shares);
        side_add(book_side(&books[sym], side), px, shares, side_ascending(side));
        check_quote((uint16_t)sym);
        break;
    }
    case 'E': {
        if (length < 31) return;
        Order *order = order_find(be64(m + 11));
        if (!order) return;
        uint32_t shares = be32(m + 19);
        emit_event('E', order->sym, order->ref, 0, order->side, order->px, shares);
        emit('T', order->sym, order->px, shares);
        reduce_order(order, shares);
        break;
    }
    case 'C': {
        if (length < 36) return;
        Order *order = order_find(be64(m + 11));
        if (!order) return;
        uint32_t shares = be32(m + 19);
        emit_event('C', order->sym, order->ref, 0, order->side, be32(m + 32), shares);
        if (m[31] == 'Y') emit('T', order->sym, be32(m + 32), shares);
        reduce_order(order, shares);
        break;
    }
    case 'X': {
        if (length < 23) return;
        Order *order = order_find(be64(m + 11));
        if (!order) return;
        emit_event('X', order->sym, order->ref, 0, order->side, order->px, be32(m + 19) > order->shares ? order->shares : be32(m + 19));
        reduce_order(order, be32(m + 19));
        break;
    }
    case 'D': {
        if (length < 19) return;
        Order *order = order_find(be64(m + 11));
        if (!order) return;
        emit_event('D', order->sym, order->ref, 0, order->side, order->px, order->shares);
        reduce_order(order, order->shares);
        break;
    }
    case 'U': {
        if (length < 35) return;
        Order *order = order_find(be64(m + 11));
        if (!order) return;
        uint16_t sym = order->sym; uint8_t side = order->side;
        emit_event('U', sym, order->ref, be64(m + 19), side, be32(m + 31), be32(m + 27));
        side_sub(book_side(&books[sym], side), order->px, order->shares, side_ascending(side));
        order_erase(order);
        uint32_t shares = be32(m + 27), px = be32(m + 31);
        order_insert(be64(m + 19), sym, side, px, shares);
        side_add(book_side(&books[sym], side), px, shares, side_ascending(side));
        check_quote(sym);
        break;
    }
    case 'P': {
        int sym = locate_to_symbol[be16(m + 1)];
        if (sym < 0 || length < 44) return;
        emit_event('P', (uint16_t)sym, 0, 0, m[19], be32(m + 32), be32(m + 20));
        emit('T', (uint16_t)sym, be32(m + 32), be32(m + 20));
        break;
    }
    case 'Q': {
        int sym = locate_to_symbol[be16(m + 1)];
        if (sym < 0 || length < 40) return;
        uint64_t shares = be64(m + 11);
        emit_event('Q', (uint16_t)sym, 0, 0, 0, be32(m + 27), shares > 0xFFFFFFFFULL ? 0xFFFFFFFFU : (uint32_t)shares);
        emit('X', (uint16_t)sym, be32(m + 27), shares > 0xFFFFFFFFULL ? 0xFFFFFFFFU : (uint32_t)shares);
        break;
    }
    default: break;
    }
}

/* ----- Cboe PITCH ------------------------------------------------------------------------------------------------------------ */

/* The symbol field (6 or 8 characters, space padded) as one integer: trailing blanks dropped, ' ' and '-' written as '.', like normalise(). */
static uint64_t symbol_key(const uint8_t *p, int n) {
    while (n > 0 && (p[n - 1] == ' ' || p[n - 1] == 0)) n--;
    uint64_t key = 0;
    for (int i = 0; i < n; i++) key = key << 8 | (p[i] == ' ' || p[i] == '-' ? '.' : p[i]);
    return key;
}

static void key_name(uint64_t key, char *name) {
    int n = 0;
    char reversed[9];
    for (; key; key >>= 8) reversed[n++] = (char)(key & 0xff);
    for (int i = 0; i < n; i++) name[i] = reversed[n - 1 - i];
    name[n] = 0;
}

static inline uint64_t key_slot(uint64_t key, int bits) { return (key * 0x9E3779B97F4A7C15ULL) >> (64 - bits); }

static void index_symbols(void) {
    for (int s = 0; s < symbol_count; s++) {
        uint64_t key = symbol_key((const uint8_t *)books[s].name, (int)strlen(books[s].name));
        uint64_t slot = key_slot(key, 14);
        while (sym_keys[slot] && sym_keys[slot] != key) slot = (slot + 1) & ((1 << 14) - 1);
        sym_keys[slot] = key; sym_vals[slot] = (uint16_t)(s + 1);
    }
}

static int lookup_symbol(const uint8_t *p, int n) {
    uint64_t key = symbol_key(p, n);
    if (!key) return -1;
    uint64_t slot = key_slot(key, 14);
    while (sym_keys[slot]) {
        if (sym_keys[slot] == key) return sym_vals[slot] - 1;
        slot = (slot + 1) & ((1 << 14) - 1);
    }
    return -1;
}

static void note_symbol(const uint8_t *p, int n) {
    uint64_t key = symbol_key(p, n);
    if (!key) return;
    uint64_t slot = key_slot(key, 16);
    while (list_keys[slot] && list_keys[slot] != key) slot = (slot + 1) & ((1 << 16) - 1);
    list_keys[slot] = key;
}

static int compare_names(const void *a, const void *b) { return strcmp((const char *)a, (const char *)b); }

static void print_listed_symbols(void) {
    static char names[1 << 16][9];
    int count = 0;
    for (int slot = 0; slot < (1 << 16); slot++) if (list_keys[slot]) key_name(list_keys[slot], names[count++]);
    qsort(names, (size_t)count, sizeof names[0], compare_names);
    for (int i = 0; i < count; i++) puts(names[i]);
}

static uint32_t price_long(uint64_t px) { return px > 0xFFFFFFFFULL ? 0xFFFFFFFFU : (uint32_t)px; }  /* 1/10000 dollar already */
static uint32_t price_short(uint16_t px) { return (uint32_t)px * 100; }                             /* 1/100 dollar */

/* m points at the length byte: m[0] length, m[1] type, m[2..5] nanoseconds since the last Time message, then the fields of the message. */
static void add_order(const uint8_t *m, int sym, uint8_t side, uint32_t px, uint32_t shares) {
    uint64_t ref = le64(m + 6);
    order_insert(ref, (uint16_t)sym, side, px, shares);
    emit_event('A', (uint16_t)sym, ref, 0, side, px, shares);
    side_add(book_side(&books[sym], side), px, shares, side_ascending(side));
    check_quote((uint16_t)sym);
}

static void pitch_trade(int sym, uint8_t side, uint32_t px, uint32_t shares) {
    emit_event('P', (uint16_t)sym, 0, 0, side, px, shares);
    emit('T', (uint16_t)sym, px, shares);
}

static void handle_pitch_message(const uint8_t *m, int length) {
    uint8_t type = m[1];
    if (type == 0x20) { if (length >= 6) cur_base_ns = (uint64_t)le32(m + 2) * 1000000000ULL; return; }  /* Time */
    if (length < 6) return;
    cur_itch_ns = cur_base_ns + le32(m + 2);
    if (list_mode) {
        if (type == 0x21 && length >= 34) note_symbol(m + 19, 6);
        else if (type == 0x22 && length >= 26) note_symbol(m + 17, 6);
        else if (type == 0x2F && length >= 40) note_symbol(m + 19, 8);
        return;
    }
    switch (type) {
    case 0x21: case 0x22: case 0x2F: {  /* Add Order: long (34), short (26), expanded (40) */
        int symbol_length = type == 0x2F ? 8 : 6, need = type == 0x21 ? 34 : type == 0x22 ? 26 : 40;
        if (length < need) return;
        int sym = lookup_symbol(m + (type == 0x22 ? 17 : 19), symbol_length);
        if (sym < 0) return;
        uint8_t side = m[14];
        if (type == 0x22) add_order(m, sym, side, price_short(le16(m + 23)), le16(m + 15));
        else add_order(m, sym, side, price_long(le64(m + 19 + symbol_length)), le32(m + 15));
        break;
    }
    case 0x23: {  /* Order Executed: at the order's price */
        if (length < 26) return;
        Order *order = order_find(le64(m + 6));
        if (!order) return;
        uint32_t shares = le32(m + 14);
        emit_event('E', order->sym, order->ref, 0, order->side, order->px, shares);
        emit('T', order->sym, order->px, shares);
        reduce_order(order, shares);
        break;
    }
    case 0x24: {  /* Order Executed at Price/Size */
        if (length < 38) return;
        Order *order = order_find(le64(m + 6));
        if (!order) return;
        uint32_t shares = le32(m + 14), px = price_long(le64(m + 30));
        emit_event('C', order->sym, order->ref, 0, order->side, px, shares);
        emit('T', order->sym, px, shares);
        reduce_order(order, shares);
        break;
    }
    case 0x25: case 0x26: {  /* Reduce Size: long (18), short (16) */
        if (length < (type == 0x25 ? 18 : 16)) return;
        Order *order = order_find(le64(m + 6));
        if (!order) return;
        uint32_t shares = type == 0x25 ? le32(m + 14) : le16(m + 14);
        emit_event('X', order->sym, order->ref, 0, order->side, order->px, shares > order->shares ? order->shares : shares);
        reduce_order(order, shares);
        break;
    }
    case 0x27: case 0x28: {  /* Modify Order: long (27), short (19); the order keeps its id and loses its place */
        if (length < (type == 0x27 ? 27 : 19)) return;
        Order *order = order_find(le64(m + 6));
        if (!order) return;
        uint16_t sym = order->sym; uint8_t side = order->side; uint64_t ref = order->ref;
        uint32_t shares = type == 0x27 ? le32(m + 14) : le16(m + 14);
        uint32_t px = type == 0x27 ? price_long(le64(m + 18)) : price_short(le16(m + 16));
        emit_event('U', sym, ref, ref, side, px, shares);
        side_sub(book_side(&books[sym], side), order->px, order->shares, side_ascending(side));
        order_erase(order);
        order_insert(ref, sym, side, px, shares);
        side_add(book_side(&books[sym], side), px, shares, side_ascending(side));
        check_quote(sym);
        break;
    }
    case 0x29: {  /* Delete Order */
        if (length < 14) return;
        Order *order = order_find(le64(m + 6));
        if (!order) return;
        emit_event('D', order->sym, order->ref, 0, order->side, order->px, order->shares);
        reduce_order(order, order->shares);
        break;
    }
    case 0x2A: case 0x2B: case 0x30: {  /* Trade (non-displayed): long (41), short (33), expanded (43) */
        int symbol_length = type == 0x30 ? 8 : 6, need = type == 0x2A ? 41 : type == 0x2B ? 33 : 43;
        if (length < need) return;
        int sym = lookup_symbol(m + (type == 0x2B ? 17 : 19), symbol_length);
        if (sym < 0) return;
        if (type == 0x2B) pitch_trade(sym, m[14], price_short(le16(m + 23)), le16(m + 15));
        else pitch_trade(sym, m[14], price_long(le64(m + 19 + symbol_length)), le32(m + 15));
        break;
    }
    default: break;
    }
}

int main(int argc, char **argv) {
    const char *symbols_arg = NULL, *symbols_file = NULL, *out_path = NULL, *stats_path = NULL;
    int port = 0;
    uint64_t stop_at_ns = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--symbols") && i + 1 < argc) symbols_arg = argv[++i];
        else if (!strcmp(argv[i], "--symbols-file") && i + 1 < argc) symbols_file = argv[++i];
        else if (!strcmp(argv[i], "--stats") && i + 1 < argc) stats_path = argv[++i];
        else if (!strcmp(argv[i], "--stop-at") && i + 1 < argc) stop_at_ns = strtoull(argv[++i], NULL, 10) * 1000000000ULL;
        else if (!strcmp(argv[i], "--list-symbols")) list_mode = 1;
        else if (!strcmp(argv[i], "--out") && i + 1 < argc) out_path = argv[++i];
        else if (!strcmp(argv[i], "--port") && i + 1 < argc) port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--table-bits") && i + 1 < argc) table_bits = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--depth") && i + 1 < argc) depth = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--events")) events_mode = 1;
        else if (!strcmp(argv[i], "--protocol") && i + 1 < argc) {
            const char *name = argv[++i];
            if (!strcmp(name, "pitch")) pitch = 1;
            else if (strcmp(name, "itch")) { fprintf(stderr, "--protocol must be itch or pitch\n"); return 1; }
        }
        else { fprintf(stderr, "bad argument %s\n", argv[i]); return 1; }
    }
    if (depth < 0 || depth > MAX_DEPTH) { fprintf(stderr, "--depth must be between 0 and %d\n", MAX_DEPTH); return 1; }
    if (events_mode && depth) { fprintf(stderr, "--events and --depth are exclusive\n"); return 1; }
    if (!list_mode && ((!symbols_arg && !symbols_file) || !out_path)) { fprintf(stderr, "usage: itch_tob (--symbols A,B | --symbols-file F) --out file|- [--port N]\n"); return 1; }
    if (symbols_arg) {
        char *list = strdup(symbols_arg);
        for (char *token = strtok(list, ","); token && symbol_count < MAX_SYMBOLS; token = strtok(NULL, ",")) {
            snprintf(books[symbol_count].name, sizeof books[0].name, "%s", token);
            normalise(books[symbol_count++].name);
        }
    }
    if (symbols_file) {
        FILE *list = fopen(symbols_file, "r");
        char line[64];
        if (!list) { perror("symbols-file"); return 2; }
        while (fgets(line, sizeof line, list) && symbol_count < MAX_SYMBOLS) {
            line[strcspn(line, "\r\n")] = 0;
            if (!line[0] || strlen(line) > 8) continue;
            snprintf(books[symbol_count].name, sizeof books[0].name, "%s", line);
            normalise(books[symbol_count++].name);
        }
        fclose(list);
    }
    for (int i = 0; i < 65536; i++) locate_to_symbol[i] = -1;
    if (pitch) index_symbols();
    table_capacity = 1ULL << table_bits; table_mask = table_capacity - 1;
    table = calloc(table_capacity, sizeof(Order));
    out = list_mode ? stdout : strcmp(out_path, "-") ? fopen(out_path, "wb") : stdout;
    if (!table || !out) { perror("init"); return 2; }
    if (!list_mode) setvbuf(out, NULL, _IOFBF, 1 << 22);
    setvbuf(stdin, NULL, _IOFBF, 1 << 22);

    uint8_t header[24];
    if (fread(header, 1, 24, stdin) != 24) { fprintf(stderr, "no pcap header\n"); return 2; }
    uint32_t magic; memcpy(&magic, header, 4);
    int swapped = magic == 0xd4c3b2a1 || magic == 0x4d3cb2a1, nano = magic == 0xa1b23c4d || magic == 0x4d3cb2a1;
    if (!swapped && magic != 0xa1b2c3d4 && !nano) { fprintf(stderr, "not a classic pcap (magic %08x)\n", magic); return 2; }

    static uint8_t buffer[1 << 18];
    uint64_t packets = 0, udp_packets = 0, messages = 0, gaps = 0, duplicate_packets = 0, expected = 0;
    int have_expected = 0, stopping = 0;
    uint8_t packet_header[16];
    uint32_t unit_expected[256] = {0};  /* PITCH: next sequence number of each unit */
    uint8_t unit_started[256] = {0};
    while (!stopping && fread(packet_header, 1, 16, stdin) == 16) {
        uint32_t fields[4]; memcpy(fields, packet_header, 16);
        if (swapped) for (int i = 0; i < 4; i++) fields[i] = __builtin_bswap32(fields[i]);
        uint32_t length = fields[2];
        if (length > sizeof buffer) { fprintf(stderr, "packet too large %u\n", length); return 2; }
        if (fread(buffer, 1, length, stdin) != length) break;
        packets++;
        if (length < 42) continue;
        int offset = 12;
        uint16_t ether_type = be16(buffer + offset);
        while ((ether_type == 0x8100 || ether_type == 0x88a8) && offset + 6 < (int)length) { offset += 4; ether_type = be16(buffer + offset); }
        offset += 2;
        if (ether_type != 0x0800) continue;
        const uint8_t *ip = buffer + offset;
        int ip_length = (ip[0] & 0x0f) * 4;
        if (ip[9] != 17 || offset + ip_length + 8 > (int)length) continue;
        const uint8_t *udp = ip + ip_length;
        if (port && be16(udp + 2) != port) continue;
        udp_packets++;
        const uint8_t *payload = udp + 8;
        int payload_length = (int)(length - (size_t)(payload - buffer));
        if (pitch) {  /* Sequenced Unit Header: length u16, count u8, unit u8, sequence u32, little-endian; then count messages, each led by its length byte */
            if (payload_length < 8) continue;
            int unit_length = le16(payload) < payload_length ? le16(payload) : payload_length;
            uint32_t unit_count = payload[2], unit_seq = le32(payload + 4);
            uint8_t unit = payload[3];
            if (unit_count == 0) continue;  /* heartbeat */
            if (!unit_started[unit]) { unit_expected[unit] = unit_seq; unit_started[unit] = 1; }
            if ((int32_t)(unit_seq + unit_count - unit_expected[unit]) <= 0) { duplicate_packets++; continue; }
            uint32_t unit_skip = (int32_t)(unit_expected[unit] - unit_seq) > 0 ? unit_expected[unit] - unit_seq : 0;
            if ((int32_t)(unit_seq - unit_expected[unit]) > 0) gaps += unit_seq - unit_expected[unit];
            unit_expected[unit] = unit_seq + unit_count;
            const uint8_t *q = payload + 8, *unit_end = payload + unit_length;
            for (uint32_t i = 0; i < unit_count && q < unit_end; i++) {
                int message_length = q[0];
                if (message_length < 2 || q + message_length > unit_end) break;
                if (i >= unit_skip) { cur_seq = (uint64_t)unit << 32 | (uint32_t)(unit_seq + i); handle_pitch_message(q, message_length); messages++; }
                if (stop_requested || (stop_at_ns && cur_itch_ns > stop_at_ns)) { stopping = 1; break; }
                q += message_length;
            }
            continue;
        }
        if (payload_length < 20) continue;
        uint64_t seq = be64(payload + 10);
        uint32_t count = be16(payload + 18);
        if (count == 0 || count == 0xFFFF) continue;  /* heartbeat / end of session */
        if (!have_expected) { expected = seq; have_expected = 1; }
        if (seq + count <= expected) { duplicate_packets++; continue; }
        uint64_t skip = expected > seq ? expected - seq : 0;
        if (seq > expected) gaps += seq - expected;
        expected = seq + count;
        const uint8_t *p = payload + 20, *end = buffer + length;
        for (uint32_t i = 0; i < count && p + 2 <= end; i++) {
            int message_length = be16(p);
            p += 2;
            if (p + message_length > end) break;
            if (i >= skip) { cur_seq = seq + i; handle_message(p, message_length); messages++; }
            if (stop_requested || (stop_at_ns && cur_itch_ns > stop_at_ns)) { stopping = 1; break; }
            p += message_length;
        }
        if ((packets & 0xFFFFF) == 0)
            fprintf(stderr, "progress: %lluM messages, %llu rows, itch time %.0fs\n", (unsigned long long)(messages >> 20), (unsigned long long)rows_written, cur_itch_ns / 1e9);
    }
    if (pitch && list_mode) print_listed_symbols();
    fflush(out);
    if (out != stdout) fclose(out);
    if (stats_path) {
        FILE *stats = fopen(stats_path, "w");
        if (stats) {
            fprintf(stats, "symbol,quotes,trades,crosses\n");
            for (int s = 0; s < symbol_count; s++)
                fprintf(stats, "%s,%llu,%llu,%llu\n", books[s].name, (unsigned long long)rows_by_event[s][0], (unsigned long long)rows_by_event[s][1], (unsigned long long)rows_by_event[s][2]);
            fclose(stats);
        }
    }
    fprintf(stderr, "done: packets=%llu udp=%llu messages=%llu duplicates=%llu seq_gap_messages=%llu rows=%llu orders_live=%llu missing_level=%llu\n",
            (unsigned long long)packets, (unsigned long long)udp_packets, (unsigned long long)messages, (unsigned long long)duplicate_packets,
            (unsigned long long)gaps, (unsigned long long)rows_written, (unsigned long long)table_count, (unsigned long long)anomalies_missing_level);
    return 0;
}
