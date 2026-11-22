/* itch_tob: read a classic pcap on stdin carrying MoldUDP64 / Nasdaq TotalView-ITCH 5.0,
 * rebuild the order book for the chosen symbols and write one record per top-of-book change or trade.
 * Usage: xz -dc day.pcap.xz | itch_tob (--symbols SPY,QQQ | --symbols-file list.txt) --out day.bin|- [--port N] [--table-bits 22]
 *                                      [--stop-at SECONDS_SINCE_MIDNIGHT] [--stats per_symbol.csv]
 *        xz -dc day.pcap.xz | itch_tob --list-symbols      prints the symbols of the day's stock directory, one per line, then stops
 * Duplicate packets (A/B feeds, retransmits) are dropped by MoldUDP64 sequence number.
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
#pragma pack(pop)

typedef struct { uint32_t px; uint32_t qty; } Level;
typedef struct { Level *levels; int count, capacity; } Side;
typedef struct {
    char name[9];
    Side bid, ask;  /* bid ascending by price, ask descending, so the best level is always the last one */
    uint32_t last_bid_px, last_bid_sz, last_ask_px, last_ask_sz;
    int have_last;
} Book;
typedef struct { uint64_t ref; uint32_t px, shares; uint16_t sym; uint8_t side, used; } Order;

#define MAX_SYMBOLS 4096
static Book books[MAX_SYMBOLS];
static int symbol_count = 0;
static int16_t locate_to_symbol[65536];
static Order *table;
static uint64_t table_mask, table_count, table_capacity;
static int table_bits = 22;
static FILE *out;
static int list_mode, stop_requested;
static uint64_t cur_itch_ns, cur_seq, rows_written;
static uint64_t rows_by_event[MAX_SYMBOLS][3];  /* Q, T, X */
static uint64_t anomalies_missing_level;

static void normalise(char *name) {  /* ITCH writes class shares as 'BRK B', Polygon as 'BRK.B' or 'BRK-B' */
    for (; *name; name++) if (*name == ' ' || *name == '-') *name = '.';
}

static inline uint16_t be16(const uint8_t *p) { return (uint16_t)(p[0] << 8 | p[1]); }
static inline uint32_t be32(const uint8_t *p) { return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 | (uint32_t)p[2] << 8 | p[3]; }
static inline uint64_t be48(const uint8_t *p) { return (uint64_t)be16(p) << 32 | be32(p + 2); }
static inline uint64_t be64(const uint8_t *p) { return (uint64_t)be32(p) << 32 | be32(p + 4); }

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

static void emit(uint8_t event, uint16_t sym, uint32_t trade_px, uint32_t trade_sz) {
    const Book *book = &books[sym];
    Record record = {cur_itch_ns, cur_seq, 0, 0, 0, 0, trade_px, trade_sz, sym, event};
    rows_by_event[sym][event == 'Q' ? 0 : event == 'T' ? 1 : 2]++;
    if (book->bid.count) { record.bid_px = book->bid.levels[book->bid.count - 1].px; record.bid_sz = book->bid.levels[book->bid.count - 1].qty; }
    if (book->ask.count) { record.ask_px = book->ask.levels[book->ask.count - 1].px; record.ask_sz = book->ask.levels[book->ask.count - 1].qty; }
    fwrite(&record, sizeof record, 1, out);
    rows_written++;
}

static void check_quote(uint16_t sym) {
    Book *book = &books[sym];
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
        side_add(book_side(&books[sym], side), px, shares, side_ascending(side));
        check_quote((uint16_t)sym);
        break;
    }
    case 'E': {
        if (length < 31) return;
        Order *order = order_find(be64(m + 11));
        if (!order) return;
        uint32_t shares = be32(m + 19);
        emit('T', order->sym, order->px, shares);
        reduce_order(order, shares);
        break;
    }
    case 'C': {
        if (length < 36) return;
        Order *order = order_find(be64(m + 11));
        if (!order) return;
        uint32_t shares = be32(m + 19);
        if (m[31] == 'Y') emit('T', order->sym, be32(m + 32), shares);
        reduce_order(order, shares);
        break;
    }
    case 'X': {
        if (length < 23) return;
        Order *order = order_find(be64(m + 11));
        if (order) reduce_order(order, be32(m + 19));
        break;
    }
    case 'D': {
        if (length < 19) return;
        Order *order = order_find(be64(m + 11));
        if (order) reduce_order(order, order->shares);
        break;
    }
    case 'U': {
        if (length < 35) return;
        Order *order = order_find(be64(m + 11));
        if (!order) return;
        uint16_t sym = order->sym; uint8_t side = order->side;
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
        emit('T', (uint16_t)sym, be32(m + 32), be32(m + 20));
        break;
    }
    case 'Q': {
        int sym = locate_to_symbol[be16(m + 1)];
        if (sym < 0 || length < 40) return;
        uint64_t shares = be64(m + 11);
        emit('X', (uint16_t)sym, be32(m + 27), shares > 0xFFFFFFFFULL ? 0xFFFFFFFFU : (uint32_t)shares);
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
        else { fprintf(stderr, "bad argument %s\n", argv[i]); return 1; }
    }
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
