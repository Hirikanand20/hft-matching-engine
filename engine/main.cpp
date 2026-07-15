#define NOMINMAX
#include <iostream>
#include <map>
#include <unordered_map>
#include <string>
#include <vector>
#include <random>
#include <ctime>
#include <cstdint>
#include <stdexcept>
#include <chrono>
#include <cstring>
#include <algorithm>
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <hiredis/hiredis.h>
#include <atomic>
#include <thread>
#include <moodycamel/concurrentqueue.h>
#include <immintrin.h>
#include <sqlite3.h>
#include <memory>   // NEW: for std::unique_ptr / std::make_unique
#include <mutex>    // NEW: for fixes #3 and #4
#include <cerrno>

using json = nlohmann::json;

// ---------------- CONFIG ----------------
constexpr int MAX_ORDERS = 1'000'000;
constexpr int NULL_IDX = -1;

// ---------------- ORDER ----------------
struct alignas(64) Order {
    int id;
    int type; // 0 = BUY, 1 = SELL
    char symbol[16];
    int quantity;
    double price;
    int64_t timestamp;
    int next_idx;
    int prev_idx;
};

// ---------------- REDIS EVENT ----------------
#pragma pack(push, 1)
struct alignas(8) OrderEvent {
    uint64_t seq;
    char symbol[16];
    double price;
    int32_t qty;
    uint64_t order_id;
    int64_t ts_ns;
    char side;
    char event;
    char pad[2];
};
#pragma pack(pop)
static_assert(sizeof(OrderEvent) == 56, "OrderEvent must be 56 bytes");

struct alignas(16) TradeRecord {
    uint64_t ts_ns;
    char symbol[16];
    double price;
    int32_t qty;
    uint64_t buy_id;
    uint64_t sell_id;
};

static moodycamel::ConcurrentQueue<TradeRecord> g_trade_queue{ 1'000'000 };
static std::atomic<bool> g_db_running{ true };
static sqlite3* g_db = nullptr;

// ---------------- SQLITE DB THREAD ----------------
void db_writer_thread() {
    if (sqlite3_open_v2("trades.db", &g_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, nullptr) != SQLITE_OK) {
        std::cerr << "SQLite open failed: " << sqlite3_errmsg(g_db) << "\n";
        return;
    }
    sqlite3_exec(g_db, "PRAGMA journal_mode=WAL;", 0, 0, 0);
    sqlite3_exec(g_db, "PRAGMA synchronous=NORMAL;", 0, 0, 0);
    sqlite3_exec(g_db, "PRAGMA cache_size=-64000;", 0, 0, 0);
    sqlite3_exec(g_db, "PRAGMA temp_store=MEMORY;", 0, 0, 0);
    sqlite3_busy_timeout(g_db, 1000);
    sqlite3_exec(g_db, "CREATE TABLE IF NOT EXISTS trades (ts_ns INTEGER, symbol TEXT, price REAL, qty INTEGER, buy_id INTEGER, sell_id INTEGER);", 0, 0, 0);

    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(g_db, "INSERT INTO trades VALUES (?,?,?,?,?,?)", -1, &stmt, 0) != SQLITE_OK) {
        std::cerr << "SQLite prepare failed: " << sqlite3_errmsg(g_db) << "\n";
        sqlite3_close(g_db);
        return;
    }

    std::vector<TradeRecord> batch;
    batch.reserve(8192);

    while (g_db_running.load() || g_trade_queue.size_approx() > 0) {
        TradeRecord tr;
        while (batch.size() < 8192 && g_trade_queue.try_dequeue(tr)) {
            batch.push_back(tr);
        }
        if (!batch.empty()) {
            sqlite3_exec(g_db, "BEGIN TRANSACTION", 0, 0, 0);
            for (const auto& t : batch) {
                sqlite3_bind_int64(stmt, 1, t.ts_ns);
                sqlite3_bind_text(stmt, 2, t.symbol, -1, SQLITE_STATIC);
                sqlite3_bind_double(stmt, 3, t.price);
                sqlite3_bind_int(stmt, 4, t.qty);
                sqlite3_bind_int64(stmt, 5, t.buy_id);
                sqlite3_bind_int64(stmt, 6, t.sell_id);
                if (sqlite3_step(stmt) != SQLITE_DONE) {
                    std::cerr << "SQLite step failed: " << sqlite3_errmsg(g_db) << "\n";
                }
                sqlite3_reset(stmt);
            }
            sqlite3_exec(g_db, "COMMIT", 0, 0, 0);
            batch.clear();
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    sqlite3_finalize(stmt);
    sqlite3_close(g_db);
}

// ---------------- REDIS GLOBALS ----------------
static redisContext* g_redis_ctx = nullptr;
static std::atomic<uint64_t> g_seq{ 0 };
static std::atomic<bool> g_redis_running{ true };
static std::mutex g_redis_mutex;   // FIX: was referenced but never declared

bool redis_init(const char* host = "127.0.0.1", int port = 6379) {
    g_redis_ctx = redisConnect(host, port);
    if (g_redis_ctx == nullptr || g_redis_ctx->err) {
        if (g_redis_ctx) {
            std::cerr << "Redis error: " << g_redis_ctx->errstr << "\n";
            redisFree(g_redis_ctx);
            g_redis_ctx = nullptr;
        }
        return false;
    }
    g_redis_running.store(true);
    return true;
}

void redis_cleanup() {
    g_redis_running.store(false);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::lock_guard<std::mutex> lk(g_redis_mutex);   // FIX: missing semicolon before
    if (g_redis_ctx) {
        redisFree(g_redis_ctx);
        g_redis_ctx = nullptr;
    }
}

void redis_publish(const char* channel, const void* data, size_t len) {
    if (!g_redis_ctx || !g_redis_running.load(std::memory_order_acquire)) return;
    std::lock_guard<std::mutex> lk(g_redis_mutex);
    if (!g_redis_ctx) return;

    const char* argv[3];
    size_t argvlen[3];
    argv[0] = "PUBLISH";        // CHANGED: was "LPUSH"
    argvlen[0] = 7;              // CHANGED: was 5
    argv[1] = channel;
    argvlen[1] = std::strlen(channel);
    argv[2] = (const char*)data;
    argvlen[2] = len;

    void* reply = redisCommandArgv(g_redis_ctx, 3, argv, argvlen);
    if (reply) freeReplyObject(reply);
}

void publish_event(const Order& ord, char event_type, int32_t event_qty = -1) {
    OrderEvent ev{};
    ev.seq = g_seq.fetch_add(1, std::memory_order_relaxed);
    ev.price = ord.price;
    ev.qty = (event_qty == -1) ? ord.quantity : event_qty;
    ev.order_id = ord.id;
    ev.ts_ns = ord.timestamp;
    ev.side = (ord.type == 0 ? 'B' : 'S');
    ev.event = event_type;
    std::memcpy(ev.symbol, ord.symbol, sizeof(ev.symbol));
    ev.symbol[sizeof(ev.symbol) - 1] = '\0';

    char channel[48];
    std::snprintf(channel, sizeof(channel), "book:%s", ev.symbol);   // CHANGED: was "queue:book:%s"
    redis_publish(channel, &ev, sizeof(ev));
}

// ---------------- ORDER POOL ----------------
struct OrderPool {
    Order data[MAX_ORDERS];
    int free_head = 0;
    int used_count = 0;
    OrderPool() {
        for (int i = 0; i < MAX_ORDERS - 1; ++i) data[i].next_idx = i + 1;
        data[MAX_ORDERS - 1].next_idx = NULL_IDX;
    }
    int allocate() {
        if (free_head == NULL_IDX) throw std::runtime_error("OrderPool exhausted");
        int idx = free_head;
        free_head = data[free_head].next_idx;
        used_count++;
        data[idx].next_idx = NULL_IDX;
        data[idx].prev_idx = NULL_IDX;
        return idx;
    }
    void deallocate(int idx) {
        data[idx].next_idx = free_head;
        free_head = idx;
        used_count--;
    }
    Order& get(int idx) { return data[idx]; }
    const Order& get(int idx) const { return data[idx]; }
};

// ---------------- PRICE LEVEL ----------------
struct PriceLevel {
    int head_idx = NULL_IDX;
    int tail_idx = NULL_IDX;
    int count = 0;
    void push_back(OrderPool& pool, int order_idx) {
        auto& order = pool.get(order_idx);
        order.prev_idx = tail_idx;
        order.next_idx = NULL_IDX;
        if (tail_idx != NULL_IDX) pool.get(tail_idx).next_idx = order_idx;
        else head_idx = order_idx;
        tail_idx = order_idx;
        ++count;
    }
    void remove(OrderPool& pool, int order_idx) {
        auto& o = pool.get(order_idx);
        int prev = o.prev_idx, next = o.next_idx;
        if (prev != NULL_IDX) pool.get(prev).next_idx = next; else head_idx = next;
        if (next != NULL_IDX) pool.get(next).prev_idx = prev; else tail_idx = prev;
        pool.deallocate(order_idx);
        --count;
    }
    void pop_front(OrderPool& pool) {
        if (head_idx == NULL_IDX) return;
        auto& order = pool.get(head_idx);
        int next = order.next_idx;
        if (next != NULL_IDX) pool.get(next).prev_idx = NULL_IDX;
        else tail_idx = NULL_IDX;
        pool.deallocate(head_idx);
        head_idx = next;
        --count;
    }
    bool empty() const { return count == 0; }
};

// ---------------- ORDER BOOK ----------------
struct OrderBook {
    std::map<double, PriceLevel, std::greater<double>> buy_book;
    std::map<double, PriceLevel> sell_book;
};

// ---------------- CMD ----------------
enum class CmdType { ADD, CANCEL };
struct OrderCmd {
    CmdType type;
    int order_type;
    int quantity;
    double price;
    int64_t ts_ns;
    int order_id = -1;
};

// ---------------- SYMBOL ENGINE ----------------
struct SymbolEngine {
    std::string symbol;
    OrderBook book;
    OrderPool pool;
    moodycamel::ConcurrentQueue<OrderCmd> cmd_queue;
    std::thread worker;
    std::atomic<bool> running{ true };
    int next_order_id = 1;
    std::unordered_map<int, int> id_to_idx;
    std::mutex book_mutex;   // guards book + pool against snapshot-thread reads

    explicit SymbolEngine(std::string sym) : symbol(std::move(sym)) {}
    void start() { worker = std::thread(&SymbolEngine::run, this); }
    void stop() {
        running.store(false, std::memory_order_release);
        if (worker.joinable()) worker.join();
    }
    void run() {
        OrderCmd cmd;
        while (running.load(std::memory_order_acquire)) {
            while (cmd_queue.try_dequeue(cmd)) process_cmd(cmd);
            match();
            _mm_pause();
        }
    }

    void process_cmd(const OrderCmd& cmd) {
        if (cmd.type == CmdType::ADD) {
            int idx = pool.allocate();
            auto& o = pool.get(idx);
            o.id = next_order_id++;
            o.type = cmd.order_type;
            std::snprintf(o.symbol, sizeof(o.symbol), "%s", symbol.c_str());
            o.quantity = cmd.quantity;
            o.price = cmd.price;
            o.timestamp = cmd.ts_ns;
            o.next_idx = o.prev_idx = NULL_IDX;
            id_to_idx[o.id] = idx;              // FIX: was missing -> CANCEL always failed
            add_order(idx);
        } else if (cmd.type == CmdType::CANCEL) {
            auto it = id_to_idx.find(cmd.order_id);
            if (it == id_to_idx.end()) return;
            int idx = it->second;
            Order& o = pool.get(idx);
            std::lock_guard<std::mutex> lk(book_mutex);
            auto& level_map_entry = (o.type == 0)
                ? book.buy_book[o.price]
                : book.sell_book[o.price];
            publish_event(o, 'X');
            level_map_entry.remove(pool, idx);
            id_to_idx.erase(it);
            if (level_map_entry.empty()) {
                if (o.type == 0) book.buy_book.erase(o.price);
                else book.sell_book.erase(o.price);
            }
        }
    }   // FIX: closing brace for process_cmd, was missing -> brace mismatch

    void add_order(int order_idx) {
        auto& ord = pool.get(order_idx);
        std::lock_guard<std::mutex> lk(book_mutex);   // FIX: lock was dropped in last version
        if (ord.type == 0) book.buy_book[ord.price].push_back(pool, order_idx);
        else book.sell_book[ord.price].push_back(pool, order_idx);
        publish_event(ord, 'A');
    }

    void match() {
        std::lock_guard<std::mutex> lk(book_mutex);   // FIX: lock was dropped in last version
        while (!book.buy_book.empty() && !book.sell_book.empty()) {
            auto buy_it = book.buy_book.begin();
            auto sell_it = book.sell_book.begin();
            if (buy_it->first < sell_it->first) break;
            PriceLevel& buy_level = buy_it->second;
            PriceLevel& sell_level = sell_it->second;
            if (buy_level.empty() || sell_level.empty()) {
                if (buy_level.empty()) book.buy_book.erase(buy_it);
                if (sell_level.empty()) book.sell_book.erase(sell_it);
                continue;
            }
            int buy_idx = buy_level.head_idx;
            int sell_idx = sell_level.head_idx;
            Order& buy = pool.get(buy_idx);
            Order& sell = pool.get(sell_idx);
            int qty = (std::min)(buy.quantity, sell.quantity);
            double trade_px = (buy.timestamp <= sell.timestamp) ? buy.price : sell.price;

            TradeRecord tr{};
            tr.ts_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            std::memcpy(tr.symbol, buy.symbol, sizeof(tr.symbol));
            tr.price = trade_px;
            tr.qty = qty;
            tr.buy_id = buy.id;
            tr.sell_id = sell.id;
            g_trade_queue.try_enqueue(tr);

            publish_event(buy, 'E', qty);
            publish_event(sell, 'E', qty);
            buy.quantity -= qty;
            sell.quantity -= qty;

            if (buy.quantity == 0) {
                publish_event(buy, 'C');
                id_to_idx.erase(buy.id);
                buy_level.pop_front(pool);
            }
            if (sell.quantity == 0) {
                publish_event(sell, 'C');
                id_to_idx.erase(sell.id);
                sell_level.pop_front(pool);
            }
            if (buy_level.empty()) book.buy_book.erase(buy_it->first);
            if (sell_level.empty()) book.sell_book.erase(sell_it->first);
        }
    }
};

#pragma pack(push, 1)
struct SnapshotHeader { uint32_t bid_levels; uint32_t ask_levels; char symbol[16]; };
struct SnapshotLevel { double price; int32_t qty; int32_t count; };
#pragma pack(pop)

void send_snapshot(const std::string& sym, SymbolEngine* engine) {
    if (!g_redis_ctx || !g_redis_running.load(std::memory_order_acquire)) return;

    std::vector<SnapshotLevel> bids, asks;
    {
        std::lock_guard<std::mutex> lk(engine->book_mutex);
        auto& book = engine->book;
        int n = 0;
        for (auto& [px, lvl] : book.buy_book) {
            if (n++ >= 20) break;
            int total = 0; int idx = lvl.head_idx;
            while (idx != NULL_IDX) { total += engine->pool.get(idx).quantity; idx = engine->pool.get(idx).next_idx; }
            bids.push_back({ px, total, lvl.count });
        }
        n = 0;
        for (auto& [px, lvl] : book.sell_book) {
            if (n++ >= 20) break;
            int total = 0; int idx = lvl.head_idx;
            while (idx != NULL_IDX) { total += engine->pool.get(idx).quantity; idx = engine->pool.get(idx).next_idx; }
            asks.push_back({ px, total, lvl.count });
        }
    }

    SnapshotHeader hdr{};
    hdr.bid_levels = bids.size();
    hdr.ask_levels = asks.size();
    std::snprintf(hdr.symbol, sizeof(hdr.symbol), "%s", sym.c_str());

    size_t len = sizeof(hdr) + (bids.size() + asks.size()) * sizeof(SnapshotLevel);
    std::vector<char> buf(len);
    char* p = buf.data();
    std::memcpy(p, &hdr, sizeof(hdr)); p += sizeof(hdr);
    std::memcpy(p, bids.data(), bids.size() * sizeof(SnapshotLevel)); p += bids.size() * sizeof(SnapshotLevel);
    std::memcpy(p, asks.data(), asks.size() * sizeof(SnapshotLevel));

    char channel[32];
    std::snprintf(channel, sizeof(channel), "snapshot:%s", sym.c_str());
    redis_publish(channel, buf.data(), buf.size());
}

std::unordered_map<std::string, std::unique_ptr<SymbolEngine>> engines;

size_t write_callback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

// ---------------- CURL SESSION HELPER ----------------
struct CurlSession {
    CURL* curl;
    struct curl_slist* headers = nullptr;

    CurlSession() {
        curl = curl_easy_init();
        if (!curl) throw std::runtime_error("CURL init failed");
        headers = curl_slist_append(headers,
            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
        headers = curl_slist_append(headers, "Accept: application/json,text/html,*/*");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_COOKIEFILE, "");
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
        curl_easy_setopt(curl, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1_2);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    }
    ~CurlSession() {
        if (headers) curl_slist_free_all(headers);
        if (curl) curl_easy_cleanup(curl);
    }
    std::string get(const std::string& url, long* http_code_out = nullptr) {
        std::string response;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        CURLcode res = curl_easy_perform(curl);
        if (res != CURLE_OK)
            throw std::runtime_error(std::string("CURL error: ") + curl_easy_strerror(res));
        if (http_code_out) curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, http_code_out);
        return response;
    }
};   // <-- single closing brace, one definition, don
// ---------------- ATTEMPT A: batched v7/quote with cookie+crumb ----------------
std::unordered_map<std::string, double> try_fetch_v7_batched(const std::string& symbols_csv) {
    CurlSession session; // one handle -> cookies persist across these 3 requests

    long code = 0;
    session.get("https://fc.yahoo.com", &code); // seeds session cookies; ignore body

    std::string crumb = session.get("https://query2.finance.yahoo.com/v1/test/getcrumb", &code);
// trim whitespace/newlines
while (!crumb.empty() && (crumb.back() == '\n' || crumb.back() == '\r' || crumb.back() == ' '))
    crumb.pop_back();

if (crumb.empty() || crumb.find("Too Many Requests") != std::string::npos)
    throw std::runtime_error("could not obtain crumb (HTTP " + std::to_string(code) + ")");

char* escaped = curl_easy_escape(session.curl, crumb.c_str(), (int)crumb.length());
std::string crumb_encoded = escaped ? escaped : crumb;
if (escaped) curl_free(escaped);

std::string url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=" + symbols_csv + "&crumb=" + crumb_encoded;
    
    std::string body = session.get(url, &code);
    if (code != 200) throw std::runtime_error("v7/quote returned HTTP " + std::to_string(code));

    json j = json::parse(body); // let it throw on malformed JSON — no silent catch(...)
    auto results = j.at("quoteResponse").at("result");

    std::unordered_map<std::string, double> prices;
    for (auto& stock : results)
        prices[std::string(stock.at("symbol"))] = stock.at("regularMarketPrice").get<double>();

    if (prices.empty()) throw std::runtime_error("v7/quote returned zero results");
    return prices;
}

// ---------------- ATTEMPT B: unauthenticated v8/chart, one call per symbol ----------------
std::unordered_map<std::string, double> fetch_v8_chart_fallback(const std::vector<std::string>& symbols) {
    std::unordered_map<std::string, double> prices;
    for (const auto& sym : symbols) {
        try {
            CurlSession session;
            long code = 0;
            std::string body = session.get("https://query1.finance.yahoo.com/v8/finance/chart/" + sym, &code);
            if (code != 200) { std::cerr << "[v8/chart] " << sym << ": HTTP " << code << "\n"; continue; }
            json j = json::parse(body);
            prices[sym] = j.at("chart").at("result")[0].at("meta").at("regularMarketPrice").get<double>();
        } catch (const std::exception& e) {
            std::cerr << "[v8/chart] " << sym << " failed: " << e.what() << "\n";
        }
    }
    return prices;
}

// ---------------- COMBINED: try fast path, fall back automatically ----------------
std::unordered_map<std::string, double> fetch_prices_hybrid(
    const std::string& symbols_csv, const std::vector<std::string>& symbols) {
    try {
        auto prices = try_fetch_v7_batched(symbols_csv);
        std::cout << "[prices] fetched via v7/quote (batched, authenticated)\n";
        return prices;
    } catch (const std::exception& e) {
        std::cerr << "[prices] v7/quote failed (" << e.what() << ") — falling back to v8/chart\n";
    }
    auto prices = fetch_v8_chart_fallback(symbols);
    if (!prices.empty()) std::cout << "[prices] fetched via v8/chart (per-symbol fallback)\n";
    return prices;
}


int main() {
    curl_global_init(CURL_GLOBAL_DEFAULT);   // FIX: required before any curl_easy_init()
    try {
        if (!redis_init()) std::cerr << "Redis not connected, continuing without publish\n";
        std::thread db_thread(db_writer_thread);

        std::vector<std::string> symbol_list = { "RELIANCE.NS", "TCS.NS", "VBL.NS" };
        std::string symbols_str = "RELIANCE.NS,TCS.NS,VBL.NS";

        std::cout << "Fetching prices...\n";
        std::unordered_map<std::string, double> base_price = fetch_prices_hybrid(symbols_str, symbol_list);

        if (base_price.empty()) {
           std::cout << "Using default static engine prices.\n";
           base_price = { {"RELIANCE.NS", 2520.5}, {"TCS.NS", 3800.0}, {"VBL.NS", 1450.5} };
          }

        std::vector<std::pair<double, SymbolEngine*>> active_engines;
        for (auto& [sym, price] : base_price) {
            engines[sym] = std::make_unique<SymbolEngine>(sym);
            engines[sym]->start();
            active_engines.emplace_back(price, engines[sym].get());
            std::cout << "Started engine for " << sym << " at price " << price << "\n";
        }

        std::atomic<bool> gen_running{ true };
        std::thread gen_thread([active_engines, &gen_running] {
            thread_local std::mt19937_64 eng(std::chrono::steady_clock::now().time_since_epoch().count());
            std::uniform_int_distribution<int> type_dist(0, 1);
            std::uniform_int_distribution<int> qty_dist(1, 100);
            while (gen_running.load(std::memory_order_acquire)) {
                for (auto& [price, engine_ptr] : active_engines) {
                    std::normal_distribution<double> price_dist(price, price * 0.01);
                    OrderCmd cmd{ CmdType::ADD, type_dist(eng), qty_dist(eng), price_dist(eng),
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count() };
                    engine_ptr->cmd_queue.enqueue(cmd);
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });

        std::thread cmd_thread([&gen_running, &base_price] {
            redisContext* cmd_ctx = redisConnect("127.0.0.1", 6379);
            if (!cmd_ctx || cmd_ctx->err) { if (cmd_ctx) redisFree(cmd_ctx); return; }

            std::string blpop_cmd = "BLPOP";
            for (auto& [sym, price] : base_price) blpop_cmd += " cmd:" + sym;
            blpop_cmd += " 1";
            while (gen_running.load(std::memory_order_acquire)) {
                redisReply* reply = (redisReply*)redisCommand(cmd_ctx, blpop_cmd.c_str());
                if (reply && reply->type == REDIS_REPLY_ARRAY && reply->elements == 2) {
                    std::string key = reply->element[0]->str;
                    std::string sym = key.substr(4);
                    try {
                        json j = json::parse(reply->element[1]->str);
                        OrderCmd cmd{
                            j["type"] == "ADD" ? CmdType::ADD : CmdType::CANCEL,
                            j.value("order_type", 0),
                            j.value("quantity", 0),
                            j.value("price", 0.0),
                            j.value("ts_ns", (int64_t)0),
                            j.value("order_id", -1)
                        };
                        if (engines.count(sym)) engines[sym]->cmd_queue.enqueue(cmd);
                    } catch (...) {}
                }
                if (reply) freeReplyObject(reply);
            }
            redisFree(cmd_ctx);
        });

        std::thread snap_thread([&gen_running] {
            redisContext* snap_ctx = redisConnect("127.0.0.1", 6379);
            if (!snap_ctx || snap_ctx->err) { if (snap_ctx) redisFree(snap_ctx); return; }

            struct timeval tv{ 1, 0 };
            redisSetTimeout(snap_ctx, tv);

            redisReply* sub_reply = (redisReply*)redisCommand(snap_ctx, "PSUBSCRIBE snapshot_req:*");
            if (sub_reply) freeReplyObject(sub_reply);

            while (gen_running.load(std::memory_order_acquire)) {
                redisReply* reply = nullptr;
                int rc = redisGetReply(snap_ctx, (void**)&reply);
                if (rc != REDIS_OK) {
                    if (reply) freeReplyObject(reply);
                    if (snap_ctx->err == REDIS_ERR_IO && errno == EAGAIN) continue;
                    break;
                }
                if (reply && reply->type == REDIS_REPLY_ARRAY && reply->elements == 4) {
                    std::string sym = reply->element[2]->str;
                    sym = sym.substr(13);
                    if (engines.count(sym)) send_snapshot(sym, engines[sym].get());
                }
                if (reply) freeReplyObject(reply);
            }
            redisFree(snap_ctx);
        });

        std::cout << "All threads started. Press Enter to stop...\n";
        std::cin.get();

        std::cout << "Shutting down engines safely...\n";
        gen_running.store(false, std::memory_order_release);

        for (auto& [sym, engine] : engines) {
            engine->stop();
        }

        g_db_running.store(false, std::memory_order_release);

        if (gen_thread.joinable()) gen_thread.join();
        if (cmd_thread.joinable()) cmd_thread.join();
        if (snap_thread.joinable()) snap_thread.join();
        if (db_thread.joinable()) db_thread.join();

        redis_cleanup();

        std::cout << "Engine stopped cleanly without memory leaks.\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
    }
    curl_global_cleanup();   // FIX: pairs with curl_global_init above
    return 0;
}

