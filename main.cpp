/*
 * Mini HFT Demo Engine — GOOGL on Alpaca Paper Trading
 * =====================================================
 * Threads:
 *   1. Feed thread     — connects to Alpaca WebSocket, parses GOOGL ticks
 *   2. Strategy thread — simple market-making: quote bid/ask around mid
 *   3. Risk thread     — monitors position & P&L, flips kill switch
 *   4. Order thread    — simulates order fills, tracks latency
 *
 * Build:
 *   sudo apt install libwebsocketpp-dev libboost-all-dev libssl-dev nlohmann-json3-dev
 *   g++ -std=c++17 -O2 -pthread main.cpp -lssl -lcrypto -lboost_system -o hft_demo
 *
 * Run:
 *   export ALPACA_KEY="YOUR_NEW_KEY_HERE"
 *   export ALPACA_SECRET="YOUR_NEW_SECRET_HERE"
 *   ./hft_demo
 */

#include <iostream>
#include <thread>
#include <atomic>
#include <mutex>
#include <queue>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <vector>
#include <string>
#ifdef _WIN32
#include <windows.h>
#endif
using namespace std;

// ─── WebSocket + JSON ────────────────────────────────────────────────────────
// We use websocketpp (header-only) + nlohmann/json (header-only)
// If not installed, the feed thread will use a simple HTTP fallback
#ifdef __has_include
  #if __has_include(<websocketpp/config/asio_client.hpp>)
    #include <websocketpp/config/asio_client.hpp>
    #include <websocketpp/client.hpp>
    #define HAS_WEBSOCKETPP
  #endif
  #if __has_include(<nlohmann/json.hpp>)
    #include <nlohmann/json.hpp>
    #define HAS_JSON
    using json = nlohmann::json;
  #endif
#endif

// ─── Data Structures ─────────────────────────────────────────────────────────

struct Tick {
    double bid;
    double ask;
    double last;
    long long timestamp_ns;  // nanoseconds since epoch
};

struct Order {
    enum Side { BUY, SELL };
    Side     side;
    double   price;
    int      qty;
    long long sent_ns;       // when we "sent" the order
};

struct Fill {
    Order    order;
    long long filled_ns;     // when it was "filled"
    long long latency_ns;    // filled_ns - sent_ns
};

// ─── Shared State ─────────────────────────────────────────────────────────────

// Latest market data (written by feed thread, read by strategy thread)
struct alignas(64) MarketData {
    std::atomic<double> bid{0.0};
    std::atomic<double> ask{0.0};
    std::atomic<double> last{0.0};
    std::atomic<long long> timestamp{0};
};
MarketData market;

// Order queue: strategy → order thread (protected by mutex for simplicity)
std::queue<Order> order_queue;
std::mutex        order_mutex;

// Fill log: order thread writes, main reads for stats
std::vector<Fill> fill_log;
std::mutex        fill_mutex;

// Kill switch: risk thread can flip this to stop all trading
std::atomic<bool> kill_switch{false};

// Position tracking
std::atomic<int>    net_position{0};    // positive = long, negative = short
std::atomic<double> realized_pnl{0.0};
std::atomic<long long> orders_sent{0};
std::atomic<long long> orders_filled{0};

// ─── Helpers ──────────────────────────────────────────────────────────────────

long long now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()
    ).count();
}

void print_dashboard() {
    double bid  = market.bid.load();
    double ask  = market.ask.load();
    double mid  = (bid + ask) / 2.0;
    int    pos  = net_position.load();
    double pnl  = realized_pnl.load();
    long long sent   = orders_sent.load();
    long long filled = orders_filled.load();

    // Compute avg latency from fill log
    double avg_lat_us = 0.0;
    double min_lat_us = 1e9, max_lat_us = 0.0;
    {
        std::lock_guard<std::mutex> lock(fill_mutex);
        if (!fill_log.empty()) {
            double sum = 0;
            for (auto& f : fill_log) {
                double lat = f.latency_ns / 1000.0;
                sum += lat;
                if (lat < min_lat_us) min_lat_us = lat;
                if (lat > max_lat_us) max_lat_us = lat;
            }
            avg_lat_us = sum / fill_log.size();
        }
    }

    // Clear terminal line and print
    std::cout << "\r\033[K";  // clear line
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "[GOOGL] "
              << "Bid:" << bid << " Ask:" << ask << " Mid:" << mid
              << "  Pos:" << std::showpos << pos << std::noshowpos
              << "  P&L:$" << std::showpos << pnl << std::noshowpos
              << "  Orders:" << sent << "/" << filled
              << "  Lat(us): avg=" << avg_lat_us
              << " min=" << min_lat_us
              << " max=" << max_lat_us
              << (kill_switch ? "  [KILLED]" : "")
              << std::flush;
}

// ─── Thread 1: Feed Simulator ─────────────────────────────────────────────────
// Connects to Alpaca WebSocket for live GOOGL quotes.
// Falls back to a simple sine-wave price simulator if libs not available.

void feed_thread_func() {
    const char* key    = std::getenv("ALPACA_KEY");
    const char* secret = std::getenv("ALPACA_SECRET");

    bool use_live = (key != nullptr && secret != nullptr);

#ifdef HAS_WEBSOCKETPP
    if (use_live) {
        // ── Live Alpaca feed ──────────────────────────────────────────────
        using WsClient = websocketpp::client<websocketpp::config::asio_tls_client>;
        WsClient ws;
        ws.init_asio();
        ws.set_tls_init_handler([](websocketpp::connection_hdl) {
            return std::make_shared<boost::asio::ssl::context>(
                boost::asio::ssl::context::tlsv12_client);
        });

        ws.set_open_handler([&](websocketpp::connection_hdl hdl) {
            // Authenticate
            std::string auth = std::string(R"({"action":"auth","key":")") 
                               + key + R"(","secret":")" + secret + R"("})";
            ws.send(hdl, auth, websocketpp::frame::opcode::text);
        });

        ws.set_message_handler([&](websocketpp::connection_hdl hdl, 
                                    WsClient::message_ptr msg) {
#ifdef HAS_JSON
            try {
                auto arr = json::parse(msg->get_payload());
                for (auto& item : arr) {
                    std::string T = item.value("T", "");
                    if (T == "success" && item.value("msg","") == "authenticated") {
                        // Subscribe to GOOGL quotes and trades
                        std::string sub = R"({"action":"subscribe","quotes":["GOOGL"],"trades":["GOOGL"]})";
                        ws.send(hdl, sub, websocketpp::frame::opcode::text);
                        std::cout << "\n[Feed] Subscribed to GOOGL\n";
                    }
                    else if (T == "q") {  // quote
                        double bp = item.value("bp", 0.0);  // bid price
                        double ap = item.value("ap", 0.0);  // ask price
                        if (bp > 0 && ap > 0) {
                            market.bid.store(bp);
                            market.ask.store(ap);
                            market.last.store((bp + ap) / 2.0);
                            market.timestamp.store(now_ns());
                        }
                    }
                    else if (T == "t") {  // trade
                        double p = item.value("p", 0.0);
                        if (p > 0) market.last.store(p);
                    }
                }
            } catch (...) {}
#endif
        });

        std::string uri = "wss://stream.data.alpaca.markets/v2/iex";
        websocketpp::lib::error_code ec;
        auto con = ws.get_connection(uri, ec);
        ws.connect(con);
        ws.run();
        return;
    }
#endif

    // ── Fallback: simulated GOOGL price feed ─────────────────────────────
    // Starts at ~$175, adds realistic noise + drift
    std::cout << "[Feed] Running simulated GOOGL price feed (no live keys found)\n";
    std::cout << "[Feed] Set ALPACA_KEY and ALPACA_SECRET for live data\n\n";

    double price = 175.00;
    double spread = 0.05;
    int    tick   = 0;

    while (true) {
        // Simulate realistic price movement: random walk + mean reversion
        double noise  = ((std::rand() % 200) - 100) / 10000.0;  // ±0.01
        double drift  = (175.0 - price) * 0.001;                 // mean reversion
        price += noise + drift;

        double bid = price - spread / 2.0;
        double ask = price + spread / 2.0;

        market.bid.store(bid);
        market.ask.store(ask);
        market.last.store(price);
        market.timestamp.store(now_ns());

        tick++;
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); // 10 ticks/sec
    }
}

// ─── Thread 2: Strategy Engine ────────────────────────────────────────────────
// Simple market-making: quote a bid 2bp below mid, ask 2bp above mid.
// When inventory gets large, skew quotes to reduce position.

void strategy_thread_func() {
    const int    LOT_SIZE       = 10;     // shares per order
    const double SPREAD_BPS     = 2.0;    // basis points each side
    const int    MAX_INVENTORY  = 100;    // shares max

    while (true) {
        if (kill_switch.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        double bid = market.bid.load();
        double ask = market.ask.load();
        if (bid <= 0 || ask <= 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        double mid    = (bid + ask) / 2.0;
        double spread = mid * (SPREAD_BPS / 10000.0);
        int    pos    = net_position.load();

        // Inventory skew: if long, lower ask to sell; if short, raise bid to buy
        double skew = pos * 0.001;

        double my_bid = mid - spread - skew;
        double my_ask = mid + spread - skew;

        // Only place orders if we're within inventory limits
        if (pos < MAX_INVENTORY) {
            Order buy_order;
            buy_order.side    = Order::BUY;
            buy_order.price   = my_bid;
            buy_order.qty     = LOT_SIZE;
            buy_order.sent_ns = now_ns();

            std::lock_guard<std::mutex> lock(order_mutex);
            order_queue.push(buy_order);
            orders_sent++;
        }

        if (pos > -MAX_INVENTORY) {
            Order sell_order;
            sell_order.side    = Order::SELL;
            sell_order.price   = my_ask;
            sell_order.qty     = LOT_SIZE;
            sell_order.sent_ns = now_ns();

            std::lock_guard<std::mutex> lock(order_mutex);
            order_queue.push(sell_order);
            orders_sent++;
        }

        // Quote every 50ms — ~20 quotes/sec
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

// ─── Thread 3: Risk Guard ─────────────────────────────────────────────────────
// Watches position and P&L. Flips kill switch if limits are breached.

void risk_thread_func() {
    const int    MAX_POSITION   = 200;    // shares
    const double MAX_DRAWDOWN   = 500.0;  // dollars
    const long long MAX_OPS     = 10000;  // max orders per run

    while (true) {
        int    pos  = std::abs(net_position.load());
        double pnl  = realized_pnl.load();
        long long sent = orders_sent.load();

        if (pos > MAX_POSITION) {
            std::cout << "\n[RISK] Kill switch: position limit breached ("
                      << pos << " > " << MAX_POSITION << ")\n";
            kill_switch.store(true);
        }
        if (pnl < -MAX_DRAWDOWN) {
            std::cout << "\n[RISK] Kill switch: drawdown limit breached ($"
                      << pnl << ")\n";
            kill_switch.store(true);
        }
        if (sent > MAX_OPS) {
            std::cout << "\n[RISK] Kill switch: max orders reached\n";
            kill_switch.store(true);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

// ─── Thread 4: Order Manager ──────────────────────────────────────────────────
// Simulates order fills with realistic latency.
// In a real system, this sends to the Alpaca REST API and waits for fill events.

void order_thread_func() {
    while (true) {
        Order ord;
        bool  has_order = false;

        {
            std::lock_guard<std::mutex> lock(order_mutex);
            if (!order_queue.empty()) {
                ord = order_queue.front();
                order_queue.pop();
                has_order = true;
            }
        }

        if (!has_order) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            continue;
        }

        // Simulate fill: 70% of orders get filled at mid price
        // (in reality, market-making orders may not always fill)
        bool filled = (std::rand() % 10) < 7;
        if (!filled) continue;

        // Simulate network + exchange latency: 150–900 microseconds
        long long lat_us = 150 + (std::rand() % 750);
        std::this_thread::sleep_for(std::chrono::microseconds(lat_us));

        long long fill_time = now_ns();
        long long latency   = fill_time - ord.sent_ns;

        // Update position and P&L
        double fill_price = (market.bid.load() + market.ask.load()) / 2.0;
        if (ord.side == Order::BUY) {
            net_position.fetch_add(ord.qty);
            // Cost basis tracking (simplified)
            realized_pnl.store(realized_pnl.load() - fill_price * ord.qty * 0.001);
        } else {
            net_position.fetch_sub(ord.qty);
            realized_pnl.store(realized_pnl.load() + fill_price * ord.qty * 0.001);
        }

        orders_filled++;

        Fill f;
        f.order     = ord;
        f.filled_ns = fill_time;
        f.latency_ns = latency;

        std::lock_guard<std::mutex> lock(fill_mutex);
        fill_log.push_back(f);

        // Keep fill log bounded
        if (fill_log.size() > 10000) {
            fill_log.erase(fill_log.begin(),
                           fill_log.begin() + 1000);
        }
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

int main() {
#ifdef _WIN32
    // Enable ANSI escape codes on Windows console
    HANDLE hOut = GetStdHandle(STD_OUTPUT_HANDLE);
    DWORD dwMode = 0;
    GetConsoleMode(hOut, &dwMode);
    SetConsoleMode(hOut, dwMode | ENABLE_VIRTUAL_TERMINAL_PROCESSING);
#endif

    std::cout << "=== Mini HFT Demo — GOOGL Paper Trading ===\n";
    std::cout << "Threads: Feed | Strategy | Risk | Order Manager\n";
    std::cout << "Press Ctrl+C to stop and see final stats\n\n";

    // Seed RNG
    std::srand(static_cast<unsigned>(std::time(nullptr)));

    // Launch all 4 threads
    std::thread t1(feed_thread_func);
    std::thread t2(strategy_thread_func);
    std::thread t3(risk_thread_func);
    std::thread t4(order_thread_func);

    // Main thread: print dashboard every 200ms
    while (true) {
        print_dashboard();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // (threads run until Ctrl+C — join would be here in production)
    t1.join(); t2.join(); t3.join(); t4.join();
    return 0;
}