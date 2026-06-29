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
 *   export ALPACA_KEY="YOUR_KEY"
 *   export ALPACA_SECRET="YOUR_SECRET"
 *   ./hft_demo
 *
 * Output CSVs (written on Ctrl+C):
 *   hft_ticks.csv     — every price tick (bid, ask, mid)
 *   hft_fills.csv     — every order fill with latency
 *   hft_snapshots.csv — position & P&L sampled every second
 *   hft_summary.csv   — final stats
 */

#include <iostream>
#include <fstream>
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
#include <csignal>
#ifdef _WIN32
#include <windows.h>
#endif

using namespace std;

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
    long long timestamp_ns;
};

struct Order {
    enum Side { BUY, SELL };
    Side     side;
    double   price;
    int      qty;
    long long sent_ns;
};

struct Fill {
    Order    order;
    long long filled_ns;
    long long latency_ns;
    double   fill_price;
};

// ─── Log Structures (for CSV export) ─────────────────────────────────────────

struct TickLog {
    long long timestamp_ns;
    double bid, ask, mid;
};

struct Snapshot {
    long long timestamp_ns;
    int       position;
    double    pnl;
    long long orders_sent;
    long long orders_filled;
    double    bid, ask;
};

// ─── Shared State ─────────────────────────────────────────────────────────────

struct alignas(64) MarketData {
    std::atomic<double>    bid{0.0};
    std::atomic<double>    ask{0.0};
    std::atomic<double>    last{0.0};
    std::atomic<long long> timestamp{0};
};
MarketData market;

std::queue<Order> order_queue;
std::mutex        order_mutex;

std::vector<Fill> fill_log;
std::mutex        fill_mutex;

std::vector<TickLog>  tick_log;
std::mutex            tick_log_mutex;

std::vector<Snapshot> snapshot_log;
std::mutex            snapshot_mutex;

std::atomic<bool>      kill_switch{false};
std::atomic<bool>      running{true};       // set false on Ctrl+C

std::atomic<int>       net_position{0};
std::atomic<double>    realized_pnl{0.0};
std::atomic<long long> orders_sent{0};
std::atomic<long long> orders_filled{0};

// ─── Helpers ──────────────────────────────────────────────────────────────────

long long now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()
    ).count();
}

// ─── CSV Export ───────────────────────────────────────────────────────────────

void write_csv() {
    std::cout << "\n\nWriting CSV files...\n";

    // 1. hft_ticks.csv
    {
        std::ofstream f("hft_ticks.csv");
        f << "timestamp_ns,bid,ask,mid\n";
        std::lock_guard<std::mutex> lock(tick_log_mutex);
        f << std::fixed << std::setprecision(4);
        for (auto& t : tick_log)
            f << t.timestamp_ns << "," << t.bid << "," << t.ask << "," << t.mid << "\n";
        std::cout << "  hft_ticks.csv     — " << tick_log.size() << " ticks\n";
    }

    // 2. hft_fills.csv
    {
        std::ofstream f("hft_fills.csv");
        f << "fill_timestamp_ns,side,order_price,fill_price,qty,latency_us\n";
        std::lock_guard<std::mutex> lock(fill_mutex);
        f << std::fixed << std::setprecision(4);
        for (auto& fl : fill_log) {
            f << fl.filled_ns << ","
              << (fl.order.side == Order::BUY ? "BUY" : "SELL") << ","
              << fl.order.price << ","
              << fl.fill_price << ","
              << fl.order.qty << ","
              << (fl.latency_ns / 1000.0) << "\n";
        }
        std::cout << "  hft_fills.csv     — " << fill_log.size() << " fills\n";
    }

    // 3. hft_snapshots.csv
    {
        std::ofstream f("hft_snapshots.csv");
        f << "timestamp_ns,position,pnl,orders_sent,orders_filled,bid,ask\n";
        std::lock_guard<std::mutex> lock(snapshot_mutex);
        f << std::fixed << std::setprecision(4);
        for (auto& s : snapshot_log) {
            f << s.timestamp_ns << ","
              << s.position << ","
              << s.pnl << ","
              << s.orders_sent << ","
              << s.orders_filled << ","
              << s.bid << ","
              << s.ask << "\n";
        }
        std::cout << "  hft_snapshots.csv — " << snapshot_log.size() << " snapshots\n";
    }

    // 4. hft_summary.csv
    {
        std::ofstream f("hft_summary.csv");
        f << "metric,value\n";

        double avg_lat = 0, min_lat = 1e9, max_lat = 0;
        {
            std::lock_guard<std::mutex> lock(fill_mutex);
            if (!fill_log.empty()) {
                double sum = 0;
                for (auto& fl : fill_log) {
                    double lat = fl.latency_ns / 1000.0;
                    sum += lat;
                    if (lat < min_lat) min_lat = lat;
                    if (lat > max_lat) max_lat = lat;
                }
                avg_lat = sum / fill_log.size();
            }
        }

        long long sent   = orders_sent.load();
        long long filled = orders_filled.load();
        f << std::fixed << std::setprecision(4);
        f << "symbol,GOOGL\n";
        f << "orders_sent,"   << sent   << "\n";
        f << "orders_filled," << filled << "\n";
        f << "fill_rate_pct," << (sent > 0 ? (100.0 * filled / sent) : 0) << "\n";
        f << "final_position," << net_position.load() << "\n";
        f << "final_pnl,"      << realized_pnl.load() << "\n";
        f << "avg_latency_us," << avg_lat << "\n";
        f << "min_latency_us," << (min_lat < 1e9 ? min_lat : 0) << "\n";
        f << "max_latency_us," << max_lat << "\n";
        f << "total_ticks,";
        { std::lock_guard<std::mutex> lock(tick_log_mutex); f << tick_log.size(); }
        f << "\n";
        std::cout << "  hft_summary.csv   — final stats\n";
    }

    std::cout << "Done. CSV files saved in current directory.\n";
}

// ─── Signal / Ctrl+C Handler ──────────────────────────────────────────────────

#ifdef _WIN32
BOOL WINAPI ctrl_handler(DWORD dwType) {
    if (dwType == CTRL_C_EVENT || dwType == CTRL_BREAK_EVENT) {
        running.store(false);
        return TRUE;
    }
    return FALSE;
}
#else
void sig_handler(int) { running.store(false); }
#endif

// ─── Dashboard ────────────────────────────────────────────────────────────────

void print_dashboard() {
    double bid  = market.bid.load();
    double ask  = market.ask.load();
    double mid  = (bid + ask) / 2.0;
    int    pos  = net_position.load();
    double pnl  = realized_pnl.load();
    long long sent   = orders_sent.load();
    long long filled = orders_filled.load();

    double avg_lat = 0, min_lat = 1e9, max_lat = 0;
    {
        std::lock_guard<std::mutex> lock(fill_mutex);
        if (!fill_log.empty()) {
            double sum = 0;
            for (auto& f : fill_log) {
                double lat = f.latency_ns / 1000.0;
                sum += lat;
                if (lat < min_lat) min_lat = lat;
                if (lat > max_lat) max_lat = lat;
            }
            avg_lat = sum / fill_log.size();
        }
    }

    std::cout << "\r\033[K";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "[GOOGL] "
              << "Bid:" << bid << " Ask:" << ask << " Mid:" << mid
              << "  Pos:" << std::showpos << pos << std::noshowpos
              << "  P&L:$" << std::showpos << pnl << std::noshowpos
              << "  Orders:" << sent << "/" << filled
              << "  Lat(us): avg=" << avg_lat
              << " min=" << (min_lat < 1e9 ? min_lat : 0)
              << " max=" << max_lat
              << (kill_switch ? "  [KILLED]" : "")
              << std::flush;
}

// ─── Thread 1: Feed ───────────────────────────────────────────────────────────

void feed_thread_func() {
    const char* key    = std::getenv("ALPACA_KEY");
    const char* secret = std::getenv("ALPACA_SECRET");
    bool use_live = (key != nullptr && secret != nullptr);

#ifdef HAS_WEBSOCKETPP
    if (use_live) {
        using WsClient = websocketpp::client<websocketpp::config::asio_tls_client>;
        WsClient ws;
        ws.init_asio();
        ws.set_tls_init_handler([](websocketpp::connection_hdl) {
            return std::make_shared<boost::asio::ssl::context>(
                boost::asio::ssl::context::tlsv12_client);
        });
        ws.set_open_handler([&](websocketpp::connection_hdl hdl) {
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
                        std::string sub = R"({"action":"subscribe","quotes":["GOOGL"],"trades":["GOOGL"]})";
                        ws.send(hdl, sub, websocketpp::frame::opcode::text);
                        std::cout << "\n[Feed] Subscribed to GOOGL\n";
                    }
                    else if (T == "q") {
                        double bp = item.value("bp", 0.0);
                        double ap = item.value("ap", 0.0);
                        if (bp > 0 && ap > 0) {
                            market.bid.store(bp);
                            market.ask.store(ap);
                            market.last.store((bp + ap) / 2.0);
                            long long ts = now_ns();
                            market.timestamp.store(ts);
                            // Log tick
                            std::lock_guard<std::mutex> lock(tick_log_mutex);
                            tick_log.push_back({ts, bp, ap, (bp + ap) / 2.0});
                        }
                    }
                    else if (T == "t") {
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

    // Simulated feed
    std::cout << "[Feed] Running simulated GOOGL price feed (no live keys found)\n\n";
    double price = 175.00;
    double spread = 0.05;

    while (running.load()) {
        double noise = ((std::rand() % 200) - 100) / 10000.0;
        double drift = (175.0 - price) * 0.001;
        price += noise + drift;

        double bid = price - spread / 2.0;
        double ask = price + spread / 2.0;
        long long ts = now_ns();

        market.bid.store(bid);
        market.ask.store(ask);
        market.last.store(price);
        market.timestamp.store(ts);

        {
            std::lock_guard<std::mutex> lock(tick_log_mutex);
            if (tick_log.size() < 500000)
                tick_log.push_back({ts, bid, ask, price});
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

// ─── Thread 2: Strategy ───────────────────────────────────────────────────────

void strategy_thread_func() {
    const int    LOT_SIZE      = 10;
    const double SPREAD_BPS    = 2.0;
    const int    MAX_INVENTORY = 100;

    while (running.load()) {
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

        double mid   = (bid + ask) / 2.0;
        double spread = mid * (SPREAD_BPS / 10000.0);
        int    pos   = net_position.load();
        double skew  = pos * 0.001;

        double my_bid = mid - spread - skew;
        double my_ask = mid + spread - skew;

        if (pos < MAX_INVENTORY) {
            Order o; o.side = Order::BUY; o.price = my_bid;
            o.qty = LOT_SIZE; o.sent_ns = now_ns();
            std::lock_guard<std::mutex> lock(order_mutex);
            order_queue.push(o); orders_sent++;
        }
        if (pos > -MAX_INVENTORY) {
            Order o; o.side = Order::SELL; o.price = my_ask;
            o.qty = LOT_SIZE; o.sent_ns = now_ns();
            std::lock_guard<std::mutex> lock(order_mutex);
            order_queue.push(o); orders_sent++;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

// ─── Thread 3: Risk ───────────────────────────────────────────────────────────

void risk_thread_func() {
    const int    MAX_POSITION = 200;
    const double MAX_DRAWDOWN = 500.0;
    const long long MAX_OPS   = 10000;

    while (running.load()) {
        int    pos  = std::abs(net_position.load());
        double pnl  = realized_pnl.load();
        long long sent = orders_sent.load();

        if (pos > MAX_POSITION) {
            std::cout << "\n[RISK] Kill switch: position limit (" << pos << ")\n";
            kill_switch.store(true);
        }
        if (pnl < -MAX_DRAWDOWN) {
            std::cout << "\n[RISK] Kill switch: drawdown ($" << pnl << ")\n";
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

void order_thread_func() {
    while (running.load()) {
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

        bool filled = (std::rand() % 10) < 7;
        if (!filled) continue;

        long long lat_us = 150 + (std::rand() % 750);
        std::this_thread::sleep_for(std::chrono::microseconds(lat_us));

        long long fill_time = now_ns();
        long long latency   = fill_time - ord.sent_ns;
        double fill_price   = (market.bid.load() + market.ask.load()) / 2.0;

        if (ord.side == Order::BUY) {
            net_position.fetch_add(ord.qty);
            realized_pnl.store(realized_pnl.load() - fill_price * ord.qty * 0.001);
        } else {
            net_position.fetch_sub(ord.qty);
            realized_pnl.store(realized_pnl.load() + fill_price * ord.qty * 0.001);
        }
        orders_filled++;

        Fill f;
        f.order      = ord;
        f.filled_ns  = fill_time;
        f.latency_ns = latency;
        f.fill_price = fill_price;

        std::lock_guard<std::mutex> lock(fill_mutex);
        fill_log.push_back(f);
        if (fill_log.size() > 100000)
            fill_log.erase(fill_log.begin(), fill_log.begin() + 10000);
    }
}

// ─── Thread 5: Snapshot Logger ────────────────────────────────────────────────

void snapshot_thread_func() {
    while (running.load()) {
        Snapshot s;
        s.timestamp_ns  = now_ns();
        s.position      = net_position.load();
        s.pnl           = realized_pnl.load();
        s.orders_sent   = orders_sent.load();
        s.orders_filled = orders_filled.load();
        s.bid           = market.bid.load();
        s.ask           = market.ask.load();

        {
            std::lock_guard<std::mutex> lock(snapshot_mutex);
            snapshot_log.push_back(s);
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

int main() {
#ifdef _WIN32
    HANDLE hOut = GetStdHandle(STD_OUTPUT_HANDLE);
    DWORD dwMode = 0;
    GetConsoleMode(hOut, &dwMode);
    SetConsoleMode(hOut, dwMode | ENABLE_VIRTUAL_TERMINAL_PROCESSING);
    SetConsoleCtrlHandler(ctrl_handler, TRUE);
#else
    signal(SIGINT, sig_handler);
#endif

    std::cout << "=== Mini HFT Demo — GOOGL Paper Trading ===\n";
    std::cout << "Threads: Feed | Strategy | Risk | Order Manager | Snapshot Logger\n";
    std::cout << "Press Ctrl+C to stop and export CSV files\n\n";

    std::srand(static_cast<unsigned>(std::time(nullptr)));

    std::thread t1(feed_thread_func);
    std::thread t2(strategy_thread_func);
    std::thread t3(risk_thread_func);
    std::thread t4(order_thread_func);
    std::thread t5(snapshot_thread_func);

    while (running.load()) {
        print_dashboard();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "\nStopping threads...\n";
    t1.detach(); t2.join(); t3.join(); t4.join(); t5.join();

    write_csv();
    return 0;
}
