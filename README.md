# ⚡ Low-Latency HFT Engine — GOOGL on Alpaca Paper Trading

> A 4-thread C++ High-Frequency Trading engine with nanosecond latency tracking, lock-free atomics, and a real market-making strategy.

---

## Architecture — How the 4 Threads Work Together

```
┌─────────────────────────────────────────────────────────────────────┐
│                        HFT ENGINE (main.cpp)                        │
│                                                                     │
│   ┌─────────────────┐        std::atomic<double>                   │
│   │   THREAD 1      │ ──── bid / ask / mid ────►┐                  │
│   │  Market Feed    │                            │                  │
│   │                 │   (Alpaca WebSocket or      │                  │
│   │  Streams live   │    simulated price feed)   │                  │
│   │  GOOGL bid/ask  │                            │                  │
│   └─────────────────┘                            │                  │
│                                                  ▼                  │
│   ┌─────────────────┐      mutex-protected   ┌──────────────────┐  │
│   │   THREAD 3      │◄─── position / P&L ───│   THREAD 2       │  │
│   │  Risk Guard     │                        │  Strategy Engine  │  │
│   │                 │                        │                   │  │
│   │  Monitors:      │   kill switch          │  Market-making:   │  │
│   │  • Position     │ ──std::atomic<bool>──► │  • Quotes spread  │  │
│   │  • P&L limits   │                        │  • Inventory skew │  │
│   │  • Kill switch  │                        │  • Mid-price algo │  │
│   └─────────────────┘                        └────────┬──────────┘  │
│                                                       │             │
│                                              order_queue (mutex)    │
│                                                       │             │
│                                                       ▼             │
│                                              ┌─────────────────┐   │
│                                              │   THREAD 4      │   │
│                                              │  Order Manager  │   │
│                                              │                 │   │
│                                              │  • Simulates    │   │
│                                              │    fills        │   │
│                                              │  • Tracks       │   │
│                                              │    latency (ns) │   │
│                                              └─────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow — One Trading Cycle

```
  Alpaca WebSocket
  (or sim feed)
        │
        ▼
  ┌───────────┐
  │  Thread 1 │  ── writes ──►  atomic<bid>, atomic<ask>, atomic<mid>
  │  Feed     │
  └───────────┘
                                        │
                                        ▼
                              ┌───────────────────┐
                              │     Thread 2      │
                              │  Strategy Engine  │
                              │                   │
                              │  mid ± spread     │
                              │  inventory skew   │
                              │  → place order    │
                              └────────┬──────────┘
                                       │
                              push to order_queue
                                       │
                                       ▼
                              ┌───────────────────┐
                              │     Thread 4      │
                              │  Order Manager    │
                              │                   │
                              │  pop order        │
                              │  simulate fill    │──► update position
                              │  record latency   │    update P&L
                              └───────────────────┘
                                                        │
                                                        ▼
                                              ┌──────────────────┐
                                              │    Thread 3      │
                                              │   Risk Guard     │
                                              │                  │
                                              │  pos > limit?    │──► KILL SWITCH
                                              │  P&L < floor?    │    (atomic bool)
                                              └──────────────────┘
```

---

## Concurrency Model

| Mechanism | Used For | Why |
|---|---|---|
| `std::atomic<double>` | bid / ask / mid price | Lock-free reads across threads |
| `std::atomic<bool>` | Kill switch | Instant propagation, no mutex overhead |
| `std::mutex` + `std::queue` | Order queue (T2 → T4) | Safe producer-consumer handoff |
| `std::mutex` | Position & P&L updates | Prevent torn reads on multi-field state |
| `std::chrono::high_resolution_clock` | Latency tracking | Nanosecond timestamps per order |

---

## Quick Start — Simulated Mode (no API keys needed)

```bash
g++ -std=c++17 -O2 -pthread main.cpp -o hft_demo
./hft_demo
```

Runs instantly with a built-in GOOGL price simulator.

---

## Live Mode — Real Alpaca Data

### 1. Install dependencies

```bash
# Ubuntu / WSL
sudo apt update && sudo apt install \
  libwebsocketpp-dev libboost-all-dev libssl-dev nlohmann-json3-dev

# macOS
brew install websocketpp boost openssl nlohmann-json
```

### 2. Set Alpaca paper trading keys

```bash
export ALPACA_KEY="YOUR_KEY_HERE"
export ALPACA_SECRET="YOUR_SECRET_HERE"
```

Get keys at: [app.alpaca.markets](https://app.alpaca.markets) → API (left sidebar)

### 3. Build and run

```bash
g++ -std=c++17 -O2 -pthread main.cpp \
    -lssl -lcrypto -lboost_system -o hft_demo
./hft_demo
```

---

## Live Output

```
[GOOGL] Bid:175.02 Ask:175.08 Mid:175.05  Pos:+30  P&L:$1.42  Orders:1842/1290  Lat(us): avg=342 min=181 max=891
```

| Field | Meaning |
|---|---|
| `Bid / Ask / Mid` | Latest market data from Thread 1 |
| `Pos` | Net share position (Thread 4 updates) |
| `P&L` | Unrealized profit/loss |
| `Orders sent/filled` | Strategy output vs fills |
| `Lat avg/min/max` | Nanosecond order latency stats |

---

## Key Concepts Demonstrated

```
Lock-free atomics ──────────► Zero contention on hot path (price reads)
Mutex-protected queue ──────► Safe order handoff without busy-waiting
Kill switch pattern ────────► Instant risk shutdown across all threads
Nanosecond latency tracking ► Real HFT-grade performance measurement
Market-making strategy ─────► Spread quoting + inventory skew logic
```

---

## Project Structure

```
Threads_HFT/
├── main.cpp          — full engine, single file
├── CMakeLists.txt    — cmake build config
└── README.md         — this file
```

---

> Built to demonstrate production HFT concepts: lock-free data structures, thread coordination, and sub-microsecond latency measurement in C++17.
