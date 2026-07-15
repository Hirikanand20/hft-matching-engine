

Ultra-Low Latency HFT Order Book and Execution Simulator

<img width="1412" height="905" alt="t1" src="https://github.com/user-attachments/assets/af4e32c9-8d24-48e6-b8d5-1221625653e6" />
<img width="1533" height="214" alt="t2" src="https://github.com/user-attachments/assets/e0396d08-e047-473e-bca1-6da897451e44" />






An end-to-end, high-performance High-Frequency Trading (HFT) pipeline. This system bridges a deterministic C++ Matching Engine, a highly optimized Node.js WebSocket Gateway, and a zero-allocation React/TypeScript Frontend to stream microsecond-level market events and order book updates in real time.

Project Demonstration

To see the system in action, watch the video demonstration at: https://www.youtube.com/watch?v=jKczt7WSMz4

This video showcases the live system under heavy load, illustrating dynamic price discovery, sub-millisecond Pub/Sub streaming, and low-latency frontend rendering.

System Architecture

The pipeline uses a highly optimized three-tier architecture to handle extreme throughput while ensuring deterministic execution latency:

1. C++ Matching Engine: Maintained as a deterministic state machine managing fast Bid/Ask memory limit queues using strict memory-locality patterns.
2. Node.js Web Gateway: Actively monitors backpressure while serving as an asymmetric, non-blocking bridge between the back-end and Web UI.
3. React/TypeScript Frontend: Decodes raw binary TCP payloads using Web-native, zero-copy memory offsets for maximum UI rendering efficiency.

Core Engineering Decisions and Performance Optimizations

1. CPU Cache Locality: Array of Structs (AoS) vs. Linked Lists
Standard textbook order books utilize Doubly Linked Lists for O(1) node alterations. However, in low-latency systems, pointer-chasing scatters memory across RAM, introducing fatal CPU cache misses. Our approach structures order allocations using a contiguous Array of Structs with strict alignment (alignas). This allows the hardware prefetcher to load adjacent orders into L1/L3 CPU caches in advance. We also eliminate dynamic new and delete allocations on the hot path, bypassing standard heap fragmentation and operating system allocation overheads.
2. Zero-Copy Binary Serialization (Downstream Hot-Path)
To completely bypass costly JSON stringification overhead in the market-data hot path, the C++ engine streams raw 56-byte binary packets directly to network sockets via Redis. The Order Event Packet (56 Bytes) packs sequence numbers, fixed-char asset identifiers like RELIANCE.NS, double-precision floating-point prices, volumes, and flags (A for Add, C for Cancel, E for Execute). The Snapshot Payload (Dynamic) transmits high-density arrays of 12-byte binary price/quantity segments on request so newly connected browsers can load instantly without rendering historical gaps.
3. Dynamic Price Discovery
Instead of using static mock tickers, the matching engine adjusts prices dynamically based on physical supply and demand. Aggressive buys consume available sells, lifting the Last Traded Price higher, while intense sells deplete bidding depth, causing the spread to drop.
4. Active Backpressure Protections and Flow Controls
Sub-millisecond data rates can easily choke web browsers. The Node.js gateway acts as an automated circuit breaker: If a browser's WebSocket buffer queue (ws.bufferedAmount) exceeds 1MB, it selectively drops frames. If queue buffers accumulate beyond 5MB or drop over 100 consecutive frames, the gateway terminates the socket, shielding the system against resource exhaustion.
5. Bit-Level Parsing in React (DataView)
The React client abandons traditional JSON string parsing. It consumes raw incoming ArrayBuffers natively via JavaScript's DataView, directly translating sequence bytes, big integers, and float representations into memory arrays.

Repository Structure

The root directory contains:

* engine/: C++ Matching Engine folder (with CMake, libcurl, hiredis, sqlite3).
* gateway/: Node.js WebSocket Gateway folder (with Express, ioredis, ws).
* frontend/: React and TypeScript Frontend folder (with Vite, TailwindCSS).
* .gitignore: Global git configurations (ignores node_modules, build, etc.).

Setup and Execution Guide

Prerequisites
You will need a C++ Compiler like MSVC for Windows or GCC/Clang for Linux/macOS supporting C++17 or higher. You will also need a package manager like vcpkg or conan (for libcurl, hiredis, sqlite3, and nlohmann/json). Finally, make sure you have Node.js (v18+) and Docker Desktop installed.

1. Spin Up the Redis Broker
Run your Docker container to host the Redis publish/subscribe environment:
docker run -d --name hft-redis -p 6379:6379 redis:latest
2. Launch the Node.js WebSocket Gateway
Navigate to the gateway directory, install packages, and start the script:
cd gateway
npm install
node gateway.js
3. Run the Frontend UI
Navigate to the frontend directory, install packages, and run the development server:
cd frontend
npm install
npm run dev
Once initialized, open http://localhost:5173 on your browser.
4. Compile and Execute the C++ Matching Engine
Navigate to the engine directory, configure with CMake, compile, and run:
cd engine
cmake -B build -S .
cmake --build build -j
build\engine.exe

System Metrics and Output Verification
As soon as the C++ engine starts up, you will see real-time performance streaming logs across your pipeline.

The C++ Output will show:
[prices] fetched via v8/chart (per-symbol fallback)
Started engine for RELIANCE.NS at price 1275.90
Started engine for TCS.NS at price 2057.50
Started engine for VBL.NS at price 474.55
All threads started...

The Gateway Output will show:
WS gateway on :3001
[Redis] Subscribed to book and snapshot channels...
[WebSocket] Client connected: subscribed to RELIANCE.NS
