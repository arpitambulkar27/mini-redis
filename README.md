# Mini Redis

A high-performance, from-scratch implementation of a Redis server built in Go using only the standard library.

---

## 🏆 Project Milestones Overview

### 1. Milestone 1: The "Hello World" Socket
- TCP server listening on `0.0.0.0:6379` (with customizable `-port` flag).
- Concurrent client connection handling (dedicated goroutine per connection).
- Graceful client disconnection handling (`io.EOF` and network errors).
- Signal trapping for clean server shutdown (`SIGINT`, `SIGTERM`).

### 2. Milestone 2: Speaking the Language (RESP2 Protocol)
- Full RESP2 protocol deserializer (`resp.Reader`) wrapping `bufio.Reader`.
- Parses incoming RESP Array of Bulk Strings (`*<count>\r\n$<len>\r\n<data>\r\n`) and inline commands.
- Standard RESP serializers: Simple Strings (`+`), Bulk Strings (`$`), Integers (`:`), and Errors (`-`).
- Core commands: `PING` (with and without message arguments), `ECHO <msg>`, and `QUIT`.
- Case-insensitive command dispatch (`ping`, `Ping`, `PING`).
- Persistent connection loop processing multiple commands per connection.

### 3. Milestone 3: The Whiteboard (In-Memory Key-Value Store)
- Thread-safe storage engine (`store.Store`) backed by `map[string]item`.
- Concurrency control with `sync.RWMutex` (shared `RLock()` for reads, exclusive `Lock()` for writes).
- Basic CRUD operations: `SET <key> <value>`, `GET <key>`, and multi-key `DEL <key> [keys...]`.
- Standard Redis null bulk string (`$-1\r\n`) returned for missing keys.
- Command argument count validation with standard Redis error messages.

### 4. Milestone 4: Friday Night Rush (TTL & Key Expiration)
- Per-key expiration deadline tracking using `time.Time`.
- Key expiration commands: `EXPIRE <key> <seconds>` and `TTL <key>`.
- `SET <key> <value> EX <seconds>` support with case-insensitive `EX` flag.
- **Dual Expiration Strategy**:
  - **Passive (Lazy) Eviction**: Expired keys are checked and purged on access during `GET` or `TTL`.
  - **Active (Background Cleaner) Eviction**: Periodic background scanner goroutine (1-second ticker) scanning and removing expired keys.

### 5. Milestone 5: The Benchmark (Performance Tuning & Profiling)
- **Buffered Socket I/O**: `bufio.WriterSize(conn, 32*1024)` batching outgoing socket responses to minimize syscall overhead.
- **Pipelining Optimization**: Intelligent flushing when `reader.Buffered() == 0`, coalescing pipelined command replies (`redis-benchmark -P <N>`) into single network packets.
- **Connection Churn Protection**: Clean socket and goroutine teardown preventing file descriptor and goroutine leaks during rapid connection turnover.
- **High-Performance Server Mode**: Console logging guarded behind optional `-verbose` flag to eliminate console lock contention under high-throughput workloads.
- **High-Concurrency Benchmarking**: Stress-tested with official `redis-benchmark` simulating 50 simultaneous client connections across 100,000 total operations (50,000 SET + 50,000 GET):
  ```bash
  redis-benchmark -p 6379 -n 50000 -c 50 -t set,get -q
  ```
- Zero-reflection fast path in RESP serializers.

---

## 📁 Project Structure

```
d:\Mini Redis\
├── go.mod               # Go module definition (mini-redis)
├── main.go              # Server lifecycle, buffered connection loop & command dispatch
├── main_test.go         # Integration tests (commands, expiration & connection churn)
├── benchmark_test.go    # Performance benchmarks (PING, SET/GET, Pipelined batches)
├── resp/
│   ├── resp.go          # RESP2 deserializer & fast buffered serializer helpers
│   └── resp_test.go     # Unit tests for RESP protocol parser & serializer
├── store/
│   ├── store.go         # Thread-safe in-memory store with dual-strategy TTL/expiration
│   └── store_test.go    # Unit tests & concurrency tests for store and background cleaner
└── README.md            # Project documentation & benchmark guide
```

---

## 🚀 Running the Server

### 1. Prerequisites
Make sure Go (1.21+) is installed:
```bash
go version
```

### 2. Start the Server
Run directly using:
```bash
go run main.go
```
The server will start and listen on `0.0.0.0:6379`:
```text
==================================================
          MINI REDIS (Go) - Milestone 5           
==================================================
[INFO] Mini Redis server listening on 0.0.0.0:6379
[INFO] Waiting for incoming connections... (Press Ctrl+C to stop)
[INFO] Running in high-performance mode (use -verbose for per-command logs)
```

*(Optional flags:)*
```bash
# Custom port
go run main.go -port 6380

# Verbose mode (logs every connection and parsed command)
go run main.go -verbose
```

---

## ⚡ Running Benchmarks

### 1. Using Go Built-in Benchmarks
Run the automated server micro-benchmarks:
```bash
go test -bench=Benchmark -benchmem .
```

Example Benchmark Results:
```text
BenchmarkServerPing-16         	  432729	      4838 ns/op
BenchmarkServerSetGet-16       	   72735	     19458 ns/op
BenchmarkServerPipelined-16    	   44014	     26327 ns/op
```

### 2. Using `redis-benchmark`
Test the server with official Redis benchmarking tools:

```bash
# High-concurrency test: 50 connections, 100,000 total operations (50,000 SET + 50,000 GET)
redis-benchmark -p 6379 -n 50000 -c 50 -t set,get -q

# Standard 100,000 requests across 50 concurrent clients
redis-benchmark -p 6379 -t ping,set,get -n 100000 -q

# Pipelined benchmark (16 commands per pipeline)
redis-benchmark -p 6379 -t ping,set,get -n 100000 -c 50 -P 16 -q
```

---

## 🧪 Running Automated Tests

Run all unit, integration, and concurrency tests across packages:
```bash
go test -v ./...
```

---

## 🗺️ Milestones Roadmap
- [x] **Milestone 1:** The "Hello World" Socket (TCP connection handling & raw byte logging)
- [x] **Milestone 2:** Speaking the Language (RESP2 parser, PING & ECHO)
- [x] **Milestone 3:** The Whiteboard (In-memory GET, SET, DEL)
- [x] **Milestone 4:** Friday Night Rush & Expiration (TTL, EXPIRE, Passive/Active eviction)
- [x] **Milestone 5:** The Benchmark (Buffered I/O, Pipelining, Teardown & Profiling)
