# Mini Redis - Milestone 5: Benchmark Readiness & Performance Tuning

A high-performance, from-scratch implementation of a Redis server built in Go using only the standard library.

---

## 🎯 Milestone 5 Goals

1. **Buffered I/O for Socket Writes**:
   - Outgoing socket responses are wrapped with `bufio.WriterSize(conn, 32*1024)`.
   - Coalesces per-command replies into buffered chunks to eliminate excessive syscall overhead.
   - **Pipelining Optimization**: Flushes immediately when `reader.Buffered() == 0`, allowing batches of pipelined commands (`redis-benchmark -P <N>`) to be written in a single TCP frame.
   - Fast-path string and integer writing in RESP serializers avoiding reflection.
2. **Graceful Connection Teardown & Zero Goroutine Leaks**:
   - Connection handling guarantees socket closures via `defer conn.Close()` and `defer writer.Flush()`.
   - Rapid connection churn from benchmark runs does not leak file descriptors or goroutines.
3. **Concurrency & Thread Safety**:
   - Read/write operations and TTL expiration are fully synchronized via `sync.RWMutex`.
   - Zero shared mutable state across connection goroutines.
4. **All Commands Intact**:
   - `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXPIRE`, `TTL`, `SET ... EX`, `QUIT`.
   - Case-insensitive command lookup.
   - Strictly standard library Go (compiles cleanly with `go run main.go`).
 5. **High-Concurrency Benchmarking & Performance**
   - Stress-tested against the official `redis-benchmark` utility simulating 50 simultaneous client connections across 100,000 total operations (50,000 SET + 50,000 GET):
```bash
redis-benchmark -p 6379 -n 50000 -c 50 -t set,get -q

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
