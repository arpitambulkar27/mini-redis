package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"mini-redis/resp"
	"mini-redis/store"
)

// executeCommand executes a parsed RESP command against the thread-safe store and serializes the response.
func executeCommand(w io.Writer, cmd []string, kv *store.Store) error {
	cmdName := strings.ToUpper(cmd[0])
	args := cmd[1:]

	switch cmdName {
	case "PING":
		if len(args) == 0 {
			return resp.WriteSimpleString(w, "PONG")
		} else if len(args) == 1 {
			return resp.WriteBulkString(w, args[0])
		}
		return resp.WriteError(w, "wrong number of arguments for 'ping' command")

	case "ECHO":
		if len(args) != 1 {
			return resp.WriteError(w, "wrong number of arguments for 'echo' command")
		}
		return resp.WriteBulkString(w, args[0])

	case "SET":
		if len(args) == 2 {
			kv.Set(args[0], args[1])
			return resp.WriteSimpleString(w, "OK")
		} else if len(args) == 4 {
			if strings.ToUpper(args[2]) != "EX" {
				return resp.WriteError(w, "syntax error")
			}
			secs, err := strconv.Atoi(args[3])
			if err != nil || secs <= 0 {
				return resp.WriteError(w, "value is not an integer or out of range")
			}
			kv.SetWithTTL(args[0], args[1], time.Duration(secs)*time.Second)
			return resp.WriteSimpleString(w, "OK")
		}
		return resp.WriteError(w, "wrong number of arguments for 'set' command")

	case "GET":
		if len(args) != 1 {
			return resp.WriteError(w, "wrong number of arguments for 'get' command")
		}
		val, ok := kv.Get(args[0])
		if !ok {
			return resp.WriteNullBulkString(w)
		}
		return resp.WriteBulkString(w, val)

	case "DEL":
		if len(args) < 1 {
			return resp.WriteError(w, "wrong number of arguments for 'del' command")
		}
		deletedCount := kv.Del(args...)
		return resp.WriteInteger(w, deletedCount)

	case "EXPIRE":
		if len(args) != 2 {
			return resp.WriteError(w, "wrong number of arguments for 'expire' command")
		}
		secs, err := strconv.Atoi(args[1])
		if err != nil {
			return resp.WriteError(w, "value is not an integer or out of range")
		}
		if kv.Expire(args[0], secs) {
			return resp.WriteInteger(w, 1)
		}
		return resp.WriteInteger(w, 0)

	case "TTL":
		if len(args) != 1 {
			return resp.WriteError(w, "wrong number of arguments for 'ttl' command")
		}
		ttl := kv.TTL(args[0])
		return resp.WriteInteger(w, ttl)

	case "QUIT":
		_ = resp.WriteSimpleString(w, "OK")
		return io.EOF

	default:
		return resp.WriteError(w, fmt.Sprintf("unknown command '%s'", cmd[0]))
	}
}

// handleConnection manages an individual client's lifecycle in its own goroutine.
func handleConnection(conn net.Conn, kv *store.Store, verbose bool) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	if verbose {
		log.Printf("[INFO] Client connected: %s\n", remoteAddr)
	}

	reader := bufio.NewReader(conn)
	// Buffered writer to coalesce network writes and minimize system call overhead
	writer := bufio.NewWriterSize(conn, 32*1024)
	defer writer.Flush()

	// Keep connection open in a loop so the client can send multiple commands across the same connection
	for {
		cmd, err := resp.ReadCommand(reader)
		if err != nil {
			if verbose {
				if errors.Is(err, io.EOF) {
					log.Printf("[INFO] Client disconnected: %s\n", remoteAddr)
				} else {
					log.Printf("[INFO] Client %s connection closed: %v\n", remoteAddr, err)
				}
			}
			return
		}

		if len(cmd) == 0 {
			continue
		}

		if verbose {
			log.Printf("[DEBUG] Command received from %s: %v\n", remoteAddr, cmd)
		}

		if err := executeCommand(writer, cmd, kv); err != nil {
			if verbose {
				if errors.Is(err, io.EOF) {
					log.Printf("[INFO] Client disconnected: %s\n", remoteAddr)
				} else {
					log.Printf("[ERROR] Failed to send response to %s: %v\n", remoteAddr, err)
				}
			}
			_ = writer.Flush()
			return
		}

		// Pipelining optimization: flush immediately if no more commands are waiting in the input buffer
		if reader.Buffered() == 0 {
			if err := writer.Flush(); err != nil {
				return
			}
		}
	}
}

func main() {
	port := flag.Int("port", 6379, "Port to listen on")
	verbose := flag.Bool("verbose", false, "Enable verbose per-command logging (disabled by default for high-throughput benchmarks)")
	flag.Parse()

	address := fmt.Sprintf("0.0.0.0:%d", *port)

	// Bind and listen on TCP
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("[ERROR] Failed to bind to %s: %v\n", address, err)
	}
	defer listener.Close()

	// Initialize shared thread-safe key-value store with TTL support
	kvStore := store.New()

	// Spawn background active cleaner scanning every 1 second
	stopCleaner := kvStore.StartCleaner(1 * time.Second)
	defer stopCleaner()

	fmt.Println("==================================================")
	fmt.Println("          MINI REDIS (Go) - Milestone 5           ")
	fmt.Println("==================================================")
	log.Printf("[INFO] Mini Redis server listening on %s\n", address)
	log.Printf("[INFO] Waiting for incoming connections... (Press Ctrl+C to stop)\n")
	if !*verbose {
		log.Printf("[INFO] Running in high-performance mode (use -verbose for per-command logs)\n")
	}

	// Trap interrupt signals (SIGINT, SIGTERM) for clean shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Printf("[INFO] Shutdown signal received. Closing listener...\n")
		stopCleaner()
		listener.Close()
		os.Exit(0)
	}()

	// Infinite loop accepting incoming connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-sigChan:
				return
			default:
				log.Printf("[WARN] Failed to accept connection: %v\n", err)
				continue
			}
		}

		// Handle connection concurrently in a separate goroutine
		go handleConnection(conn, kvStore, *verbose)
	}
}
