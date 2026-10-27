package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"mini-redis/store"
)

func TestServerCommands(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	kvStore := store.New()
	go handleConnection(serverConn, kvStore, false)

	reader := bufio.NewReader(clientConn)

	tests := []struct {
		name     string
		payload  string
		expected string
		wait     time.Duration
	}{
		// PING
		{
			name:     "PING without arguments",
			payload:  "*1\r\n$4\r\nPING\r\n",
			expected: "+PONG\r\n",
		},
		{
			name:     "ping lowercase without arguments",
			payload:  "*1\r\n$4\r\nping\r\n",
			expected: "+PONG\r\n",
		},
		{
			name:     "PING with argument",
			payload:  "*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n",
			expected: "$5\r\nhello\r\n",
		},
		// ECHO
		{
			name:     "ECHO with argument",
			payload:  "*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n",
			expected: "$11\r\nhello world\r\n",
		},
		// GET non-existent key
		{
			name:     "GET non-existent key returns null bulk string",
			payload:  "*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n",
			expected: "$-1\r\n",
		},
		// SET key value
		{
			name:     "SET key value",
			payload:  "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n",
			expected: "+OK\r\n",
		},
		// GET existing key
		{
			name:     "GET existing key",
			payload:  "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n",
			expected: "$7\r\nmyvalue\r\n",
		},
		// TTL of persistent key
		{
			name:     "TTL of persistent key returns -1",
			payload:  "*2\r\n$3\r\nTTL\r\n$5\r\nmykey\r\n",
			expected: ":-1\r\n",
		},
		// EXPIRE on existing key
		{
			name:     "EXPIRE on existing key returns 1",
			payload:  "*3\r\n$6\r\nEXPIRE\r\n$5\r\nmykey\r\n$2\r\n60\r\n",
			expected: ":1\r\n",
		},
		// TTL of expiring key
		{
			name:     "TTL of expiring key returns positive integer",
			payload:  "*2\r\n$3\r\nttl\r\n$5\r\nmykey\r\n",
			expected: ":60\r\n",
		},
		// EXPIRE on non-existent key
		{
			name:     "EXPIRE on non-existent key returns 0",
			payload:  "*3\r\n$6\r\nexpire\r\n$7\r\nmissing\r\n$2\r\n10\r\n",
			expected: ":0\r\n",
		},
		// TTL of non-existent key
		{
			name:     "TTL of non-existent key returns -2",
			payload:  "*2\r\n$3\r\nTTL\r\n$7\r\nmissing\r\n",
			expected: ":-2\r\n",
		},
		// SET with EX option
		{
			name:     "SET with EX 10",
			payload:  "*5\r\n$3\r\nSET\r\n$9\r\nexpirekey\r\n$3\r\nval\r\n$2\r\nEX\r\n$2\r\n10\r\n",
			expected: "+OK\r\n",
		},
		{
			name:     "TTL of key set with EX",
			payload:  "*2\r\n$3\r\nTTL\r\n$9\r\nexpirekey\r\n",
			expected: ":10\r\n",
		},
		// SET with lowercase ex option and short TTL for passive expiration test
		{
			name:     "SET with lowercase ex 1 second",
			payload:  "*5\r\n$3\r\nset\r\n$8\r\nshortkey\r\n$4\r\ntemp\r\n$2\r\nex\r\n$1\r\n1\r\n",
			expected: "+OK\r\n",
		},
		{
			name:     "GET before expiration returns value",
			payload:  "*2\r\n$3\r\nGET\r\n$8\r\nshortkey\r\n",
			expected: "$4\r\ntemp\r\n",
		},
		{
			name:     "GET after expiration returns null bulk string (Passive Expiration)",
			payload:  "*2\r\n$3\r\nGET\r\n$8\r\nshortkey\r\n",
			expected: "$-1\r\n",
			wait:     1100 * time.Millisecond,
		},
		{
			name:     "TTL after expiration returns -2",
			payload:  "*2\r\n$3\r\nTTL\r\n$8\r\nshortkey\r\n",
			expected: ":-2\r\n",
		},
		// DEL existing key
		{
			name:     "DEL existing key",
			payload:  "*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n",
			expected: ":1\r\n",
		},
		// DEL non-existent key
		{
			name:     "DEL missing key returns 0",
			payload:  "*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n",
			expected: ":0\r\n",
		},
		// Error handling for wrong argument counts
		{
			name:     "SET missing value",
			payload:  "*2\r\n$3\r\nSET\r\n$3\r\nfoo\r\n",
			expected: "-ERR wrong number of arguments for 'set' command\r\n",
		},
		{
			name:     "SET with invalid syntax option",
			payload:  "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nXX\r\n$2\r\n10\r\n",
			expected: "-ERR syntax error\r\n",
		},
		{
			name:     "SET with non-integer EX value",
			payload:  "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nEX\r\n$3\r\nabc\r\n",
			expected: "-ERR value is not an integer or out of range\r\n",
		},
		{
			name:     "EXPIRE with non-integer seconds",
			payload:  "*3\r\n$6\r\nEXPIRE\r\n$3\r\nfoo\r\n$3\r\nabc\r\n",
			expected: "-ERR value is not an integer or out of range\r\n",
		},
		{
			name:     "EXPIRE wrong argument count",
			payload:  "*2\r\n$6\r\nEXPIRE\r\n$3\r\nfoo\r\n",
			expected: "-ERR wrong number of arguments for 'expire' command\r\n",
		},
		{
			name:     "TTL wrong argument count",
			payload:  "*1\r\n$3\r\nTTL\r\n",
			expected: "-ERR wrong number of arguments for 'ttl' command\r\n",
		},
		// Unknown command
		{
			name:     "Unknown command",
			payload:  "*1\r\n$3\r\nFOO\r\n",
			expected: "-ERR unknown command 'FOO'\r\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wait > 0 {
				time.Sleep(tc.wait)
			}

			_, err := clientConn.Write([]byte(tc.payload))
			if err != nil {
				t.Fatalf("failed to write payload: %v", err)
			}

			var got []byte
			switch tc.expected[0] {
			case '+', '-', ':':
				line, err := reader.ReadBytes('\n')
				if err != nil {
					t.Fatalf("failed to read line: %v", err)
				}
				got = line
			case '$':
				header, err := reader.ReadBytes('\n')
				if err != nil {
					t.Fatalf("failed to read bulk header: %v", err)
				}
				if string(header) == "$-1\r\n" {
					got = header
				} else {
					remaining := len(tc.expected) - len(header)
					data := make([]byte, remaining)
					if _, err := io.ReadFull(reader, data); err != nil {
						t.Fatalf("failed to read bulk data: %v", err)
					}
					got = append(header, data...)
				}
			}

			if string(got) != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, string(got))
			}
		})
	}
}

func TestPipelinedBatchWrites(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	kvStore := store.New()
	go handleConnection(serverConn, kvStore, false)

	// Send 50 PINGs in a single pipelined write
	numCommands := 50
	var payload []byte
	for i := 0; i < numCommands; i++ {
		payload = append(payload, []byte("*1\r\n$4\r\nPING\r\n")...)
	}

	go func() {
		_, _ = clientConn.Write(payload)
	}()

	reader := bufio.NewReader(clientConn)
	for i := 0; i < numCommands; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("command %d failed to read reply: %v", i, err)
		}
		if line != "+PONG\r\n" {
			t.Fatalf("command %d expected +PONG\\r\\n, got %q", i, line)
		}
	}
}

func TestRapidConnectionChurn(t *testing.T) {
	kvStore := store.New()
	numConnections := 100

	startGoroutines := runtime.NumGoroutine()

	var wg sync.WaitGroup
	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sConn, cConn := net.Pipe()

			go handleConnection(sConn, kvStore, false)

			// Quickly send PING, read PONG, and close
			_, _ = cConn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
			reader := bufio.NewReader(cConn)
			_, _ = reader.ReadString('\n')
			_ = cConn.Close()
		}()
	}

	wg.Wait()

	// Allow goroutines to wind down
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		runtime.Gosched()
		if runtime.NumGoroutine() <= startGoroutines+2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	finalGoroutines := runtime.NumGoroutine()
	if finalGoroutines > startGoroutines+5 {
		t.Errorf("potential goroutine leak: started with %d, ended with %d", startGoroutines, finalGoroutines)
	}
}

func TestConcurrentClientsWithTTL(t *testing.T) {
	kvStore := store.New()
	stop := kvStore.StartCleaner(50 * time.Millisecond)
	defer stop()

	var wg sync.WaitGroup
	numClients := 15
	opsPerClient := 30

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientId int) {
			defer wg.Done()

			sConn, cConn := net.Pipe()
			defer cConn.Close()

			go handleConnection(sConn, kvStore, false)
			reader := bufio.NewReader(cConn)

			for i := 0; i < opsPerClient; i++ {
				key := fmt.Sprintf("c_%d_%d", clientId, i)
				val := fmt.Sprintf("val_%d", i)

				// SET with EX
				setCmd := fmt.Sprintf("*5\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$2\r\nEX\r\n$1\r\n5\r\n", len(key), key, len(val), val)
				if _, err := cConn.Write([]byte(setCmd)); err != nil {
					t.Errorf("client %d failed to write SET: %v", clientId, err)
					return
				}
				line, err := reader.ReadBytes('\n')
				if err != nil || string(line) != "+OK\r\n" {
					t.Errorf("client %d SET reply error: %s (err=%v)", clientId, line, err)
					return
				}

				// TTL
				ttlCmd := fmt.Sprintf("*2\r\n$3\r\nTTL\r\n$%d\r\n%s\r\n", len(key), key)
				if _, err := cConn.Write([]byte(ttlCmd)); err != nil {
					t.Errorf("client %d failed to write TTL: %v", clientId, err)
					return
				}
				ttlLine, err := reader.ReadBytes('\n')
				if err != nil || ttlLine[0] != ':' {
					t.Errorf("client %d TTL reply error: %s (err=%v)", clientId, ttlLine, err)
					return
				}

				// GET
				getCmd := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
				if _, err := cConn.Write([]byte(getCmd)); err != nil {
					t.Errorf("client %d failed to write GET: %v", clientId, err)
					return
				}
				h, err := reader.ReadBytes('\n')
				if err != nil {
					t.Errorf("client %d GET header error: %v", clientId, err)
					return
				}
				expectedH := fmt.Sprintf("$%d\r\n", len(val))
				if string(h) != expectedH {
					t.Errorf("client %d GET header expected %q, got %q", clientId, expectedH, h)
					return
				}
				body := make([]byte, len(val)+2)
				if _, err := io.ReadFull(reader, body); err != nil {
					t.Errorf("client %d GET body error: %v", clientId, err)
					return
				}
				if string(body) != val+"\r\n" {
					t.Errorf("client %d GET body expected %q, got %q", clientId, val+"\r\n", body)
					return
				}
			}
		}(c)
	}

	wg.Wait()
}
