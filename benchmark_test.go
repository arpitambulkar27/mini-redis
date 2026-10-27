package main

import (
	"bufio"
	"fmt"
	"net"
	"testing"

	"mini-redis/store"
)

func BenchmarkServerPing(b *testing.B) {
	sConn, cConn := net.Pipe()
	defer cConn.Close()

	kvStore := store.New()
	go handleConnection(sConn, kvStore, false)

	payload := []byte("*1\r\n$4\r\nPING\r\n")
	reader := bufio.NewReader(cConn)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cConn.Write(payload); err != nil {
			b.Fatalf("write failed: %v", err)
		}
		if _, err := reader.ReadString('\n'); err != nil {
			b.Fatalf("read failed: %v", err)
		}
	}
}

func BenchmarkServerSetGet(b *testing.B) {
	sConn, cConn := net.Pipe()
	defer cConn.Close()

	kvStore := store.New()
	go handleConnection(sConn, kvStore, false)

	reader := bufio.NewReader(cConn)

	setPayload := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	getPayload := []byte("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// SET
		if _, err := cConn.Write(setPayload); err != nil {
			b.Fatalf("write SET failed: %v", err)
		}
		if _, err := reader.ReadString('\n'); err != nil {
			b.Fatalf("read SET failed: %v", err)
		}
		// GET
		if _, err := cConn.Write(getPayload); err != nil {
			b.Fatalf("write GET failed: %v", err)
		}
		// read $3\r\n
		if _, err := reader.ReadString('\n'); err != nil {
			b.Fatalf("read GET header failed: %v", err)
		}
		// read bar\r\n
		if _, err := reader.ReadString('\n'); err != nil {
			b.Fatalf("read GET data failed: %v", err)
		}
	}
}

func BenchmarkServerPipelined(b *testing.B) {
	sConn, cConn := net.Pipe()
	defer cConn.Close()

	kvStore := store.New()
	go handleConnection(sConn, kvStore, false)

	reader := bufio.NewReader(cConn)

	batchSize := 16
	var batch []byte
	for i := 0; i < batchSize; i++ {
		batch = append(batch, []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"))...)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cConn.Write(batch); err != nil {
			b.Fatalf("write batch failed: %v", err)
		}
		for j := 0; j < batchSize; j++ {
			if _, err := reader.ReadString('\n'); err != nil {
				b.Fatalf("read batch reply failed: %v", err)
			}
		}
	}
}
