package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestStoreBasicOperations(t *testing.T) {
	s := New()

	// Get non-existent key
	_, ok := s.Get("missing")
	if ok {
		t.Fatalf("expected missing key to return false")
	}

	// Set and Get
	s.Set("name", "antigravity")
	val, ok := s.Get("name")
	if !ok || val != "antigravity" {
		t.Fatalf("expected 'antigravity', got %q (ok=%v)", val, ok)
	}

	// Overwrite
	s.Set("name", "redis")
	val, ok = s.Get("name")
	if !ok || val != "redis" {
		t.Fatalf("expected 'redis', got %q (ok=%v)", val, ok)
	}

	// Del single key
	deleted := s.Del("name")
	if deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", deleted)
	}
	_, ok = s.Get("name")
	if ok {
		t.Fatalf("expected deleted key to be absent")
	}

	// Del non-existent key
	deleted = s.Del("name")
	if deleted != 0 {
		t.Fatalf("expected 0 deleted, got %d", deleted)
	}

	// Del multiple keys
	s.Set("k1", "v1")
	s.Set("k2", "v2")
	deleted = s.Del("k1", "k2", "k3")
	if deleted != 2 {
		t.Fatalf("expected 2 deleted, got %d", deleted)
	}
}

func TestStoreTTLAndExpiration(t *testing.T) {
	s := New()

	// TTL of non-existent key should be -2
	if ttl := s.TTL("none"); ttl != -2 {
		t.Fatalf("expected TTL -2 for non-existent key, got %d", ttl)
	}

	// Key with no expiry should have TTL -1
	s.Set("persist", "val")
	if ttl := s.TTL("persist"); ttl != -1 {
		t.Fatalf("expected TTL -1 for persistent key, got %d", ttl)
	}

	// SetWithTTL
	s.SetWithTTL("temp", "val", 2*time.Second)
	ttl := s.TTL("temp")
	if ttl <= 0 || ttl > 2 {
		t.Fatalf("expected TTL between 1 and 2, got %d", ttl)
	}

	// Expire command
	if ok := s.Expire("persist", 3); !ok {
		t.Fatalf("expected Expire to succeed on existing key")
	}
	if ttl := s.TTL("persist"); ttl <= 0 || ttl > 3 {
		t.Fatalf("expected TTL between 1 and 3, got %d", ttl)
	}

	// Expire on non-existent key
	if ok := s.Expire("ghost", 5); ok {
		t.Fatalf("expected Expire to return false for non-existent key")
	}

	// Expire with seconds <= 0 immediately deletes
	s.Set("ephemeral", "val")
	if ok := s.Expire("ephemeral", 0); !ok {
		t.Fatalf("expected Expire(0) to return true for existing key")
	}
	if _, ok := s.Get("ephemeral"); ok {
		t.Fatalf("expected key to be deleted after Expire(0)")
	}

	// Passive (Lazy) expiration
	s.SetWithTTL("quick", "val", 50*time.Millisecond)
	time.Sleep(70 * time.Millisecond)
	// GET should detect expiration, delete key, and return false
	if _, ok := s.Get("quick"); ok {
		t.Fatalf("expected key to be expired and removed by Get")
	}
	if ttl := s.TTL("quick"); ttl != -2 {
		t.Fatalf("expected TTL -2 after expiration, got %d", ttl)
	}

	// Overwriting a key with Set clears the expiration
	s.SetWithTTL("cleared", "val", 10*time.Second)
	s.Set("cleared", "new_val")
	if ttl := s.TTL("cleared"); ttl != -1 {
		t.Fatalf("expected TTL -1 after Set overwrite, got %d", ttl)
	}
}

func TestStoreActiveCleaner(t *testing.T) {
	s := New()
	stop := s.StartCleaner(50 * time.Millisecond)
	defer stop()

	s.SetWithTTL("auto_exp1", "val1", 40*time.Millisecond)
	s.SetWithTTL("auto_exp2", "val2", 40*time.Millisecond)
	s.Set("auto_persist", "val3")

	time.Sleep(120 * time.Millisecond)

	s.mu.RLock()
	_, exp1 := s.data["auto_exp1"]
	_, exp2 := s.data["auto_exp2"]
	_, persist := s.data["auto_persist"]
	s.mu.RUnlock()

	if exp1 || exp2 {
		t.Fatalf("expected background cleaner to remove expired keys from map directly")
	}
	if !persist {
		t.Fatalf("expected persistent key to remain")
	}
}

func TestStoreConcurrency(t *testing.T) {
	s := New()
	stop := s.StartCleaner(20 * time.Millisecond)
	defer stop()

	var wg sync.WaitGroup
	numGoroutines := 30
	ops := 50

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k_%d_%d", gid, i)
				s.SetWithTTL(key, "val", 100*time.Millisecond)
				s.Get(key)
				s.TTL(key)
				s.Expire(key, 2)
				if i%2 == 0 {
					s.Del(key)
				}
			}
		}(g)
	}

	wg.Wait()
}
