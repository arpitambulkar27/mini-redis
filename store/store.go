package store

import (
	"math"
	"sync"
	"time"
)

type item struct {
	value   string
	expires time.Time // IsZero() means no expiration deadline
}

// Store provides a thread-safe in-memory key-value store with TTL support.
type Store struct {
	mu   sync.RWMutex
	data map[string]item
}

// New creates and initializes a new thread-safe Store.
func New() *Store {
	return &Store{
		data: make(map[string]item),
	}
}

// Set stores a key-value pair without expiration (or clears any previous expiration).
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = item{
		value:   value,
		expires: time.Time{},
	}
}

// SetWithTTL stores a key-value pair with a time-to-live duration.
func (s *Store) SetWithTTL(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = item{
		value:   value,
		expires: time.Now().Add(ttl),
	}
}

// Get retrieves the value associated with key.
// Performs passive (lazy) expiration: if the key has expired, it is deleted and returns ("", false).
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	it, ok := s.data[key]
	if !ok {
		s.mu.RUnlock()
		return "", false
	}

	if !it.expires.IsZero() && time.Now().After(it.expires) {
		s.mu.RUnlock()
		// Lazy delete under exclusive write lock
		s.mu.Lock()
		if current, exists := s.data[key]; exists && !current.expires.IsZero() && time.Now().After(current.expires) {
			delete(s.data, key)
		}
		s.mu.Unlock()
		return "", false
	}

	val := it.value
	s.mu.RUnlock()
	return val, true
}

// Del removes one or more keys from the store.
// Returns the count of active (non-expired) keys removed.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	deleted := 0
	for _, key := range keys {
		it, ok := s.data[key]
		if ok {
			delete(s.data, key)
			if it.expires.IsZero() || now.Before(it.expires) {
				deleted++
			}
		}
	}
	return deleted
}

// Expire sets a timeout on the key in seconds.
// Returns true if the timeout was set, or false if the key does not exist / has expired.
// If seconds <= 0, the key is immediately removed and returns true (if it existed).
func (s *Store) Expire(key string, seconds int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	it, ok := s.data[key]
	if !ok {
		return false
	}

	now := time.Now()
	if !it.expires.IsZero() && now.After(it.expires) {
		delete(s.data, key)
		return false
	}

	if seconds <= 0 {
		delete(s.data, key)
		return true
	}

	it.expires = now.Add(time.Duration(seconds) * time.Second)
	s.data[key] = it
	return true
}

// TTL returns the remaining time-to-live of a key in seconds.
// Returns -2 if the key does not exist or has expired.
// Returns -1 if the key exists but has no expiration deadline.
func (s *Store) TTL(key string) int {
	s.mu.RLock()
	it, ok := s.data[key]
	if !ok {
		s.mu.RUnlock()
		return -2
	}

	if it.expires.IsZero() {
		s.mu.RUnlock()
		return -1
	}

	now := time.Now()
	if now.After(it.expires) {
		s.mu.RUnlock()
		// Lazy delete under exclusive write lock
		s.mu.Lock()
		if current, exists := s.data[key]; exists && !current.expires.IsZero() && time.Now().After(current.expires) {
			delete(s.data, key)
		}
		s.mu.Unlock()
		return -2
	}

	remaining := int(math.Ceil(time.Until(it.expires).Seconds()))
	s.mu.RUnlock()
	if remaining <= 0 {
		return -2
	}
	return remaining
}

// CleanExpired scans the store and deletes all expired keys (Active Expiration).
// Returns the count of keys deleted.
func (s *Store) CleanExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	deleted := 0
	for key, it := range s.data {
		if !it.expires.IsZero() && now.After(it.expires) {
			delete(s.data, key)
			deleted++
		}
	}
	return deleted
}

// StartCleaner launches a background goroutine that scans and purges expired keys periodically.
// It returns a cleanup function to stop the background goroutine.
func (s *Store) StartCleaner(interval time.Duration) func() {
	ticker := time.NewTicker(interval)
	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				s.CleanExpired()
			case <-stop:
				ticker.Stop()
				return
			}
		}
	}()

	return func() {
		close(stop)
	}
}
