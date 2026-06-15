package store

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const ShardCount = 32

type shard struct {
	mu sync.RWMutex
	db map[string]*Entry
}

type Store struct {
	shards      [ShardCount]*shard
	wal         *WAL
	lastVersion uint64
	versionMu   sync.Mutex
	reaperStop  chan struct{}
	wg          sync.WaitGroup
	keyCount    int64 // Atomic gauge for size
}

// NewStore initializes a sharded store. If wal is provided, it is used for persistence.
func NewStore(wal *WAL) *Store {
	s := &Store{
		wal:        wal,
		reaperStop: make(chan struct{}),
	}
	for i := 0; i < ShardCount; i++ {
		s.shards[i] = &shard{
			db: make(map[string]*Entry),
		}
	}
	return s
}

// StartReaper starts the background TTL reaper goroutine.
func (s *Store) StartReaper(interval time.Duration) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.reap()
			case <-s.reaperStop:
				return
			}
		}
	}()
}

// Close stops the reaper and cleans up.
func (s *Store) Close() error {
	close(s.reaperStop)
	s.wg.Wait()
	if s.wal != nil {
		return s.wal.Close()
	}
	return nil
}

// NextVersion generates a monotonic version number based on UnixNano time.
func (s *Store) NextVersion() uint64 {
	s.versionMu.Lock()
	defer s.versionMu.Unlock()
	v := uint64(time.Now().UnixNano())
	if v <= s.lastVersion {
		v = s.lastVersion + 1
	}
	s.lastVersion = v
	return v
}

// getShard returns the appropriate shard for the given key using FNV-1a.
func (s *Store) getShard(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	idx := h.Sum32() % ShardCount
	return s.shards[idx]
}

// Get retrieves an entry. If it is expired, it reaps it lazily and returns not found.
func (s *Store) Get(key string) ([]byte, uint64, bool) {
	sh := s.getShard(key)
	sh.mu.RLock()
	entry, exists := sh.db[key]
	sh.mu.RUnlock()

	if !exists {
		return nil, 0, false
	}

	if entry.IsExpired() {
		// Lazy reap
		sh.mu.Lock()
		// Double check under write lock
		if e, ok := sh.db[key]; ok && e.IsExpired() {
			delete(sh.db, key)
			atomic.AddInt64(&s.keyCount, -1)
		}
		sh.mu.Unlock()
		return nil, 0, false
	}

	return entry.Value, entry.Version, true
}

// Set sets a key-value pair. If local is true, it generates a new version.
// If it is a replication write, it uses the provided version.
func (s *Store) Set(key string, value []byte, ttl time.Duration, version uint64) (uint64, error) {
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	if version == 0 {
		version = s.NextVersion()
	}

	// Write to WAL first
	if s.wal != nil {
		if err := s.wal.Append(RecordTypeSet, key, value, version, expiresAt); err != nil {
			return 0, fmt.Errorf("wal append failed: %w", err)
		}
	}

	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	current, exists := sh.db[key]
	if exists {
		// Conflict resolution: LWW (incoming version must be >= current)
		if version < current.Version {
			return current.Version, nil
		}
	} else {
		atomic.AddInt64(&s.keyCount, 1)
	}

	sh.db[key] = &Entry{
		Value:     value,
		Version:   version,
		ExpiresAt: expiresAt,
	}

	return version, nil
}

// Delete deletes a key. If version is 0, it generates a new version.
func (s *Store) Delete(key string, version uint64) (uint64, bool, error) {
	if version == 0 {
		version = s.NextVersion()
	}

	// Write to WAL first
	if s.wal != nil {
		if err := s.wal.Append(RecordTypeDelete, key, nil, version, time.Time{}); err != nil {
			return 0, false, fmt.Errorf("wal append failed: %w", err)
		}
	}

	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	current, exists := sh.db[key]
	if !exists {
		return version, false, nil
	}

	// Conflict resolution: LWW
	if version < current.Version {
		return current.Version, false, nil
	}

	delete(sh.db, key)
	atomic.AddInt64(&s.keyCount, -1)
	return version, true, nil
}

// Scan returns a paginated list of keys matching a prefix.
// Returns list of keys, next cursor, and error.
func (s *Store) Scan(prefix string, limit int, cursor string) ([]string, string) {
	var matched []string
	now := time.Now()

	// Since we are sharded and need sorted/predictable listing, we gather all active keys.
	// For production-grade pagination under moderate size, we sort keys matching prefix.
	var allKeys []string
	for i := 0; i < ShardCount; i++ {
		sh := s.shards[i]
		sh.mu.RLock()
		for k, entry := range sh.db {
			if !entry.ExpiresAt.IsZero() && now.After(entry.ExpiresAt) {
				continue
			}
			if strings.HasPrefix(k, prefix) {
				allKeys = append(allKeys, k)
			}
		}
		sh.mu.RUnlock()
	}

	// Sort keys to provide deterministic cursor-based pagination
	// Simple bubble or quicksort. Go standard library sort is great.
	sortStrings(allKeys)

	// Apply cursor
	startIndex := 0
	if cursor != "" {
		for i, k := range allKeys {
			if k > cursor {
				startIndex = i
				break
			}
			if i == len(allKeys)-1 {
				startIndex = len(allKeys) // cursor is past all keys
			}
		}
	}

	endIndex := startIndex + limit
	if endIndex > len(allKeys) {
		endIndex = len(allKeys)
	}

	if startIndex >= len(allKeys) {
		return nil, ""
	}

	matched = allKeys[startIndex:endIndex]
	nextCursor := ""
	if endIndex < len(allKeys) {
		nextCursor = matched[len(matched)-1]
	}

	return matched, nextCursor
}

// Size returns the count of keys in the store.
func (s *Store) Size() int64 {
	return atomic.LoadInt64(&s.keyCount)
}

// reap clears out expired keys.
func (s *Store) reap() {
	now := time.Now()
	for i := 0; i < ShardCount; i++ {
		sh := s.shards[i]
		sh.mu.Lock()
		for k, entry := range sh.db {
			if !entry.ExpiresAt.IsZero() && now.After(entry.ExpiresAt) {
				delete(sh.db, k)
				atomic.AddInt64(&s.keyCount, -1)
			}
		}
		sh.mu.Unlock()
	}
}

// SetWAL associates a WAL with the store for logging updates.
func (s *Store) SetWAL(wal *WAL) {
	s.wal = wal
}

// Simple in-place string sorting to keep zero dependencies
func sortStrings(arr []string) {
	if len(arr) < 2 {
		return
	}
	quickSort(arr, 0, len(arr)-1)
}

func quickSort(arr []string, low, high int) {
	if low < high {
		p := partition(arr, low, high)
		quickSort(arr, low, p-1)
		quickSort(arr, p+1, high)
	}
}

func partition(arr []string, low, high int) int {
	pivot := arr[high]
	i := low - 1
	for j := low; j < high; j++ {
		if arr[j] < pivot {
			i++
			arr[i], arr[j] = arr[j], arr[i]
		}
	}
	arr[i+1], arr[high] = arr[high], arr[i+1]
	return i + 1
}
