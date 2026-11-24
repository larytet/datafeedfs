package main

import (
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

// What the web console and the telemetry gauges read: cache sizes from the last scan, pass records and counters.

type kindStat struct {
	Bytes int64
	Files int
}

// CacheStats is the result of one walk over the cache directories.
type CacheStats struct {
	At     time.Time
	Bytes  int64
	Files  int
	ByKind map[string]kindStat
}

type passRecord struct {
	Kind     string
	Day      string
	Symbols  int
	Seconds  float64
	Status   string
	Finished time.Time
}

type runInfo struct {
	kind, day string
	symbols   int
	started   time.Time
}

const historyLimit = 100

// counters are monotonic; the sampler turns them into rates.
type counters struct {
	hits                       atomic.Int64 // requests served from the cache
	misses                     atomic.Int64 // requests that needed an extraction (including joined ones)
	written                    atomic.Int64 // bytes of parquet moved into the cache by passes
	evictedFiles, evictedBytes atomic.Int64
}

// kindOfPath is the kind directory (tick, orderbook-5, stream) of a file in the cache.
func (store *Store) kindOfPath(path string) string {
	relative, err := filepath.Rel(store.cache, path)
	if err != nil {
		return ""
	}
	return strings.SplitN(filepath.ToSlash(relative), "/", 2)[0]
}

func (store *Store) recordPass(record passRecord) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.history = append(store.history, record)
	if len(store.history) > historyLimit {
		store.history = store.history[len(store.history)-historyLimit:]
	}
}
