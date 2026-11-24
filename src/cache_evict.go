package main

import (
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Cache eviction: least recently used first, deliberately lazy so that serving reads never churns the disk.
//
//   - A read does not touch the disk for bookkeeping except to bump the file's mtime, and only once per touch_interval per file.
//   - A background pass runs every scan_interval_seconds. It walks the cache once and does nothing unless the cache is over the cap.
//   - Over the cap it deletes the oldest files, in batches with a pause between them, down to evict_to*cap (hysteresis), so the
//     next passes find nothing to do until the cache has grown by the gap again. The cache may exceed the cap between passes.
//   - Never evicted: files used within min_age, and files under minEvictBytes (an empty table is a marker that the day was
//     extracted; deleting it would cost a whole pass over the pcap to save a few bytes).
const (
	minEvictBytes = 16 << 10
	evictBatch    = 50
)

type cacheFile struct {
	path  string
	size  int64
	mtime time.Time
}

// touch records a use of a cached file by bumping its mtime, at most once per touch interval.
func (store *Store) touch(path string) {
	if store.config.Cache.MaxGB <= 0 {
		return
	}
	now := time.Now()
	interval := time.Duration(store.config.Cache.TouchIntervalSeconds * float64(time.Second))
	store.touchMutex.Lock()
	last, seen := store.touched[path]
	if seen && now.Sub(last) < interval {
		store.touchMutex.Unlock()
		return
	}
	store.touched[path] = now
	store.touchMutex.Unlock()
	os.Chtimes(path, now, now) // best effort: a failure only makes the file look older
}

// scanCache lists the parquet and csv files of every kind directory (not _work, _logs, _symbols) and their total size.
func (store *Store) scanCache() ([]cacheFile, int64) {
	var files []cacheFile
	var total int64
	entries, err := os.ReadDir(store.cache)
	if err != nil {
		return nil, 0
	}
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), "_") {
			continue
		}
		filepath.WalkDir(filepath.Join(store.cache, entry.Name()), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !(strings.HasSuffix(path, ".parquet") || strings.HasSuffix(path, ".csv")) {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return nil
			}
			files = append(files, cacheFile{path, info.Size(), info.ModTime()})
			total += info.Size()
			return nil
		})
	}
	return files, total
}

// evictOnce is one background pass. It returns what it deleted and the cache size it found.
func (store *Store) evictOnce() (deletedFiles int, deletedBytes, found int64) {
	limit := int64(store.config.Cache.MaxGB * (1 << 30))
	files, total := store.scanCache()
	found = total
	if limit <= 0 || total <= limit {
		store.pruneTouched()
		return 0, 0, found
	}
	target := int64(float64(limit) * store.config.Cache.EvictTo)
	minAge := time.Duration(store.config.Cache.MinAgeSeconds * float64(time.Second))
	cutoff := time.Now().Add(-minAge)
	var candidates []cacheFile
	for _, file := range files {
		if file.mtime.Before(cutoff) && file.size >= minEvictBytes {
			candidates = append(candidates, file)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].mtime.Before(candidates[j].mtime) })
	for _, file := range candidates {
		if total <= target {
			break
		}
		if err := os.Remove(file.path); err != nil && !os.IsNotExist(err) {
			continue
		}
		total -= file.size
		deletedFiles++
		deletedBytes += file.size
		store.touchMutex.Lock()
		delete(store.touched, file.path)
		store.touchMutex.Unlock()
		if deletedFiles%evictBatch == 0 {
			time.Sleep(store.evictPause) // gradual: never a burst of deletions
		}
	}
	if total > target {
		log.Printf("cache: %.1f GiB is over the cap of %.1f GiB but the rest was used within %s or is too small to evict",
			float64(total)/(1<<30), store.config.Cache.MaxGB, minAge)
	}
	store.pruneTouched()
	return deletedFiles, deletedBytes, found
}

// pruneTouched forgets bump times older than the touch interval: they no longer throttle anything.
func (store *Store) pruneTouched() {
	interval := time.Duration(store.config.Cache.TouchIntervalSeconds * float64(time.Second))
	store.touchMutex.Lock()
	defer store.touchMutex.Unlock()
	for path, last := range store.touched {
		if time.Since(last) >= interval {
			delete(store.touched, path)
		}
	}
}

func (store *Store) evictLoop() {
	ticker := time.NewTicker(time.Duration(store.config.Cache.ScanIntervalSeconds * float64(time.Second)))
	defer ticker.Stop()
	for range ticker.C {
		started := time.Now()
		files, bytes, found := store.evictOnce()
		if files > 0 {
			log.Printf("cache: was %.1f GiB, evicted %d least recently used files (%.1f GiB) in %s",
				float64(found)/(1<<30), files, float64(bytes)/(1<<30), time.Since(started).Round(time.Second))
		}
	}
}
