package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func evictTestStore(t *testing.T, maxGB float64) *Store {
	t.Helper()
	config := &Config{CacheDir: t.TempDir(), Extract: ExtractConfig{BatchWindowSeconds: 3, MaxParallel: 1}, Orderbook: OrderbookConfig{Depth: 5}}
	config.Cache = CacheConfig{MaxGB: maxGB}
	if err := config.Cache.normalize(); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(config)
	if err != nil {
		t.Fatal(err)
	}
	store.evictPause = 0
	return store
}

func writeCached(t *testing.T, store *Store, symbol, day string, size int, age time.Duration) string {
	t.Helper()
	path := store.ParquetPath("", kindTick, symbol, day)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
	when := time.Now().Add(-age)
	os.Chtimes(path, when, when)
	return path
}

func exists(path string) bool { _, err := os.Stat(path); return err == nil }

func TestEvictOldestFirstDownToTarget(t *testing.T) {
	store := evictTestStore(t, 1e-3) // about 1.05 MB cap, evict to 0.9 of it
	hours := func(n int) time.Duration { return time.Duration(n) * time.Hour }
	oldest := writeCached(t, store, "AAA", "2024-01-02", 400_000, hours(30))
	older := writeCached(t, store, "BBB", "2024-01-02", 400_000, hours(20))
	newer := writeCached(t, store, "CCC", "2024-01-02", 400_000, hours(10))
	fresh := writeCached(t, store, "DDD", "2024-01-02", 400_000, 0)    // inside min_age: protected
	tiny := writeCached(t, store, "EEE", "2024-01-02", 100, hours(40)) // an empty-table marker: protected
	files, _, found := store.evictOnce()
	if found != 1_600_100 {
		t.Fatalf("found %d bytes", found)
	}
	// 1.6 MB over a ~1.05 MB cap with a ~0.94 MB target: the two oldest go, which brings it to 0.8 MB
	if files != 2 || exists(oldest) || exists(older) || !exists(newer) || !exists(fresh) || !exists(tiny) {
		t.Fatalf("evicted %d: oldest=%v older=%v newer=%v fresh=%v tiny=%v", files, exists(oldest), exists(older), exists(newer), exists(fresh), exists(tiny))
	}
}

func TestNoEvictionUnderCapOrWhenUnlimited(t *testing.T) {
	store := evictTestStore(t, 1) // 1 GiB
	path := writeCached(t, store, "AAA", "2024-01-02", 400_000, 48*time.Hour)
	if files, _, _ := store.evictOnce(); files != 0 || !exists(path) {
		t.Fatal("evicted under the cap")
	}
	store = evictTestStore(t, 0)
	path = writeCached(t, store, "AAA", "2024-01-02", 400_000, 48*time.Hour)
	if files, _, _ := store.evictOnce(); files != 0 || !exists(path) {
		t.Fatal("evicted with no cap")
	}
}

func TestTouchIsThrottled(t *testing.T) {
	store := evictTestStore(t, 1)
	path := writeCached(t, store, "AAA", "2024-01-02", 100, 48*time.Hour)
	store.touch(path)
	first, _ := os.Stat(path)
	if time.Since(first.ModTime()) > time.Minute {
		t.Fatal("first use did not bump the time")
	}
	old := time.Now().Add(-48 * time.Hour)
	os.Chtimes(path, old, old)
	store.touch(path) // inside the touch interval: no disk write
	second, _ := os.Stat(path)
	if time.Since(second.ModTime()) < 47*time.Hour {
		t.Fatal("second use inside the interval rewrote the mtime")
	}
}

func TestCacheConfigFromToml(t *testing.T) {
	file := filepath.Join(t.TempDir(), "config.toml")
	text := "[remote]\npcap_path = \"/x/{yyyy}\"\n[parser]\ncommand = \"true\"\n[cache]\ndir = \"/c\"\nmax_gb = 400\nevict_to = 0.8\n"
	os.WriteFile(file, []byte(text), 0o644)
	config, err := LoadConfig(file)
	if err != nil {
		t.Fatal(err)
	}
	if config.CacheDir != "/c" || config.Cache.MaxGB != 400 || config.Cache.EvictTo != 0.8 || config.Cache.MinAgeSeconds != 3600 || config.Cache.ScanIntervalSeconds != 600 {
		t.Fatalf("%+v", config.Cache)
	}
}
