package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFeedConfig(t *testing.T) {
	base := "[remote]\nssh_host = \"h\"\npcap_path = \"/p/{yyyy}/{mm}/{dd}/{feed}.pcap.xz\"\n[parser]\ncommand = \"parse.sh\"\nenv = { A = \"1\" }\n"
	config, err := ParseConfig([]byte(base+"[feeds.edgx]\n[feeds.cboe_edga]\npcap_path = \"/q/{yyyy}-{mm}-{dd}.pcap\"\nssh_host = \"\"\n[feeds.cboe_edga.parser]\ncommand = [\"other.sh\", \"-x\"]\nenv = { B = \"2\" }\n"), "c.toml")
	if err != nil {
		t.Fatal(err)
	}
	if !config.Named() || len(config.Feeds) != 2 || config.Feeds[0].Name != "cboe_edga" || config.Feeds[1].Name != "edgx" {
		t.Fatalf("feeds: %+v", config.Feeds)
	}
	edga, edgx := config.Feed("cboe_edga"), config.Feed("edgx")
	if got := edgx.PcapFor("2026-11-24"); got != "/p/2026/11/24/edgx.pcap.xz" || edgx.Remote.SSHHost != "h" || edgx.Parser.Command[0] != "parse.sh" || edgx.Parser.Env["A"] != "1" {
		t.Fatalf("inherited feed: %s %+v %+v", got, edgx.Remote, edgx.Parser)
	}
	if got := edga.PcapFor("2026-11-24"); got != "/q/2026-11-24.pcap" || edga.Remote.SSHHost != "" || strings.Join(edga.Parser.Command, " ") != "other.sh -x" || edga.Parser.Env["A"] != "1" || edga.Parser.Env["B"] != "2" {
		t.Fatalf("overridden feed: %s %+v %+v", got, edga.Remote, edga.Parser)
	}
	if match := edgx.DayRegex().FindStringSubmatch("/p/2026/11/24/edgx.pcap.xz"); match == nil || edgx.DayRegex().MatchString("/p/2026/11/24/edga.pcap.xz") {
		t.Fatal("day regex must match this feed's file only")
	}
	if legacy, err := ParseConfig([]byte(base), "c.toml"); err != nil || legacy.Named() || legacy.Feed("") == nil {
		t.Fatalf("without feeds the layout is the legacy one: %v", err)
	}
	for text, want := range map[string]string{
		"[feeds.Bad]\n":         "lowercase",
		"[feeds.tick]\n":        "kind name",
		"[feeds._x]\n":          "lowercase",
		"[remote]\n[feeds.a]\n": "pcap_path is required",
		"[remote]\npcap_path=\"/p\"\n[feeds.a]\n": "parser.command is required",
	} {
		if _, err := ParseConfig([]byte(text), "c.toml"); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: got %v, want %q", text, err, want)
		}
	}
}

// Two feeds with different pcap files on a local "host": days, symbols and cache paths must not mix, and the parser must see DFS_FEED.
func TestFeedsAreIsolated(t *testing.T) {
	dir := t.TempDir()
	for feed, day := range map[string]string{"edga": "2026-11-23", "edgx": "2026-11-24"} {
		parts := strings.Split(day, "-")
		os.MkdirAll(filepath.Join(dir, "pcaps", parts[0], parts[1], parts[2]), 0o755)
		os.WriteFile(filepath.Join(dir, "pcaps", parts[0], parts[1], parts[2], feed+".pcap.xz"), nil, 0o644)
	}
	parser := filepath.Join(dir, "parse.sh")
	os.WriteFile(parser, []byte("#!/bin/sh\n[ \"$DFS_MODE\" = list ] && echo SYM_$DFS_FEED | tr a-z A-Z | tr _ X\nexit 0\n"), 0o755)
	text := fmt.Sprintf("[remote]\npcap_path = %q\n[parser]\ncommand = %q\n[cache]\ndir = %q\n[extract]\nbatch_window_seconds = 0.05\n[feeds.edga]\n[feeds.edgx]\n",
		filepath.Join(dir, "pcaps", "{yyyy}", "{mm}", "{dd}", "{feed}.pcap.xz"), parser, filepath.Join(dir, "cache"))
	config, err := ParseConfig([]byte(text), filepath.Join(dir, "c.toml"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(config)
	if err != nil {
		t.Fatal(err)
	}
	for feed, want := range map[string]string{"edga": "2026-11-23", "edgx": "2026-11-24"} {
		if days, err := store.Days(feed); err != nil || len(days) != 1 || days[0] != want {
			t.Fatalf("%s days: %v %v", feed, days, err)
		}
		wantSymbol := "SYMX" + strings.ToUpper(feed)
		if symbols, err := store.Symbols(feed); err != nil || len(symbols) != 1 || symbols[0] != wantSymbol {
			t.Fatalf("%s symbols: %v %v", feed, symbols, err)
		}
		if ok, _ := store.HasSymbol(feed, wantSymbol); !ok {
			t.Fatalf("%s should have %s", feed, wantSymbol)
		}
		path, err := store.Request(feed, kindTick, wantSymbol, want).Wait()
		if err != nil || path != filepath.Join(dir, "cache", feed, "tick", wantSymbol, want+".parquet") {
			t.Fatalf("%s path %q err %v", feed, path, err)
		}
	}
	if ok, _ := store.HasSymbol("edga", "SYMXEDGX"); ok {
		t.Fatal("symbols leaked between feeds")
	}
	if _, err := store.Days("nope"); err == nil {
		t.Fatal("unknown feed must fail")
	}
	store.scanCache()
	stats := store.cacheStats.Load()
	if _, ok := stats.ByKind["edga/tick"]; !ok || len(stats.ByKind) != 2 {
		t.Fatalf("cache stats by feed/kind: %+v", stats.ByKind)
	}
}
