package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const usage = `usage: datafeedfs-go <command>
  mount [--allow-other] CONFIG MOUNTPOINT   mount the filesystem (foreground until unmounted)
  serve CONFIG                              run only the web console (needs [ui] listen), without mounting
  prefetch [--feed F] [--kind K] CONFIG DAY SYMBOL...
                                            extract symbols for a day into the cache without mounting; K is tick (default), orderbook or stream
  symbols [--feed F] CONFIG [DAY]           print the symbols of a day (default: newest)
  days [--feed F] CONFIG                    print the trading days available on the ssh host
  feeds CONFIG                              print the configured feeds and the pcap path of each
  F is a [feeds.F] name; it is required when the configuration has feeds
  ingest [--kind K] [--depth N] DAY SYMBOLS_FILE OUT_DIR
                                            parser helper: itch_tob records on stdin -> OUT_DIR/<SYMBOL>/<DAY>.parquet
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	command, arguments := os.Args[1], os.Args[2:]
	if err := run(command, arguments); err != nil {
		log.Fatal(err)
	}
}

func openStore(path string) (*Store, error) {
	config, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return NewStore(config)
}

func run(command string, arguments []string) error {
	switch command {
	case "ingest":
		flags := flag.NewFlagSet("ingest", flag.ExitOnError)
		kind := flags.String("kind", kindTick, "tick, orderbook or stream")
		depth := flags.Int("depth", defaultDepth, "orderbook: price levels per side")
		flags.Parse(arguments)
		if flags.NArg() != 3 {
			return fmt.Errorf("ingest [--kind K] [--depth N] DAY SYMBOLS_FILE OUT_DIR")
		}
		_, _, err := Ingest(flags.Arg(0), flags.Arg(1), flags.Arg(2), *kind, *depth, os.Stdin)
		return err
	case "feeds":
		store, err := openStoreArg(arguments, 1)
		if err != nil {
			return err
		}
		for _, feed := range store.config.Feeds {
			fmt.Printf("%s\t%s\t%s\n", feed.Name, feed.Remote.SSHHost, feed.pattern())
		}
		return nil
	case "days":
		store, feed, rest, err := openStoreFeed("days", arguments)
		if err != nil {
			return err
		}
		_ = rest
		days, err := store.Days(feed)
		for _, day := range days {
			fmt.Println(day)
		}
		return err
	case "symbols":
		store, feed, rest, err := openStoreFeed("symbols", arguments)
		if err != nil {
			return err
		}
		var symbols []string
		if len(rest) > 0 {
			symbols, err = store.SymbolsForDay(feed, rest[0])
		} else {
			symbols, err = store.Symbols(feed)
		}
		for _, symbol := range symbols {
			fmt.Println(symbol)
		}
		return err
	case "serve":
		store, err := openStoreArg(arguments, 1)
		if err != nil {
			return err
		}
		handle, err := store.StartUI()
		if err != nil {
			return err
		}
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		<-signals
		handle.Close()
		store.tel.Shutdown(5 * time.Second)
		return nil
	case "prefetch":
		flags := flag.NewFlagSet("prefetch", flag.ExitOnError)
		kind := flags.String("kind", kindTick, "tick, orderbook or stream")
		feed := flags.String("feed", "", "feed name ([feeds.NAME]); required when the configuration has feeds")
		flags.Parse(arguments)
		if flags.NArg() < 3 || !validKind(*kind) {
			return fmt.Errorf("prefetch [--kind tick|orderbook|stream] CONFIG DAY SYMBOL...")
		}
		store, err := openStore(flags.Arg(0))
		if err != nil {
			return err
		}
		if err := checkFeed(store, *feed); err != nil {
			return err
		}
		var futures []*Future
		for _, symbol := range flags.Args()[2:] {
			futures = append(futures, store.Request(*feed, *kind, symbol, flags.Arg(1)))
		}
		defer store.tel.Shutdown(5 * time.Second)
		for _, future := range futures {
			path, err := future.Wait()
			if err != nil {
				return err
			}
			fmt.Println(path)
		}
		return nil
	case "mount":
		flags := flag.NewFlagSet("mount", flag.ExitOnError)
		allowOther := flags.Bool("allow-other", false, "let other users (and docker's bind propagation) see the mount")
		flags.Parse(arguments)
		if flags.NArg() != 2 {
			return fmt.Errorf("mount [--allow-other] CONFIG MOUNTPOINT")
		}
		store, err := openStore(flags.Arg(0))
		if err != nil {
			return err
		}
		return mount(store, flags.Arg(1), *allowOther)
	}
	fmt.Fprint(os.Stderr, usage)
	return fmt.Errorf("unknown command %q", command)
}

// openStoreFeed parses [--feed F] CONFIG [rest...] and opens the store.
func openStoreFeed(name string, arguments []string) (*Store, string, []string, error) {
	flags := flag.NewFlagSet(name, flag.ExitOnError)
	feed := flags.String("feed", "", "feed name ([feeds.NAME]); required when the configuration has feeds")
	flags.Parse(arguments)
	if flags.NArg() < 1 {
		return nil, "", nil, fmt.Errorf("missing CONFIG")
	}
	store, err := openStore(flags.Arg(0))
	if err != nil {
		return nil, "", nil, err
	}
	return store, *feed, flags.Args()[1:], checkFeed(store, *feed)
}

// checkFeed rejects a missing or unknown --feed, naming the configured feeds.
func checkFeed(store *Store, feed string) error {
	if store.feeds[feed] != nil {
		return nil
	}
	var names []string
	for _, configured := range store.config.Feeds {
		names = append(names, configured.Name)
	}
	if feed == "" {
		return fmt.Errorf("--feed is required, one of: %s", strings.Join(names, ", "))
	}
	return fmt.Errorf("unknown feed %q, one of: %s", feed, strings.Join(names, ", "))
}

func openStoreArg(arguments []string, minimum int) (*Store, error) {
	if len(arguments) < minimum {
		return nil, fmt.Errorf("missing CONFIG")
	}
	return openStore(arguments[0])
}

func mount(store *Store, mountpoint string, allowOther bool) error {
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return err
	}
	store.mountpoint = mountpoint
	var console *UIHandle
	if store.config.UI.Listen != "" {
		var err error
		if console, err = store.StartUI(); err != nil {
			return err
		}
		defer console.Close()
	}
	defer store.tel.Shutdown(5 * time.Second)
	zero := new(time.Duration) // attribute and entry timeouts of 0: sizes are never served stale from the kernel cache
	server, err := fs.Mount(mountpoint, NewRoot(store), &fs.Options{
		AttrTimeout: zero, EntryTimeout: zero,
		MountOptions: fuse.MountOptions{FsName: "datafeedfs", Name: "datafeedfs", AllowOther: allowOther, Options: []string{"ro"}},
	})
	if err != nil {
		return err
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		log.Print("unmounting")
		_ = server.Unmount()
	}()
	log.Printf("mounted at %s", mountpoint)
	server.Wait()
	return nil
}
