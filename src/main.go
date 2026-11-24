package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const usage = `usage: datafeedfs-go <command>
  mount [--allow-other] CONFIG MOUNTPOINT   mount the filesystem (foreground until unmounted)
  serve CONFIG                              run only the web console (needs [ui] listen), without mounting
  prefetch [--kind K] CONFIG DAY SYMBOL...  extract symbols for a day into the cache without mounting; K is tick (default), orderbook or stream
  symbols CONFIG [DAY]                      print the symbols of a day (default: newest)
  days CONFIG                               print the trading days available on the ssh host
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
	case "days":
		store, err := openStoreArg(arguments, 1)
		if err != nil {
			return err
		}
		days, err := store.Days()
		for _, day := range days {
			fmt.Println(day)
		}
		return err
	case "symbols":
		store, err := openStoreArg(arguments, 1)
		if err != nil {
			return err
		}
		var symbols []string
		if len(arguments) > 1 {
			symbols, err = store.SymbolsForDay(arguments[1])
		} else {
			symbols, err = store.Symbols()
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
		flags.Parse(arguments)
		if flags.NArg() < 3 || !validKind(*kind) {
			return fmt.Errorf("prefetch [--kind tick|orderbook|stream] CONFIG DAY SYMBOL...")
		}
		store, err := openStore(flags.Arg(0))
		if err != nil {
			return err
		}
		var futures []*Future
		for _, symbol := range flags.Args()[2:] {
			futures = append(futures, store.Request(*kind, symbol, flags.Arg(1)))
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
