package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

var symbolPattern = regexp.MustCompile(`^[A-Z0-9.]{1,8}$`)

// Future resolves to the cached parquet path once a (symbol, day) is extracted.
type Future struct {
	done chan struct{}
	path string
	err  error
}

func newFuture() *Future { return &Future{done: make(chan struct{})} }

func (future *Future) resolve(path string, err error) {
	future.path, future.err = path, err
	close(future.done)
}

func (future *Future) Done() bool {
	select {
	case <-future.done:
		return true
	default:
		return false
	}
}

func (future *Future) Wait() (string, error) {
	<-future.done
	return future.path, future.err
}

// passKey identifies one pass over a day's pcap: the files of one kind for that day.
type passKey struct{ kind, day string }

func (key passKey) String() string { return key.kind + " " + key.day }

type fileKey struct{ kind, day, symbol string }

// Store owns remote access, extraction scheduling and the on-disk cache:
// <cache>/<kind>/<SYMBOL>/<YYYY-MM-DD>.parquet (and .csv on demand), <cache>/_symbols/<day>.txt, <cache>/_logs/. The kind directory
// is tick, orderbook-<depth> (a different depth is a different cache) or stream.
// Requests for one day and kind that arrive within the batch window are served by a single pass over that day's pcap.
type Store struct {
	config *Config
	cache  string

	mutex        sync.Mutex
	pending      map[passKey]map[string]*Future // pass -> symbol -> future waiting for the next pass
	firstRequest map[passKey]time.Time
	inflight     map[fileKey]*Future
	runningDays  map[passKey]bool
	failures     map[string]string
	passCounter  int
	wake         chan struct{}

	lookupMutex sync.Mutex
	days        []string
	daysLoaded  time.Time
	symbols     []string
	symbolSet   map[string]bool
	csvMutex    sync.Mutex
}

func NewStore(config *Config) (*Store, error) {
	store := &Store{
		config: config, cache: config.CacheDir,
		pending: map[passKey]map[string]*Future{}, firstRequest: map[passKey]time.Time{}, inflight: map[fileKey]*Future{},
		runningDays: map[passKey]bool{}, failures: map[string]string{}, wake: make(chan struct{}, 1),
	}
	for _, name := range []string{"_symbols", "_logs", "_work"} {
		if err := os.MkdirAll(filepath.Join(store.cache, name), 0o755); err != nil {
			return nil, err
		}
	}
	if err := migrateLegacyCache(store.cache); err != nil {
		return nil, err
	}
	go store.scheduler()
	return store, nil
}

// migrateLegacyCache moves <cache>/<SYMBOL> of the layout before the kinds into <cache>/tick/<SYMBOL>.
func migrateLegacyCache(cache string) error {
	entries, err := os.ReadDir(cache)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !symbolPattern.MatchString(entry.Name()) {
			continue
		}
		if err := os.MkdirAll(filepath.Join(cache, kindTick), 0o755); err != nil {
			return err
		}
		if err := os.Rename(filepath.Join(cache, entry.Name()), filepath.Join(cache, kindTick, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// ----- remote access ---------------------------------------------------------------------------------------------------------

func (store *Store) remoteCommand(command string) *exec.Cmd {
	if store.config.Remote.SSHHost == "" {
		return exec.Command("sh", "-c", command)
	}
	arguments := append(append([]string{}, store.config.Remote.SSHOptions...), store.config.Remote.SSHHost, command)
	return exec.Command("ssh", arguments...)
}

// Days returns the trading days that have a pcap on the host, ascending. Cached for days_ttl_seconds.
func (store *Store) Days() ([]string, error) {
	store.lookupMutex.Lock()
	defer store.lookupMutex.Unlock()
	if len(store.days) > 0 && time.Since(store.daysLoaded).Seconds() < store.config.DaysTTLSeconds {
		return store.days, nil
	}
	command := store.remoteCommand("ls -1 " + store.config.DayGlob() + " 2>/dev/null")
	output, _ := command.Output() // ls exits non-zero when a glob matches nothing; an empty result is handled below
	regex := store.config.DayRegex()
	found := map[string]bool{}
	for _, line := range strings.Split(string(output), "\n") {
		if match := regex.FindStringSubmatch(strings.TrimSpace(line)); match != nil {
			found[match[regex.SubexpIndex("yyyy")]+"-"+match[regex.SubexpIndex("mm")]+"-"+match[regex.SubexpIndex("dd")]] = true
		}
	}
	days := make([]string, 0, len(found))
	for day := range found {
		days = append(days, day)
	}
	sort.Strings(days)
	if len(days) > 0 || len(store.days) == 0 {
		store.days, store.daysLoaded = days, time.Now()
	}
	return store.days, nil
}

// Symbols returns the symbols of the newest day, computed once by running the parser in list mode.
func (store *Store) Symbols() ([]string, error) {
	days, err := store.Days()
	if err != nil || len(days) == 0 {
		return nil, err
	}
	store.lookupMutex.Lock()
	if store.symbols != nil {
		defer store.lookupMutex.Unlock()
		return store.symbols, nil
	}
	store.lookupMutex.Unlock()
	names, err := store.SymbolsForDay(days[len(days)-1])
	if err != nil {
		return nil, err
	}
	store.lookupMutex.Lock()
	defer store.lookupMutex.Unlock()
	store.symbols = names
	store.symbolSet = make(map[string]bool, len(names))
	for _, name := range names {
		store.symbolSet[name] = true
	}
	return names, nil
}

func (store *Store) HasSymbol(symbol string) (bool, error) {
	if _, err := store.Symbols(); err != nil {
		return false, err
	}
	store.lookupMutex.Lock()
	defer store.lookupMutex.Unlock()
	return store.symbolSet[symbol], nil
}

func (store *Store) SymbolsForDay(day string) ([]string, error) {
	cached := filepath.Join(store.cache, "_symbols", day+".txt")
	if _, err := os.Stat(cached); err != nil {
		output, err := store.runPipeline(day, "list", nil, true)
		if err != nil {
			return nil, err
		}
		seen := map[string]bool{}
		var names []string
		for _, line := range strings.Split(string(output), "\n") {
			line = strings.TrimSpace(line)
			if symbolPattern.MatchString(line) && !seen[line] {
				seen[line] = true
				names = append(names, line)
			}
		}
		if len(names) == 0 {
			return nil, fmt.Errorf("parser listed no symbols for %s", day)
		}
		sort.Strings(names)
		temporary := cached + ".tmp"
		if err := os.WriteFile(temporary, []byte(strings.Join(names, "\n")+"\n"), 0o644); err != nil {
			return nil, err
		}
		if err := os.Rename(temporary, cached); err != nil {
			return nil, err
		}
	}
	data, err := os.ReadFile(cached)
	if err != nil {
		return nil, err
	}
	return strings.Fields(string(data)), nil
}

// runPipeline runs "ssh cat <pcap> | parser" and returns the parser's stdout when capture is set.
func (store *Store) runPipeline(day, mode string, extraEnv map[string]string, capture bool) ([]byte, error) {
	pcap := store.config.PcapFor(day)
	store.mutex.Lock()
	store.passCounter++
	counter := store.passCounter
	store.mutex.Unlock()
	logPath := filepath.Join(store.cache, "_logs", fmt.Sprintf("%s-%s-%d.log", day, mode, counter))
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	defer logFile.Close()

	source := store.remoteCommand("cat " + shellQuote(pcap))
	source.Stderr = logFile
	parser := exec.Command(store.config.Parser.Command[0], store.config.Parser.Command[1:]...)
	parser.Stderr = logFile
	executable, _ := os.Executable()
	environment := append(os.Environ(), "DFS_INGEST="+executable+" ingest")
	for key, value := range store.config.Parser.Env {
		environment = append(environment, key+"="+value)
	}
	environment = append(environment, "DFS_MODE="+mode, "DFS_DATE="+day, "DFS_PCAP_NAME="+filepath.Base(pcap))
	for key, value := range extraEnv {
		environment = append(environment, key+"="+value)
	}
	parser.Env = environment
	var captured bytes.Buffer
	if capture {
		parser.Stdout = &captured
	}
	pipeReader, pipeWriter, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	source.Stdout, parser.Stdin = pipeWriter, pipeReader
	if err := source.Start(); err != nil {
		pipeReader.Close()
		pipeWriter.Close()
		return nil, err
	}
	pipeWriter.Close() // the parser owns the read end; when it exits the source gets SIGPIPE
	if err := parser.Start(); err != nil {
		pipeReader.Close()
		_ = source.Process.Kill()
		_ = source.Wait()
		return nil, err
	}
	pipeReader.Close()
	parserErr := parser.Wait()
	sourceDone := make(chan error, 1)
	go func() { sourceDone <- source.Wait() }()
	select {
	case <-sourceDone:
	case <-time.After(15 * time.Second):
		_ = source.Process.Kill()
		<-sourceDone
	}
	if parserErr != nil {
		tail, _ := os.ReadFile(logPath)
		if len(tail) > 600 {
			tail = tail[len(tail)-600:]
		}
		return nil, fmt.Errorf("parser failed for %s (%s): %v; log %s: %s", day, mode, parserErr, logPath, tail)
	}
	return captured.Bytes(), nil
}

func shellQuote(text string) string { return "'" + strings.ReplaceAll(text, "'", `'\''`) + "'" }

// ----- cache -----------------------------------------------------------------------------------------------------------------

// kindDir is the cache directory of a kind. The depth is part of the orderbook directory: changing orderbook.depth starts a new cache.
func (store *Store) kindDir(kind string) string {
	if kind == kindOrderbook {
		return filepath.Join(store.cache, fmt.Sprintf("%s-%d", kindOrderbook, store.config.Orderbook.Depth))
	}
	return filepath.Join(store.cache, kind)
}

func (store *Store) ParquetPath(kind, symbol, day string) string {
	return filepath.Join(store.kindDir(kind), symbol, day+".parquet")
}

func (store *Store) CSVPath(kind, symbol, day string) string {
	return filepath.Join(store.kindDir(kind), symbol, day+".csv")
}

func (store *Store) EnsureCSV(kind, symbol, day string) (string, error) {
	target := store.CSVPath(kind, symbol, day)
	store.csvMutex.Lock()
	defer store.csvMutex.Unlock()
	if _, err := os.Stat(target); err != nil {
		if err := WriteCSV(store.ParquetPath(kind, symbol, day), target); err != nil {
			return "", err
		}
	}
	return target, nil
}

// ----- extraction ------------------------------------------------------------------------------------------------------------

// Request returns a Future for the cached parquet path of (kind, symbol, day), queueing an extraction when it is not cached.
func (store *Store) Request(kind, symbol, day string) *Future {
	path := store.ParquetPath(kind, symbol, day)
	pass := passKey{kind, day}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if future := store.inflight[fileKey{kind, day, symbol}]; future != nil {
		return future
	}
	future := newFuture()
	if _, err := os.Stat(path); err == nil {
		future.resolve(path, nil)
		return future
	}
	store.inflight[fileKey{kind, day, symbol}] = future
	if store.pending[pass] == nil {
		store.pending[pass] = map[string]*Future{}
	}
	store.pending[pass][symbol] = future
	if _, ok := store.firstRequest[pass]; !ok {
		store.firstRequest[pass] = time.Now()
	}
	select {
	case store.wake <- struct{}{}:
	default:
	}
	return future
}

func (store *Store) scheduler() {
	window := time.Duration(store.config.Extract.BatchWindowSeconds * float64(time.Second))
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		store.mutex.Lock()
		for len(store.runningDays) < store.config.Extract.MaxParallel {
			var chosen passKey
			for pass, symbols := range store.pending {
				if len(symbols) == 0 || store.runningDays[pass] || time.Since(store.firstRequest[pass]) < window {
					continue
				}
				if chosen == (passKey{}) || store.firstRequest[pass].Before(store.firstRequest[chosen]) {
					chosen = pass
				}
			}
			if chosen == (passKey{}) {
				break
			}
			batch := store.pending[chosen]
			delete(store.pending, chosen)
			delete(store.firstRequest, chosen)
			store.runningDays[chosen] = true
			go store.runBatch(chosen, batch)
		}
		store.mutex.Unlock()
		select {
		case <-store.wake:
		case <-ticker.C:
		}
	}
}

func (store *Store) runBatch(pass passKey, batch map[string]*Future) {
	kind, day := pass.kind, pass.day
	started := time.Now()
	symbols := make([]string, 0, len(batch))
	for symbol := range batch {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols)
	preview := symbols
	if len(preview) > 8 {
		preview = preview[:8]
	}
	log.Printf("%s pass over %s for %d requested symbols: %s", kind, day, len(symbols), strings.Join(preview, ","))
	store.mutex.Lock()
	store.passCounter++
	work := filepath.Join(store.cache, "_work", fmt.Sprintf("%s-%d-%d", day, os.Getpid(), store.passCounter))
	store.mutex.Unlock()

	err := func() error {
		var missing []string // requested symbols that an earlier pass of this day did not already cache
		for _, symbol := range symbols {
			if _, err := os.Stat(store.ParquetPath(kind, symbol, day)); err != nil {
				missing = append(missing, symbol)
			}
		}
		if len(missing) == 0 {
			return nil
		}
		extract := symbols
		if store.config.Extract.WholeDay { // the pass costs the same for one symbol or all of them: take them all
			if all, err := store.SymbolsForDay(day); err == nil && len(all) > 0 {
				extract = all
			}
		}
		if err := os.MkdirAll(work, 0o755); err != nil {
			return err
		}
		symbolsFile := filepath.Join(work, "symbols.txt")
		if err := os.WriteFile(symbolsFile, []byte(strings.Join(extract, "\n")+"\n"), 0o644); err != nil {
			return err
		}
		environment := map[string]string{"DFS_SYMBOLS_FILE": symbolsFile, "DFS_OUT_DIR": filepath.Join(work, "out"), "DFS_KIND": kind}
		if kind == kindOrderbook {
			environment["DFS_DEPTH"] = fmt.Sprint(store.config.Orderbook.Depth)
		}
		if _, err := store.runPipeline(day, "extract", environment, false); err != nil {
			return err
		}
		requested := make(map[string]bool, len(symbols))
		for _, symbol := range symbols {
			requested[symbol] = true
		}
		for _, symbol := range extract {
			target := store.ParquetPath(kind, symbol, day)
			if _, err := os.Stat(target); err == nil {
				continue
			}
			produced := filepath.Join(work, "out", symbol, day+".parquet")
			if _, err := os.Stat(produced); err != nil && !requested[symbol] && !store.config.Extract.WholeDay {
				continue // not requested and no activity: nothing to cache (a whole-day pass caches an empty table so the day is never read again)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			if _, err := os.Stat(produced); err == nil {
				if err := moveFile(produced, target); err != nil {
					return err
				}
			} else if err := writeEmpty(target, schemaFor(kind, store.config.Orderbook.Depth)); err != nil { // no activity that day: serve an empty table with the right columns
				return err
			}
		}
		return nil
	}()
	if err != nil {
		log.Printf("%s pass over %s failed: %v", kind, day, err)
	}
	os.RemoveAll(work)

	store.mutex.Lock()
	for symbol, future := range batch {
		delete(store.inflight, fileKey{kind, day, symbol})
		if err == nil {
			delete(store.failures, kind+" "+symbol+" "+day)
			future.resolve(store.ParquetPath(kind, symbol, day), nil)
		} else {
			message := err.Error()
			if len(message) > 300 {
				message = message[:300]
			}
			store.failures[kind+" "+symbol+" "+day] = message
			future.resolve("", errors.New(err.Error()))
		}
	}
	delete(store.runningDays, pass)
	store.mutex.Unlock()
	status := "ok"
	if err != nil {
		status = "failed"
	}
	log.Printf("%s pass over %s finished in %.0fs (%s)", kind, day, time.Since(started).Seconds(), status)
	select {
	case store.wake <- struct{}{}:
	default:
	}
}

// moveFile renames, falling back to copy when the work and cache directories are on different filesystems.
func moveFile(source, target string) error {
	if err := os.Rename(source, target); err == nil {
		return nil
	}
	data, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	temporary := target + ".tmp"
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, target)
}

func writeEmpty(path string, schema *parquet.Schema) error {
	temporary := path + ".tmp"
	file, err := os.Create(temporary)
	if err != nil {
		return err
	}
	writer := parquet.NewWriter(file, schema, parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}))
	if err := writer.Close(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

// Status is what /.status serves.
type Status struct {
	RunningDays []string            `json:"running_days"`
	Pending     map[string][]string `json:"pending"`
	Failed      map[string]string   `json:"failed"`
	CachedDays  *int                `json:"cached_days"`
}

func (store *Store) Status() Status {
	store.mutex.Lock()
	status := Status{RunningDays: []string{}, Pending: map[string][]string{}, Failed: map[string]string{}}
	for pass := range store.runningDays {
		status.RunningDays = append(status.RunningDays, pass.String())
	}
	sort.Strings(status.RunningDays)
	for pass, symbols := range store.pending {
		for symbol := range symbols {
			status.Pending[pass.String()] = append(status.Pending[pass.String()], symbol)
		}
		sort.Strings(status.Pending[pass.String()])
	}
	for key, message := range store.failures {
		status.Failed[key] = message
	}
	store.mutex.Unlock()
	store.lookupMutex.Lock()
	if len(store.days) > 0 {
		count := len(store.days)
		status.CachedDays = &count
	}
	store.lookupMutex.Unlock()
	return status
}
