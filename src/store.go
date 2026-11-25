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
	"sync/atomic"
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
type passKey struct{ feed, kind, day string }

func (key passKey) String() string { return joinFeed(key.feed, key.kind+" "+key.day, " ") }

// joinFeed prefixes text with the feed name; the anonymous feed adds nothing.
func joinFeed(feed, text, separator string) string {
	if feed == "" {
		return text
	}
	return feed + separator + text
}

type fileKey struct{ feed, kind, day, symbol string }

// feedState is what the store knows about one feed's pcap files: the days found on the host and the symbols of the newest day.
type feedState struct {
	*Feed
	lookupMutex sync.Mutex
	days        []string
	daysLoaded  time.Time
	symbols     []string
	symbolSet   map[string]bool
}

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

	feeds    map[string]*feedState
	csvMutex sync.Mutex

	startedAt  time.Time
	mountpoint string // set by mount; empty when serving the console without a mount
	running    map[passKey]*runInfo
	history    []passRecord
	counters   counters
	cacheStats atomic.Pointer[CacheStats]
	nextScan   atomic.Int64 // unix milliseconds of the next cache scan
	tel        *Telemetry
	sampler    *Sampler

	touchMutex sync.Mutex
	touched    map[string]time.Time // path -> last access-time bump, so a hot file costs one metadata write per touch interval
	evictPause time.Duration        // pause between batches of deletions
}

func NewStore(config *Config) (*Store, error) {
	store := &Store{
		feeds:  map[string]*feedState{},
		config: config, cache: config.CacheDir,
		pending: map[passKey]map[string]*Future{}, firstRequest: map[passKey]time.Time{}, inflight: map[fileKey]*Future{},
		runningDays: map[passKey]bool{}, failures: map[string]string{}, wake: make(chan struct{}, 1),
		touched: map[string]time.Time{}, evictPause: 2 * time.Second,
		startedAt: time.Now(), running: map[passKey]*runInfo{}, tel: newTelemetry(config),
	}
	for _, feed := range config.Feeds {
		store.feeds[feed.Name] = &feedState{Feed: feed}
	}
	store.tel.observe(store)
	for _, name := range []string{"_symbols", "_logs", "_work"} {
		if err := os.MkdirAll(filepath.Join(store.cache, name), 0o755); err != nil {
			return nil, err
		}
	}
	if !config.Named() {
		if err := migrateLegacyCache(store.cache); err != nil {
			return nil, err
		}
	}
	go store.scheduler()
	if config.Cache.MaxGB > 0 || config.UI.Listen != "" || config.Telemetry.metricsOn() {
		go store.evictLoop() // also measures the cache for the console and the gauges
	}
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

// feed returns the state of a feed, or an error naming the configured ones.
func (store *Store) feed(name string) (*feedState, error) {
	if state := store.feeds[name]; state != nil {
		return state, nil
	}
	return nil, fmt.Errorf("unknown feed %q", name)
}

func (feed *feedState) remoteCommand(command string) *exec.Cmd {
	if feed.Remote.SSHHost == "" {
		return exec.Command("sh", "-c", command)
	}
	arguments := append(append([]string{}, feed.Remote.SSHOptions...), feed.Remote.SSHHost, command)
	return exec.Command("ssh", arguments...)
}

// Days returns the trading days that have a pcap on the host, ascending. Cached for days_ttl_seconds.
func (store *Store) Days(feedName string) ([]string, error) {
	feed, err := store.feed(feedName)
	if err != nil {
		return nil, err
	}
	feed.lookupMutex.Lock()
	defer feed.lookupMutex.Unlock()
	if len(feed.days) > 0 && time.Since(feed.daysLoaded).Seconds() < store.config.DaysTTLSeconds {
		return feed.days, nil
	}
	command := feed.remoteCommand("ls -1 " + feed.DayGlob() + " 2>/dev/null")
	output, _ := command.Output() // ls exits non-zero when a glob matches nothing; an empty result is handled below
	regex := feed.DayRegex()
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
	if len(days) > 0 || len(feed.days) == 0 {
		feed.days, feed.daysLoaded = days, time.Now()
	}
	return feed.days, nil
}

// Symbols returns the symbols of the newest day, computed once by running the parser in list mode.
func (store *Store) Symbols(feedName string) ([]string, error) {
	feed, err := store.feed(feedName)
	if err != nil {
		return nil, err
	}
	days, err := store.Days(feedName)
	if err != nil || len(days) == 0 {
		return nil, err
	}
	feed.lookupMutex.Lock()
	if feed.symbols != nil {
		defer feed.lookupMutex.Unlock()
		return feed.symbols, nil
	}
	feed.lookupMutex.Unlock()
	names, err := store.SymbolsForDay(feedName, days[len(days)-1])
	if err != nil {
		return nil, err
	}
	feed.lookupMutex.Lock()
	defer feed.lookupMutex.Unlock()
	feed.symbols = names
	feed.symbolSet = make(map[string]bool, len(names))
	for _, name := range names {
		feed.symbolSet[name] = true
	}
	return names, nil
}

func (store *Store) HasSymbol(feedName, symbol string) (bool, error) {
	if _, err := store.Symbols(feedName); err != nil {
		return false, err
	}
	feed := store.feeds[feedName]
	feed.lookupMutex.Lock()
	defer feed.lookupMutex.Unlock()
	return feed.symbolSet[symbol], nil
}

func (store *Store) SymbolsForDay(feedName, day string) ([]string, error) {
	cached := filepath.Join(store.cache, "_symbols", joinFeed(feedName, day, "-")+".txt")
	if _, err := os.Stat(cached); err != nil {
		output, err := store.runPipeline(feedName, day, "list", nil, true)
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
func (store *Store) runPipeline(feedName, day, mode string, extraEnv map[string]string, capture bool) ([]byte, error) {
	feed, err := store.feed(feedName)
	if err != nil {
		return nil, err
	}
	pcap := feed.PcapFor(day)
	store.mutex.Lock()
	store.passCounter++
	counter := store.passCounter
	store.mutex.Unlock()
	logPath := filepath.Join(store.cache, "_logs", fmt.Sprintf("%s-%s-%d.log", joinFeed(feedName, day, "-"), mode, counter))
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	defer logFile.Close()

	source := feed.remoteCommand("cat " + shellQuote(pcap))
	source.Stderr = logFile
	parser := exec.Command(feed.Parser.Command[0], feed.Parser.Command[1:]...)
	parser.Stderr = logFile
	executable, _ := os.Executable()
	environment := append(os.Environ(), "DFS_INGEST="+executable+" ingest")
	for key, value := range feed.Parser.Env {
		environment = append(environment, key+"="+value)
	}
	environment = append(environment, "DFS_FEED="+feedName, "DFS_MODE="+mode, "DFS_DATE="+day, "DFS_PCAP_NAME="+filepath.Base(pcap))
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
func (store *Store) kindDir(feed, kind string) string {
	if kind == kindOrderbook {
		kind = fmt.Sprintf("%s-%d", kindOrderbook, store.config.Orderbook.Depth)
	}
	return filepath.Join(store.cache, feed, kind) // the anonymous feed has no directory of its own
}

func (store *Store) ParquetPath(feed, kind, symbol, day string) string {
	return filepath.Join(store.kindDir(feed, kind), symbol, day+".parquet")
}

func (store *Store) CSVPath(feed, kind, symbol, day string) string {
	return filepath.Join(store.kindDir(feed, kind), symbol, day+".csv")
}

func (store *Store) EnsureCSV(feed, kind, symbol, day string) (string, error) {
	target := store.CSVPath(feed, kind, symbol, day)
	store.csvMutex.Lock()
	defer store.csvMutex.Unlock()
	if _, err := os.Stat(target); err != nil {
		if err := WriteCSV(store.ParquetPath(feed, kind, symbol, day), target); err != nil {
			return "", err
		}
	} else {
		store.touch(target)
	}
	return target, nil
}

// ----- extraction ------------------------------------------------------------------------------------------------------------

// Request returns a Future for the cached parquet path of (kind, symbol, day), queueing an extraction when it is not cached.
func (store *Store) Request(feed, kind, symbol, day string) *Future {
	path := store.ParquetPath(feed, kind, symbol, day)
	pass := passKey{feed, kind, day}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if future := store.inflight[fileKey{feed, kind, day, symbol}]; future != nil {
		store.counters.misses.Add(1)
		store.tel.request(feed, kind, "joined")
		return future
	}
	future := newFuture()
	if _, err := os.Stat(path); err == nil {
		store.touch(path)
		store.counters.hits.Add(1)
		store.tel.request(feed, kind, "hit")
		future.resolve(path, nil)
		return future
	}
	store.counters.misses.Add(1)
	store.tel.request(feed, kind, "miss")
	store.inflight[fileKey{feed, kind, day, symbol}] = future
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
	feed, kind, day := pass.feed, pass.kind, pass.day
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
	log.Printf("%s pass over %s for %d requested symbols: %s", joinFeed(feed, kind, " "), day, len(symbols), strings.Join(preview, ","))
	store.mutex.Lock()
	store.passCounter++
	work := filepath.Join(store.cache, "_work", fmt.Sprintf("%s-%d-%d", joinFeed(feed, day, "-"), os.Getpid(), store.passCounter))
	store.running[pass] = &runInfo{feed: feed, kind: kind, day: day, symbols: len(symbols), started: started}
	store.mutex.Unlock()
	passContext, span := store.tel.startPass(feed, kind, day, len(symbols))

	err := func() error {
		var missing []string // requested symbols that an earlier pass of this day did not already cache
		for _, symbol := range symbols {
			if _, err := os.Stat(store.ParquetPath(feed, kind, symbol, day)); err != nil {
				missing = append(missing, symbol)
			}
		}
		if len(missing) == 0 {
			return nil
		}
		extract := symbols
		if store.config.Extract.WholeDay { // the pass costs the same for one symbol or all of them: take them all
			if all, err := store.SymbolsForDay(feed, day); err == nil && len(all) > 0 {
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
		pipeline := store.tel.startPipeline(passContext, len(extract), store.config.Extract.WholeDay)
		_, pipelineErr := store.runPipeline(feed, day, "extract", environment, false)
		if pipelineErr != nil {
			pipeline.RecordError(pipelineErr)
		}
		pipeline.End()
		if pipelineErr != nil {
			return pipelineErr
		}
		requested := make(map[string]bool, len(symbols))
		for _, symbol := range symbols {
			requested[symbol] = true
		}
		for _, symbol := range extract {
			target := store.ParquetPath(feed, kind, symbol, day)
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
			if info, err := os.Stat(produced); err == nil {
				if err := moveFile(produced, target); err != nil {
					return err
				}
				store.counters.written.Add(info.Size())
			} else if err := writeEmpty(target, schemaFor(kind, store.config.Orderbook.Depth)); err != nil { // no activity that day: serve an empty table with the right columns
				return err
			}
		}
		return nil
	}()
	if err != nil {
		log.Printf("%s pass over %s failed: %v", joinFeed(feed, kind, " "), day, err)
	}
	os.RemoveAll(work)

	store.mutex.Lock()
	for symbol, future := range batch {
		delete(store.inflight, fileKey{feed, kind, day, symbol})
		if err == nil {
			delete(store.failures, joinFeed(feed, kind+" "+symbol+" "+day, " "))
			future.resolve(store.ParquetPath(feed, kind, symbol, day), nil)
		} else {
			message := err.Error()
			if len(message) > 300 {
				message = message[:300]
			}
			store.failures[joinFeed(feed, kind+" "+symbol+" "+day, " ")] = message
			future.resolve("", errors.New(err.Error()))
		}
	}
	delete(store.runningDays, pass)
	delete(store.running, pass)
	store.mutex.Unlock()
	status := "ok"
	if err != nil {
		status = "failed"
	}
	took := time.Since(started)
	store.tel.finishPass(span, feed, kind, len(symbols), took, err)
	store.recordPass(passRecord{Feed: feed, Kind: kind, Day: day, Symbols: len(symbols), Seconds: took.Seconds(), Status: status, Finished: time.Now()})
	log.Printf("%s pass over %s finished in %.0fs (%s)", joinFeed(feed, kind, " "), day, took.Seconds(), status)
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
	most := 0
	for _, feed := range store.feeds {
		feed.lookupMutex.Lock()
		if len(feed.days) > most {
			most = len(feed.days)
		}
		feed.lookupMutex.Unlock()
	}
	if most > 0 {
		status.CachedDays = &most
	}
	return status
}
