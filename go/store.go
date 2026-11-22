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

type dayKey struct{ day, symbol string }

// Store owns remote access, extraction scheduling and the on-disk cache:
// <cache>/<SYMBOL>/<YYYY-MM-DD>.parquet (and .csv on demand), <cache>/_symbols/<day>.txt, <cache>/_logs/.
// Requests for one day that arrive within the batch window are served by a single pass over that day's pcap.
type Store struct {
	config *Config
	cache  string

	mutex        sync.Mutex
	pending      map[string]map[string]*Future // day -> symbol -> future waiting for the next pass
	firstRequest map[string]time.Time
	inflight     map[dayKey]*Future
	runningDays  map[string]bool
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
		pending: map[string]map[string]*Future{}, firstRequest: map[string]time.Time{}, inflight: map[dayKey]*Future{},
		runningDays: map[string]bool{}, failures: map[string]string{}, wake: make(chan struct{}, 1),
	}
	for _, name := range []string{"_symbols", "_logs", "_work"} {
		if err := os.MkdirAll(filepath.Join(store.cache, name), 0o755); err != nil {
			return nil, err
		}
	}
	go store.scheduler()
	return store, nil
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

func (store *Store) ParquetPath(symbol, day string) string {
	return filepath.Join(store.cache, symbol, day+".parquet")
}

func (store *Store) CSVPath(symbol, day string) string {
	return filepath.Join(store.cache, symbol, day+".csv")
}

func (store *Store) EnsureCSV(symbol, day string) (string, error) {
	target := store.CSVPath(symbol, day)
	store.csvMutex.Lock()
	defer store.csvMutex.Unlock()
	if _, err := os.Stat(target); err != nil {
		if err := WriteCSV(store.ParquetPath(symbol, day), target); err != nil {
			return "", err
		}
	}
	return target, nil
}

// ----- extraction ------------------------------------------------------------------------------------------------------------

// Request returns a Future for the cached parquet path of (symbol, day), queueing an extraction when it is not cached.
func (store *Store) Request(symbol, day string) *Future {
	path := store.ParquetPath(symbol, day)
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if future := store.inflight[dayKey{day, symbol}]; future != nil {
		return future
	}
	future := newFuture()
	if _, err := os.Stat(path); err == nil {
		future.resolve(path, nil)
		return future
	}
	store.inflight[dayKey{day, symbol}] = future
	if store.pending[day] == nil {
		store.pending[day] = map[string]*Future{}
	}
	store.pending[day][symbol] = future
	if _, ok := store.firstRequest[day]; !ok {
		store.firstRequest[day] = time.Now()
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
			var chosen string
			for day, symbols := range store.pending {
				if len(symbols) == 0 || store.runningDays[day] || time.Since(store.firstRequest[day]) < window {
					continue
				}
				if chosen == "" || store.firstRequest[day].Before(store.firstRequest[chosen]) {
					chosen = day
				}
			}
			if chosen == "" {
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

func (store *Store) runBatch(day string, batch map[string]*Future) {
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
	log.Printf("pass over %s for %d symbols: %s", day, len(symbols), strings.Join(preview, ","))
	store.mutex.Lock()
	store.passCounter++
	work := filepath.Join(store.cache, "_work", fmt.Sprintf("%s-%d-%d", day, os.Getpid(), store.passCounter))
	store.mutex.Unlock()

	err := func() error {
		if err := os.MkdirAll(work, 0o755); err != nil {
			return err
		}
		symbolsFile := filepath.Join(work, "symbols.txt")
		if err := os.WriteFile(symbolsFile, []byte(strings.Join(symbols, "\n")+"\n"), 0o644); err != nil {
			return err
		}
		if _, err := store.runPipeline(day, "extract", map[string]string{"DFS_SYMBOLS_FILE": symbolsFile, "DFS_OUT_DIR": filepath.Join(work, "out")}, false); err != nil {
			return err
		}
		for _, symbol := range symbols {
			target := store.ParquetPath(symbol, day)
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			produced := filepath.Join(work, "out", symbol, day+".parquet")
			if _, err := os.Stat(produced); err == nil {
				if err := moveFile(produced, target); err != nil {
					return err
				}
			} else if err := writeEmpty(target); err != nil { // no activity that day: serve an empty table with the right columns
				return err
			}
		}
		return nil
	}()
	if err != nil {
		log.Printf("pass over %s failed: %v", day, err)
	}
	os.RemoveAll(work)

	store.mutex.Lock()
	for symbol, future := range batch {
		delete(store.inflight, dayKey{day, symbol})
		if err == nil {
			delete(store.failures, symbol+" "+day)
			future.resolve(store.ParquetPath(symbol, day), nil)
		} else {
			message := err.Error()
			if len(message) > 300 {
				message = message[:300]
			}
			store.failures[symbol+" "+day] = message
			future.resolve("", errors.New(err.Error()))
		}
	}
	delete(store.runningDays, day)
	store.mutex.Unlock()
	status := "ok"
	if err != nil {
		status = "failed"
	}
	log.Printf("pass over %s finished in %.0fs (%s)", day, time.Since(started).Seconds(), status)
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

func writeEmpty(path string) error {
	temporary := path + ".tmp"
	file, err := os.Create(temporary)
	if err != nil {
		return err
	}
	writer := parquet.NewGenericWriter[Tick](file, parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}))
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
	for day := range store.runningDays {
		status.RunningDays = append(status.RunningDays, day)
	}
	sort.Strings(status.RunningDays)
	for day, symbols := range store.pending {
		for symbol := range symbols {
			status.Pending[day] = append(status.Pending[day], symbol)
		}
		sort.Strings(status.Pending[day])
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
