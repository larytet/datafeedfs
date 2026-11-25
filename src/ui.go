package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"syscall"
	"time"
)

// The web console: one embedded page and a small JSON API. Off unless [ui] listen is set.
//
//	GET  /api/info            {auth_required}: no token needed, so the page knows whether to ask for one
//	GET  /api/status          mount, passes, failures, cache by kind, listing
//	GET  /api/metrics?range=  time series (5m, 15m, 1h), recent passes, busiest processes, host
//	GET  /api/config          config.toml text, its etag, the etag the running process started with
//	POST /api/config/validate {text}: parse and validate without saving
//	PUT  /api/config          {text, etag}: save (needs allow_edit)
//	POST /api/restart         stop the process so its supervisor restarts it (needs allow_restart)
//
// Everything under /api except /api/info needs "Authorization: Bearer <ui.token>" when a token is set. Writes also need the header
// "X-Requested-With: datafeedfs-ui" (a browser will not add it to a cross-site request without a CORS preflight, which this server never grants)
// and, when an Origin is sent, the same host as the request. Without a token only loopback Host names are accepted, against DNS rebinding.

//go:embed ui/index.html
var uiFiles embed.FS

type uiServer struct {
	store *Store
	mux   *http.ServeMux
}

type UIHandle struct {
	Addr   string
	server *http.Server
}

func (handle *UIHandle) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	handle.server.Shutdown(ctx)
}

// StartUI serves the console on config.UI.Listen.
func (store *Store) StartUI() (*UIHandle, error) {
	if store.config.UI.Listen == "" {
		return nil, fmt.Errorf("ui.listen is not set in the config")
	}
	listener, err := net.Listen("tcp", store.config.UI.Listen)
	if err != nil {
		return nil, err
	}
	if store.sampler == nil {
		store.sampler = newSampler(store)
	}
	ui := &uiServer{store: store, mux: http.NewServeMux()}
	ui.routes()
	server := &http.Server{Handler: ui, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("ui: %v", err)
		}
	}()
	address := listener.Addr().String()
	log.Printf("ui: console at http://%s (edit %v, restart %v, token %v)", address, store.config.UI.AllowEdit, store.config.UI.AllowRestart, store.config.UI.Token != "")
	return &UIHandle{Addr: address, server: server}, nil
}

func (ui *uiServer) routes() {
	ui.mux.HandleFunc("GET /{$}", ui.page)
	ui.mux.HandleFunc("GET /api/info", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"auth_required": ui.store.config.UI.Token != ""})
	})
	ui.mux.HandleFunc("GET /api/status", ui.status)
	ui.mux.HandleFunc("GET /api/metrics", ui.metrics)
	ui.mux.HandleFunc("GET /api/config", ui.getConfig)
	ui.mux.HandleFunc("POST /api/config/validate", ui.validate)
	ui.mux.HandleFunc("PUT /api/config", ui.putConfig)
	ui.mux.HandleFunc("POST /api/restart", ui.restart)
}

func (ui *uiServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	config := ui.store.config.UI
	if config.Token == "" && !loopbackRequest(r.Host, config.Listen) {
		http.Error(w, "unexpected Host header", http.StatusForbidden)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/") && r.URL.Path != "/api/info" && config.Token != "" {
		given := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(given), []byte(config.Token)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing or wrong token"})
			return
		}
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		if r.Header.Get("X-Requested-With") != "datafeedfs-ui" {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "missing X-Requested-With header"})
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" && !strings.HasSuffix(origin, "://"+r.Host) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "cross-origin request refused"})
			return
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	ui.mux.ServeHTTP(w, r)
}

// loopbackRequest accepts localhost names and addresses, and the host of the configured listen address.
func loopbackRequest(requestHost, listen string) bool {
	host := requestHost
	if h, _, err := net.SplitHostPort(requestHost); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if isLoopbackHost(host) {
		return true
	}
	configured, _, _ := net.SplitHostPort(listen)
	return configured != "" && configured == host
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func (ui *uiServer) page(w http.ResponseWriter, r *http.Request) {
	data, _ := uiFiles.ReadFile("ui/index.html")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
	w.Write(data)
}

// ----- status ----------------------------------------------------------------------------------------------------------------

func version() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && len(setting.Value) >= 7 {
				return setting.Value[:7]
			}
		}
	}
	return "dev"
}

func millis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func (ui *uiServer) status(w http.ResponseWriter, r *http.Request) {
	store := ui.store
	config := store.config
	hostname, _ := os.Hostname()
	var sources []string
	seenSource := map[string]bool{}
	for _, feed := range config.Feeds {
		source := feed.Remote.SSHHost
		if source == "" {
			source = "this machine"
		}
		if !seenSource[source] {
			seenSource[source] = true
			sources = append(sources, source)
		}
	}
	source := strings.Join(sources, ", ")

	store.mutex.Lock()
	type passJSON struct {
		Feed      string `json:"feed,omitempty"`
		Kind      string `json:"kind"`
		Day       string `json:"day"`
		Symbols   int    `json:"symbols"`
		StartedMS int64  `json:"started_ms,omitempty"`
	}
	running, pending := []passJSON{}, []passJSON{}
	for _, info := range store.running {
		running = append(running, passJSON{info.feed, info.kind, info.day, info.symbols, info.started.UnixMilli()})
	}
	for pass, symbols := range store.pending {
		pending = append(pending, passJSON{Feed: pass.feed, Kind: pass.kind, Day: pass.day, Symbols: len(symbols)})
	}
	failed := []map[string]string{}
	for key, message := range store.failures {
		failed = append(failed, map[string]string{"key": key, "message": message})
	}
	store.mutex.Unlock()
	sort.Slice(running, func(i, j int) bool { return running[i].StartedMS < running[j].StartedMS })
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].Feed+pending[i].Kind+pending[i].Day < pending[j].Feed+pending[j].Kind+pending[j].Day
	})
	sort.Slice(failed, func(i, j int) bool { return failed[i]["key"] < failed[j]["key"] })

	cache := map[string]any{
		"dir": store.cache, "limit_bytes": int64(config.Cache.MaxGB * (1 << 30)), "evict_to_bytes": int64(config.Cache.MaxGB * config.Cache.EvictTo * (1 << 30)),
		"evicted_files": store.counters.evictedFiles.Load(), "evicted_bytes": store.counters.evictedBytes.Load(),
		"next_scan_ms": store.nextScan.Load(), "bytes": 0, "files": 0, "by_kind": map[string]any{}, "scanned_ms": 0,
	}
	if stats := store.cacheStats.Load(); stats != nil {
		byKind := map[string]any{}
		for kind, stat := range stats.ByKind {
			byKind[kind] = map[string]any{"bytes": stat.Bytes, "files": stat.Files}
		}
		cache["bytes"], cache["files"], cache["by_kind"], cache["scanned_ms"] = stats.Bytes, stats.Files, byKind, stats.At.UnixMilli()
	}
	if store.sampler != nil {
		_, _, host := store.sampler.Snapshot()
		cache["disk"] = map[string]any{"total": host.DiskTotal, "free": host.DiskFree}
	}

	// the listing spans the feeds: the longest day list, the widest day range, the most symbols
	days, symbols, firstDay, lastDay := 0, 0, "", ""
	feedListing := []map[string]any{}
	for _, feed := range config.Feeds {
		state := store.feeds[feed.Name]
		state.lookupMutex.Lock()
		entry := map[string]any{"feed": feed.Name, "pcap_path": feed.pattern(), "days": len(state.days), "symbols": len(state.symbols)}
		if len(state.days) > 0 {
			first, last := state.days[0], state.days[len(state.days)-1]
			entry["first_day"], entry["last_day"] = first, last
			if firstDay == "" || first < firstDay {
				firstDay = first
			}
			if last > lastDay {
				lastDay = last
			}
		}
		days, symbols = max(days, len(state.days)), max(symbols, len(state.symbols))
		state.lookupMutex.Unlock()
		feedListing = append(feedListing, entry)
	}
	listing := map[string]any{"days": nil, "first_day": "", "last_day": "", "symbols": nil}
	if days > 0 {
		listing["days"], listing["first_day"], listing["last_day"] = days, firstDay, lastDay
	}
	if symbols > 0 {
		listing["symbols"] = symbols
	}
	if config.Named() {
		listing["feeds"] = feedListing
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"mount": map[string]any{
			"mountpoint": store.mountpoint, "host": hostname, "source": source, "version": version(),
			"started_ms": store.startedAt.UnixMilli(), "uptime_s": int64(time.Since(store.startedAt).Seconds()),
			"allow_edit": config.UI.AllowEdit, "allow_restart": config.UI.AllowRestart,
		},
		"passes":   map[string]any{"running": running, "pending": pending, "failed": failed, "slots": config.Extract.MaxParallel},
		"cache":    cache,
		"listing":  listing,
		"settings": map[string]any{"on_miss": config.Extract.OnMiss, "orderbook_depth": config.Orderbook.Depth, "whole_day": config.Extract.WholeDay},
	})
}

// ----- metrics ---------------------------------------------------------------------------------------------------------------

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func (ui *uiServer) metrics(w http.ResponseWriter, r *http.Request) {
	window, group := 90, 1
	switch r.URL.Query().Get("range") {
	case "5m":
		window = 30
	case "1h":
		window, group = 360, 3
	}
	response := map[string]any{"step_s": sampleStep.Seconds() * float64(group)}
	samples, processes, host := ui.store.sampler.Snapshot()
	if len(samples) > window {
		samples = samples[len(samples)-window:]
	}
	at := []int64{}
	net, cacheWrite, cpuTotal, load1, load15, mem := []float64{}, []float64{}, []float64{}, []float64{}, []float64{}, []float64{}
	hit := []*float64{}
	groups := map[string][]float64{}
	for _, name := range append(append([]string{}, groupNames...), "other") {
		groups[name] = []float64{}
	}
	for start := 0; start < len(samples); start += group {
		end := start + group
		if end > len(samples) {
			end = len(samples)
		}
		chunk := samples[start:end]
		pick := func(get func(Sample) float64) float64 {
			values := make([]float64, len(chunk))
			for i, sample := range chunk {
				values[i] = get(sample)
			}
			return average(values)
		}
		at = append(at, chunk[len(chunk)-1].At.UnixMilli())
		net = append(net, pick(func(s Sample) float64 { return s.NetMBs }))
		cacheWrite = append(cacheWrite, pick(func(s Sample) float64 { return s.CacheMBs }))
		cpuTotal = append(cpuTotal, pick(func(s Sample) float64 { return s.CPUCores }))
		load1 = append(load1, pick(func(s Sample) float64 { return s.Load1 }))
		load15 = append(load15, pick(func(s Sample) float64 { return s.Load15 }))
		mem = append(mem, pick(func(s Sample) float64 { return s.MemUsedGiB }))
		for name := range groups {
			name := name
			groups[name] = append(groups[name], pick(func(s Sample) float64 { return s.Groups[name] }))
		}
		var ratios []float64
		for _, sample := range chunk {
			if sample.HitRatio != nil {
				ratios = append(ratios, *sample.HitRatio)
			}
		}
		if len(ratios) == 0 {
			hit = append(hit, nil)
		} else {
			value := average(ratios)
			hit = append(hit, &value)
		}
	}
	response["t"], response["net_mbs"], response["cache_write_mbs"], response["cpu_total"] = at, net, cacheWrite, cpuTotal
	response["load1"], response["load15"], response["mem_gib"], response["hit_ratio"], response["cpu"] = load1, load15, mem, hit, groups

	ui.store.mutex.Lock()
	history := append([]passRecord(nil), ui.store.history...)
	ui.store.mutex.Unlock()
	passes := []map[string]any{}
	for i := len(history) - 1; i >= 0 && len(passes) < 12; i-- {
		record := history[i]
		passes = append(passes, map[string]any{"feed": record.Feed, "kind": record.Kind, "day": record.Day, "symbols": record.Symbols, "seconds": record.Seconds, "status": record.Status, "finished_ms": record.Finished.UnixMilli()})
	}
	response["passes"] = passes
	top := []map[string]any{}
	for _, process := range processes {
		top = append(top, map[string]any{"pid": process.PID, "name": process.Name, "command": process.Command, "cpu": process.CPU, "rss_mb": process.RSSMB})
	}
	response["processes"] = top
	response["host"] = map[string]any{"cpus": host.CPUs, "mem_total_gib": host.MemTotalGiB, "disk_total": host.DiskTotal, "disk_free": host.DiskFree, "supported": host.Supported}
	writeJSON(w, http.StatusOK, response)
}

// ----- config ----------------------------------------------------------------------------------------------------------------

func etagOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:8])
}

func (ui *uiServer) configResponse() (map[string]any, int, error) {
	config := ui.store.config
	if config.Path == "" {
		return nil, http.StatusNotFound, fmt.Errorf("the process was not started from a config file")
	}
	data, err := os.ReadFile(config.Path)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return map[string]any{
		"path": config.Path, "text": string(data), "etag": etagOf(data), "applied_etag": etagOf(config.Raw),
		"allow_edit": config.UI.AllowEdit, "allow_restart": config.UI.AllowRestart,
	}, http.StatusOK, nil
}

func (ui *uiServer) getConfig(w http.ResponseWriter, r *http.Request) {
	response, status, err := ui.configResponse()
	if err != nil {
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, response)
}

type configRequest struct {
	Text string `json:"text"`
	ETag string `json:"etag"`
}

func readConfigRequest(w http.ResponseWriter, r *http.Request) (configRequest, bool) {
	var request configRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20+1)).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request body: " + err.Error()})
		return request, false
	}
	if len(request.Text) > 1<<20 {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "config.toml is limited to 1 MiB"})
		return request, false
	}
	return request, true
}

func (ui *uiServer) validate(w http.ResponseWriter, r *http.Request) {
	request, ok := readConfigRequest(w, r)
	if !ok {
		return
	}
	if _, err := ParseConfig([]byte(request.Text), ui.store.config.Path); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": cleanError(err, ui.store.config.Path)})
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// cleanError drops the file path prefix the parser adds: the page shows the message next to the file name already.
func cleanError(err error, path string) string {
	return strings.TrimPrefix(err.Error(), path+": ")
}

func (ui *uiServer) putConfig(w http.ResponseWriter, r *http.Request) {
	config := ui.store.config
	if !config.UI.AllowEdit {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "editing is disabled: set ui.allow_edit = true in config.toml and restart"})
		return
	}
	request, ok := readConfigRequest(w, r)
	if !ok {
		return
	}
	if _, err := ParseConfig([]byte(request.Text), config.Path); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": cleanError(err, config.Path)})
		return
	}
	current, err := os.ReadFile(config.Path)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if request.ETag != etagOf(current) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "config.toml changed on disk since the page loaded it: reload to see the new version"})
		return
	}
	if err := writeConfigFile(config.Path, []byte(request.Text)); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "cannot write " + config.Path + ": " + err.Error() + " (a container bind mount must not be read-only)"})
		return
	}
	log.Printf("ui: config.toml saved (%d bytes); it takes effect after a restart", len(request.Text))
	response, status, err := ui.configResponse()
	if err != nil {
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, response)
}

// writeConfigFile replaces the file atomically; where that is impossible (a single file bind-mounted into a container cannot be renamed over) it rewrites it in place.
func writeConfigFile(path string, data []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".config-*.toml")
	if err == nil {
		name := temporary.Name()
		_, writeErr := temporary.Write(data)
		closeErr := temporary.Close()
		if writeErr == nil && closeErr == nil {
			if info, statErr := os.Stat(path); statErr == nil {
				os.Chmod(name, info.Mode().Perm())
			}
			if os.Rename(name, path) == nil {
				return nil
			}
		}
		os.Remove(name)
	}
	return os.WriteFile(path, data, 0o644)
}

func (ui *uiServer) restart(w http.ResponseWriter, r *http.Request) {
	if !ui.store.config.UI.AllowRestart {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "restart is disabled: set ui.allow_restart = true, or restart the container yourself"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "stopping"})
	log.Print("ui: restart requested, stopping")
	go func() {
		time.Sleep(300 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGTERM)
	}()
}
