package main

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	toml "github.com/pelletier/go-toml/v2"
)

// RemoteConfig says where the pcap files are. An empty SSHHost means the local machine.
type RemoteConfig struct {
	SSHHost    string   `toml:"ssh_host"`
	PcapPath   string   `toml:"pcap_path"` // one day's pcap with {yyyy} {mm} {dd} placeholders
	SSHOptions []string `toml:"ssh_options"`
}

// ParserConfig is the command run once per pass with the pcap on stdin (see parsers/parse_nasdaq_itch.sh).
type ParserConfig struct {
	Command []string          `toml:"-"`
	Env     map[string]string `toml:"env"`
}

// Feed is one pcap source: a venue's feed such as cboe_edga or nasdaq_tv. Each feed has its own pcap files, ssh host and parser and its own
// directory in the mount and the cache. A configuration without [feeds.*] has one anonymous feed (Name ""), laid out as before: /tick, /orderbook, /stream.
type Feed struct {
	Name   string
	Remote RemoteConfig
	Parser ParserConfig
}

var feedNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,39}$`)

type ExtractConfig struct {
	BatchWindowSeconds float64 `toml:"batch_window_seconds"` // requests for one day inside this window share a pass over the pcap
	MaxParallel        int     `toml:"max_parallel"`         // passes over different days at once
	OnMiss             string  `toml:"on_miss"`              // "block" or "async" (open fails with EAGAIN, extraction continues)
	WholeDay           bool    `toml:"whole_day"`            // a miss extracts every symbol of that day in the same pass: the pcap is read once per day
}

// OrderbookConfig sizes the files of /orderbook.
type OrderbookConfig struct {
	Depth int `toml:"depth"` // price levels per side (default 5, at most 64)
}

// CacheConfig bounds the cache. Eviction is lazy on purpose: access times are recorded at most once per TouchIntervalSeconds
// per file, a background scan runs every ScanIntervalSeconds, and nothing is deleted until the cache is over MaxGB, after which it is
// trimmed to EvictTo*MaxGB in small batches. So the cache can exceed the cap a little, and reads cost no disk writes in between.
type CacheConfig struct {
	MaxGB                float64 `toml:"max_gb"`                 // size cap of the parquet and csv files; 0 = unlimited (no eviction)
	EvictTo              float64 `toml:"evict_to"`               // once over the cap, delete down to this fraction of it (default 0.9)
	ScanIntervalSeconds  float64 `toml:"scan_interval_seconds"`  // how often the cache size is checked (default 600)
	TouchIntervalSeconds float64 `toml:"touch_interval_seconds"` // a file's access time is bumped at most this often (default 3600)
	MinAgeSeconds        float64 `toml:"min_age_seconds"`        // files used more recently than this are never evicted (default and floor: touch interval)
}

// TelemetryConfig turns on OpenTelemetry export (OTLP over HTTP): metrics and one trace per extraction pass. Off by default; with it off
// the instruments are no-ops. Standard OTEL_EXPORTER_OTLP_* environment variables (headers, certificates, timeouts) are honoured by the exporters.
type TelemetryConfig struct {
	Enabled                bool    `toml:"enabled"`
	Endpoint               string  `toml:"endpoint"`                 // OTLP/HTTP base URL, e.g. http://collector:4318; empty: OTEL_EXPORTER_OTLP_ENDPOINT or http://localhost:4318
	ServiceName            string  `toml:"service_name"`             // default datafeedfs
	MetricsIntervalSeconds float64 `toml:"metrics_interval_seconds"` // default 15
	Metrics                *bool   `toml:"metrics"`                  // default true
	Traces                 *bool   `toml:"traces"`                   // default true
}

// UIConfig serves the web console (status, configuration, performance, load). Off unless Listen is set.
type UIConfig struct {
	Listen       string `toml:"listen"`        // host:port, for example 127.0.0.1:8088; empty = no web console
	AllowEdit    bool   `toml:"allow_edit"`    // let the console save config.toml: whoever can save it can make the parser command run anything
	AllowRestart bool   `toml:"allow_restart"` // let the console stop the process so its supervisor (docker restart policy) starts it with the saved config
	Token        string `toml:"token"`         // bearer token for /api; required when Listen is not a loopback address
}

type Config struct {
	Path           string // the file this was read from (empty when parsed from memory)
	Raw            []byte // its content at load time: what the running process was started with
	Telemetry      TelemetryConfig
	UI             UIConfig
	Remote         RemoteConfig
	Parser         ParserConfig
	CacheDir       string
	Cache          CacheConfig
	Extract        ExtractConfig
	Orderbook      OrderbookConfig
	DaysTTLSeconds float64
	Feeds          []*Feed // sorted by name; one anonymous feed when [feeds.*] is absent
}

// Feed returns the feed with that name ("" for the anonymous one).
func (config *Config) Feed(name string) *Feed {
	for _, feed := range config.Feeds {
		if feed.Name == name {
			return feed
		}
	}
	return nil
}

// Named reports whether the feeds have their own directories (false for the legacy single-feed layout).
func (config *Config) Named() bool { return len(config.Feeds) != 1 || config.Feeds[0].Name != "" }

// rawFeed is a [feeds.NAME] table. Every field is optional and defaults to the top-level [remote] and [parser].
type rawFeed struct {
	SSHHost    *string  `toml:"ssh_host"`
	PcapPath   string   `toml:"pcap_path"`
	SSHOptions []string `toml:"ssh_options"`
	Parser     struct {
		Command any            `toml:"command"`
		Env     map[string]any `toml:"env"`
	} `toml:"parser"`
}

type rawConfig struct {
	Remote RemoteConfig `toml:"remote"`
	Parser struct {
		Command any            `toml:"command"`
		Env     map[string]any `toml:"env"`
	} `toml:"parser"`
	Cache struct {
		Dir string `toml:"dir"`
		CacheConfig
	} `toml:"cache"`
	Feeds          map[string]rawFeed `toml:"feeds"`
	Extract        ExtractConfig      `toml:"extract"`
	Orderbook      OrderbookConfig    `toml:"orderbook"`
	Telemetry      TelemetryConfig    `toml:"telemetry"`
	UI             UIConfig           `toml:"ui"`
	DaysTTLSeconds *float64           `toml:"days_ttl_seconds"`
}

func expandHome(path string) string {
	if path == "~" || strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, strings.TrimPrefix(path, "~"))
		}
	}
	return path
}

// LoadConfig reads the TOML file. Relative parser script paths are relative to the config file.
func LoadConfig(path string) (*Config, error) {
	path = expandHome(path)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseConfig(data, path)
}

// ParseConfig validates and parses config text. path is only used for error messages and to resolve a relative parser command.
func ParseConfig(data []byte, path string) (*Config, error) {
	var raw rawConfig
	if err := toml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	defaultCommand, err := parseCommand(raw.Parser.Command, path)
	if err != nil {
		return nil, err
	}
	defaultEnv := stringEnv(raw.Parser.Env)
	if raw.Remote.SSHOptions == nil {
		raw.Remote.SSHOptions = []string{"-o", "BatchMode=yes", "-o", "ServerAliveInterval=30"}
	}
	var feeds []*Feed
	if len(raw.Feeds) == 0 {
		if raw.Remote.PcapPath == "" {
			return nil, fmt.Errorf("%s: remote.pcap_path is required", path)
		}
		if len(defaultCommand) == 0 {
			return nil, fmt.Errorf("%s: parser.command is required", path)
		}
		feeds = []*Feed{{Remote: raw.Remote, Parser: ParserConfig{Command: defaultCommand, Env: defaultEnv}}}
	}
	for name, entry := range raw.Feeds {
		if !feedNamePattern.MatchString(name) || validKind(name) || strings.HasPrefix(name, "orderbook") {
			return nil, fmt.Errorf("%s: feeds.%s: the name must be lowercase letters, digits, _ or - (at most 40) and not a kind name", path, name)
		}
		feed := &Feed{Name: name, Remote: raw.Remote, Parser: ParserConfig{Command: defaultCommand, Env: map[string]string{}}}
		for key, value := range defaultEnv {
			feed.Parser.Env[key] = value
		}
		if entry.PcapPath != "" {
			feed.Remote.PcapPath = entry.PcapPath
		}
		if entry.SSHHost != nil {
			feed.Remote.SSHHost = *entry.SSHHost
		}
		if entry.SSHOptions != nil {
			feed.Remote.SSHOptions = entry.SSHOptions
		}
		command, err := parseCommand(entry.Parser.Command, path)
		if err != nil {
			return nil, err
		}
		if len(command) > 0 {
			feed.Parser.Command = command
		}
		for key, value := range stringEnv(entry.Parser.Env) {
			feed.Parser.Env[key] = value
		}
		if feed.Remote.PcapPath == "" {
			return nil, fmt.Errorf("%s: feeds.%s: pcap_path is required (here or in [remote])", path, name)
		}
		if len(feed.Parser.Command) == 0 {
			return nil, fmt.Errorf("%s: feeds.%s: parser.command is required (here or in [parser])", path, name)
		}
		feeds = append(feeds, feed)
	}
	sort.Slice(feeds, func(i, j int) bool { return feeds[i].Name < feeds[j].Name })
	for _, feed := range feeds {
		if !filepath.IsAbs(feed.Parser.Command[0]) {
			candidate := filepath.Join(filepath.Dir(path), expandHome(feed.Parser.Command[0]))
			if _, err := os.Stat(candidate); err == nil {
				feed.Parser.Command = append([]string{candidate}, feed.Parser.Command[1:]...)
			}
		}
	}
	config := &Config{Path: path, Raw: data, Telemetry: raw.Telemetry, UI: raw.UI, Remote: raw.Remote, CacheDir: expandHome(raw.Cache.Dir), Extract: raw.Extract, Orderbook: raw.Orderbook, DaysTTLSeconds: 600, Feeds: feeds}
	config.Cache = raw.Cache.CacheConfig
	config.Parser = ParserConfig{Command: defaultCommand, Env: defaultEnv}
	config.Remote = raw.Remote
	if config.CacheDir == "" {
		config.CacheDir = expandHome("~/.cache/datafeedfs")
	}
	if config.Extract.BatchWindowSeconds == 0 {
		config.Extract.BatchWindowSeconds = 3.0
	}
	if config.Extract.MaxParallel == 0 {
		config.Extract.MaxParallel = 2
	}
	if config.Extract.OnMiss == "" {
		config.Extract.OnMiss = "block"
	}
	if config.Extract.OnMiss != "block" && config.Extract.OnMiss != "async" {
		return nil, fmt.Errorf("%s: extract.on_miss must be \"block\" or \"async\"", path)
	}
	if config.Orderbook.Depth == 0 {
		config.Orderbook.Depth = defaultDepth
	}
	if config.Orderbook.Depth < 1 || config.Orderbook.Depth > maxDepth {
		return nil, fmt.Errorf("%s: orderbook.depth must be between 1 and %d", path, maxDepth)
	}
	if err := config.Telemetry.normalize(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := config.UI.normalize(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := config.Cache.normalize(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if raw.DaysTTLSeconds != nil {
		config.DaysTTLSeconds = *raw.DaysTTLSeconds
	}
	return config, nil
}

func parseCommand(value any, path string) ([]string, error) {
	var command []string
	switch value := value.(type) {
	case string:
		command = strings.Fields(value)
	case []any:
		for _, item := range value {
			command = append(command, fmt.Sprint(item))
		}
	case nil:
	default:
		return nil, fmt.Errorf("%s: parser.command must be a string or a list", path)
	}
	return command, nil
}

func stringEnv(raw map[string]any) map[string]string {
	env := map[string]string{}
	for key, value := range raw {
		env[key] = fmt.Sprint(value)
	}
	return env
}

// pattern is the pcap path with {feed} filled in; {yyyy} {mm} {dd} stay.
func (feed *Feed) pattern() string {
	return strings.ReplaceAll(feed.Remote.PcapPath, "{feed}", feed.Name)
}

// PcapFor is the pcap path of one day.
func (feed *Feed) PcapFor(day string) string {
	parts := strings.Split(day, "-")
	return strings.NewReplacer("{yyyy}", parts[0], "{mm}", parts[1], "{dd}", parts[2]).Replace(feed.pattern())
}

func (feed *Feed) DayGlob() string {
	return strings.NewReplacer("{yyyy}", "[0-9][0-9][0-9][0-9]", "{mm}", "[0-9][0-9]", "{dd}", "[0-9][0-9]").Replace(feed.pattern())
}

func (feed *Feed) DayRegex() *regexp.Regexp {
	pattern := regexp.QuoteMeta(feed.pattern())
	for name, digits := range map[string]int{"yyyy": 4, "mm": 2, "dd": 2} {
		pattern = strings.Replace(pattern, regexp.QuoteMeta("{"+name+"}"), fmt.Sprintf("(?P<%s>[0-9]{%d})", name, digits), 1)
	}
	return regexp.MustCompile("^" + pattern + "$")
}

func (cache *CacheConfig) normalize() error {
	if cache.MaxGB < 0 {
		return fmt.Errorf("cache.max_gb must not be negative")
	}
	if cache.EvictTo == 0 {
		cache.EvictTo = 0.9
	}
	if cache.EvictTo <= 0 || cache.EvictTo > 1 {
		return fmt.Errorf("cache.evict_to must be in (0, 1]")
	}
	if cache.ScanIntervalSeconds == 0 {
		cache.ScanIntervalSeconds = 600
	}
	if cache.TouchIntervalSeconds == 0 {
		cache.TouchIntervalSeconds = 3600
	}
	if cache.ScanIntervalSeconds < 0 || cache.TouchIntervalSeconds < 0 || cache.MinAgeSeconds < 0 {
		return fmt.Errorf("cache intervals must not be negative")
	}
	if cache.MinAgeSeconds < cache.TouchIntervalSeconds {
		cache.MinAgeSeconds = cache.TouchIntervalSeconds // a file read inside the touch window must not look idle
	}
	return nil
}

func (telemetry *TelemetryConfig) normalize() error {
	if telemetry.ServiceName == "" {
		telemetry.ServiceName = "datafeedfs"
	}
	if telemetry.MetricsIntervalSeconds == 0 {
		telemetry.MetricsIntervalSeconds = 15
	}
	if telemetry.MetricsIntervalSeconds < 0 {
		return fmt.Errorf("telemetry.metrics_interval_seconds must be positive")
	}
	if telemetry.Endpoint != "" {
		parsed, err := url.Parse(telemetry.Endpoint)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("telemetry.endpoint must be an http:// or https:// URL, for example http://collector:4318")
		}
	}
	return nil
}

func (telemetry *TelemetryConfig) metricsOn() bool {
	return telemetry.Enabled && (telemetry.Metrics == nil || *telemetry.Metrics)
}
func (telemetry *TelemetryConfig) tracesOn() bool {
	return telemetry.Enabled && (telemetry.Traces == nil || *telemetry.Traces)
}

func (ui *UIConfig) normalize() error {
	if ui.Listen == "" {
		return nil
	}
	host, _, err := net.SplitHostPort(ui.Listen)
	if err != nil {
		return fmt.Errorf("ui.listen must be host:port, for example 127.0.0.1:8088: %w", err)
	}
	if !isLoopbackHost(host) && ui.Token == "" {
		return fmt.Errorf("ui.listen %q is not a loopback address: set ui.token, the console shows the config and can change it", ui.Listen)
	}
	return nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
