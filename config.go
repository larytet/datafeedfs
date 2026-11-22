package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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

type ExtractConfig struct {
	BatchWindowSeconds float64 `toml:"batch_window_seconds"` // requests for one day inside this window share a pass over the pcap
	MaxParallel        int     `toml:"max_parallel"`         // passes over different days at once
	OnMiss             string  `toml:"on_miss"`              // "block" or "async" (open fails with EAGAIN, extraction continues)
	WholeDay           bool    `toml:"whole_day"`            // a miss extracts every symbol of that day in the same pass: the pcap is read once per day
}

type Config struct {
	Remote         RemoteConfig
	Parser         ParserConfig
	CacheDir       string
	Extract        ExtractConfig
	DaysTTLSeconds float64
}

type rawConfig struct {
	Remote RemoteConfig `toml:"remote"`
	Parser struct {
		Command any            `toml:"command"`
		Env     map[string]any `toml:"env"`
	} `toml:"parser"`
	Cache struct {
		Dir string `toml:"dir"`
	} `toml:"cache"`
	Extract        ExtractConfig `toml:"extract"`
	DaysTTLSeconds *float64      `toml:"days_ttl_seconds"`
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
	var raw rawConfig
	if err := toml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if raw.Remote.PcapPath == "" {
		return nil, fmt.Errorf("%s: remote.pcap_path is required", path)
	}
	var command []string
	switch value := raw.Parser.Command.(type) {
	case string:
		command = strings.Fields(value)
	case []any:
		for _, item := range value {
			command = append(command, fmt.Sprint(item))
		}
	}
	if len(command) == 0 {
		return nil, fmt.Errorf("%s: parser.command is required", path)
	}
	if !filepath.IsAbs(command[0]) {
		candidate := filepath.Join(filepath.Dir(path), expandHome(command[0]))
		if _, err := os.Stat(candidate); err == nil {
			command[0] = candidate
		}
	}
	config := &Config{Remote: raw.Remote, CacheDir: expandHome(raw.Cache.Dir), Extract: raw.Extract, DaysTTLSeconds: 600}
	config.Parser = ParserConfig{Command: command, Env: map[string]string{}}
	for key, value := range raw.Parser.Env {
		config.Parser.Env[key] = fmt.Sprint(value)
	}
	if config.Remote.SSHOptions == nil {
		config.Remote.SSHOptions = []string{"-o", "BatchMode=yes", "-o", "ServerAliveInterval=30"}
	}
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
	if raw.DaysTTLSeconds != nil {
		config.DaysTTLSeconds = *raw.DaysTTLSeconds
	}
	return config, nil
}

func (config *Config) PcapFor(day string) string {
	parts := strings.Split(day, "-")
	return strings.NewReplacer("{yyyy}", parts[0], "{mm}", parts[1], "{dd}", parts[2]).Replace(config.Remote.PcapPath)
}

func (config *Config) DayGlob() string {
	return strings.NewReplacer("{yyyy}", "[0-9][0-9][0-9][0-9]", "{mm}", "[0-9][0-9]", "{dd}", "[0-9][0-9]").Replace(config.Remote.PcapPath)
}

func (config *Config) DayRegex() *regexp.Regexp {
	pattern := regexp.QuoteMeta(config.Remote.PcapPath)
	for name, digits := range map[string]int{"yyyy": 4, "mm": 2, "dd": 2} {
		pattern = strings.Replace(pattern, regexp.QuoteMeta("{"+name+"}"), fmt.Sprintf("(?P<%s>[0-9]{%d})", name, digits), 1)
	}
	return regexp.MustCompile("^" + pattern + "$")
}
