package main

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Sampler keeps the last hour of host and process measurements for the web console, one sample every sampleStep. It reads /proc, so on other
// platforms the host figures are zero and Supported is false. In a container /proc/stat, /proc/loadavg and /proc/meminfo are the host's,
// /proc/net/dev is the container's, and the process list is the container's.
const (
	sampleStep    = 10 * time.Second
	sampleKeep    = 360
	clockTicks    = 100 // USER_HZ, 100 on every Linux the program runs on
	processGroups = "xz,itch_tob,datafeedfs-go,ssh"
)

var groupNames = strings.Split(processGroups, ",")

type Sample struct {
	At         time.Time
	NetMBs     float64 // bytes received on all non-loopback interfaces
	CacheMBs   float64 // parquet written into the cache by passes
	CPUCores   float64 // busy cores, host wide
	Groups     map[string]float64
	Load1      float64
	Load5      float64
	Load15     float64
	MemUsedGiB float64
	HitRatio   *float64 // cache hits / requests in this step; nil when there were no requests
}

type ProcessInfo struct {
	PID     int
	Name    string
	Command string
	CPU     float64 // cores
	RSSMB   float64
}

type HostInfo struct {
	CPUs        int
	MemTotalGiB float64
	DiskTotal   uint64
	DiskFree    uint64
	Supported   bool
}

type Sampler struct {
	store *Store

	mutex     sync.Mutex
	samples   []Sample
	processes []ProcessInfo
	host      HostInfo

	last struct {
		at                    time.Time
		busy, total           uint64
		netRx                 uint64
		written, hits, misses int64
	}

	// Per-process CPU is accumulated by poll every second, so a process that lives for two seconds is still counted in the 10 s sample
	// (comparing two readings 10 s apart would never see it).
	accMutex  sync.Mutex
	prevCPU   map[int]uint64 // pid -> utime+stime at the last poll; nil before the first poll
	accGroups map[string]float64
	accProcs  map[int]*procAcc
}

type procAcc struct {
	name, command string
	seconds       float64
	rssBytes      float64
}

const pollStep = time.Second

func newSampler(store *Store) *Sampler {
	sampler := &Sampler{store: store}
	sampler.host.CPUs = runtime.NumCPU()
	go sampler.run()
	return sampler
}

func (sampler *Sampler) run() {
	sampler.poll()
	sampler.sample()
	go func() {
		for range time.Tick(pollStep) {
			sampler.poll()
		}
	}()
	for range time.Tick(sampleStep) {
		sampler.sample()
	}
}

func groupOf(name string) string {
	for _, group := range groupNames {
		if strings.HasPrefix(name, group) {
			return group
		}
	}
	return ""
}

// poll adds the CPU time every process used since the previous poll to the accumulators. A process first seen now started since the
// previous poll, so all of its CPU time is new; a process that exited between two polls loses at most its last second.
func (sampler *Sampler) poll() {
	processes := processTimes()
	sampler.accMutex.Lock()
	defer sampler.accMutex.Unlock()
	if sampler.accGroups == nil {
		sampler.accGroups, sampler.accProcs = map[string]float64{}, map[int]*procAcc{}
	}
	if sampler.prevCPU != nil {
		for pid, info := range processes {
			delta := info.cpu
			if before, seen := sampler.prevCPU[pid]; seen {
				if info.cpu < before {
					continue
				}
				delta = info.cpu - before
			}
			if delta == 0 {
				continue
			}
			seconds := float64(delta) / clockTicks
			if group := groupOf(info.name); group != "" {
				sampler.accGroups[group] += seconds
			}
			entry := sampler.accProcs[pid]
			if entry == nil {
				command := strings.ReplaceAll(strings.TrimSpace(string(readProc("/proc/"+strconv.Itoa(pid)+"/cmdline"))), "\x00", " ")
				if len(command) > 70 {
					command = command[:70]
				}
				entry = &procAcc{name: info.name, command: command}
				sampler.accProcs[pid] = entry
			}
			entry.seconds += seconds
			entry.rssBytes = float64(info.rss) * float64(os.Getpagesize())
		}
	}
	sampler.prevCPU = make(map[int]uint64, len(processes))
	for pid, info := range processes {
		sampler.prevCPU[pid] = info.cpu
	}
}

func readProc(path string) []byte {
	data, _ := os.ReadFile(path)
	return data
}

func parseUints(fields []string) []uint64 {
	out := make([]uint64, len(fields))
	for i, field := range fields {
		out[i], _ = strconv.ParseUint(field, 10, 64)
	}
	return out
}

// cpuTimes returns the busy and total jiffies of the first line of /proc/stat.
func cpuTimes() (busy, total uint64, ok bool) {
	data := readProc("/proc/stat")
	line, _, _ := bytes.Cut(data, []byte("\n"))
	fields := strings.Fields(string(line))
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, 0, false
	}
	values := parseUints(fields[1:])
	for _, value := range values {
		total += value
	}
	idle := values[3]
	if len(values) > 4 {
		idle += values[4] // iowait
	}
	return total - idle, total, true
}

func netReceived() uint64 {
	var total uint64
	for _, line := range strings.Split(string(readProc("/proc/net/dev")), "\n") {
		name, rest, found := strings.Cut(line, ":")
		name = strings.TrimSpace(name)
		if !found || name == "lo" {
			continue
		}
		if fields := strings.Fields(rest); len(fields) > 0 {
			value, _ := strconv.ParseUint(fields[0], 10, 64)
			total += value
		}
	}
	return total
}

func memory() (totalGiB, usedGiB float64) {
	var total, available float64
	for _, line := range strings.Split(string(readProc("/proc/meminfo")), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, _ := strconv.ParseFloat(fields[1], 64)
		switch fields[0] {
		case "MemTotal:":
			total = value
		case "MemAvailable:":
			available = value
		}
	}
	return total / (1 << 20), (total - available) / (1 << 20)
}

// processTimes returns, per pid, the name, utime+stime and resident pages of every process.
func processTimes() map[int]struct {
	name string
	cpu  uint64
	rss  uint64
} {
	out := map[int]struct {
		name string
		cpu  uint64
		rss  uint64
	}{}
	entries, _ := os.ReadDir("/proc")
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		data := readProc(filepath.Join("/proc", entry.Name(), "stat"))
		open, closing := bytes.IndexByte(data, '('), bytes.LastIndexByte(data, ')')
		if open < 0 || closing < open {
			continue
		}
		fields := strings.Fields(string(data[closing+1:])) // state is fields[0]; utime is field 14 of the full line = fields[11]
		if len(fields) < 22 {
			continue
		}
		values := parseUints(fields)
		out[pid] = struct {
			name string
			cpu  uint64
			rss  uint64
		}{string(data[open+1 : closing]), values[11] + values[12], values[21]}
	}
	return out
}

func (sampler *Sampler) sample() {
	now := time.Now()
	busy, total, supported := cpuTimes()
	netRx := netReceived()
	totalGiB, usedGiB := memory()
	written, hits, misses := sampler.store.counters.written.Load(), sampler.store.counters.hits.Load(), sampler.store.counters.misses.Load()
	load := strings.Fields(string(readProc("/proc/loadavg")))

	sampler.mutex.Lock()
	defer sampler.mutex.Unlock()
	sampler.host.MemTotalGiB, sampler.host.Supported = totalGiB, supported
	var stat syscall.Statfs_t
	if syscall.Statfs(sampler.store.cache, &stat) == nil {
		sampler.host.DiskTotal, sampler.host.DiskFree = stat.Blocks*uint64(stat.Bsize), stat.Bavail*uint64(stat.Bsize)
	}
	previous := sampler.last
	sampler.last.at, sampler.last.busy, sampler.last.total, sampler.last.netRx = now, busy, total, netRx
	sampler.last.written, sampler.last.hits, sampler.last.misses = written, hits, misses
	sampler.accMutex.Lock()
	groupSeconds, procs := sampler.accGroups, sampler.accProcs
	sampler.accGroups, sampler.accProcs = map[string]float64{}, map[int]*procAcc{}
	sampler.accMutex.Unlock()
	if previous.at.IsZero() {
		return // rates need two readings
	}
	elapsed := now.Sub(previous.at).Seconds()
	sample := Sample{At: now, Groups: map[string]float64{}}
	if total > previous.total {
		sample.CPUCores = float64(busy-previous.busy) / float64(total-previous.total) * float64(sampler.host.CPUs)
	}
	if netRx >= previous.netRx {
		sample.NetMBs = float64(netRx-previous.netRx) / elapsed / 1e6
	}
	sample.CacheMBs = float64(written-previous.written) / elapsed / 1e6
	if requests := (hits - previous.hits) + (misses - previous.misses); requests > 0 {
		ratio := float64(hits-previous.hits) / float64(requests) * 100
		sample.HitRatio = &ratio
	}
	if len(load) >= 3 {
		sample.Load1, _ = strconv.ParseFloat(load[0], 64)
		sample.Load5, _ = strconv.ParseFloat(load[1], 64)
		sample.Load15, _ = strconv.ParseFloat(load[2], 64)
	}
	sample.MemUsedGiB = usedGiB

	named := 0.0
	for _, group := range groupNames {
		cores := groupSeconds[group] / elapsed
		sample.Groups[group] = cores
		named += cores
	}
	other := sample.CPUCores - named
	if other < 0 {
		other = 0
	}
	sample.Groups["other"] = other
	var top []ProcessInfo
	for pid, entry := range procs {
		if cores := entry.seconds / elapsed; cores > 0.005 {
			top = append(top, ProcessInfo{PID: pid, Name: entry.name, Command: entry.command, CPU: cores, RSSMB: entry.rssBytes / 1e6})
		}
	}
	sort.Slice(top, func(i, j int) bool { return top[i].CPU > top[j].CPU })
	if len(top) > 8 {
		top = top[:8]
	}
	sampler.processes = top
	sampler.samples = append(sampler.samples, sample)
	if len(sampler.samples) > sampleKeep {
		sampler.samples = sampler.samples[len(sampler.samples)-sampleKeep:]
	}
}

// Snapshot copies what the console needs.
func (sampler *Sampler) Snapshot() (samples []Sample, processes []ProcessInfo, host HostInfo) {
	sampler.mutex.Lock()
	defer sampler.mutex.Unlock()
	return append([]Sample(nil), sampler.samples...), append([]ProcessInfo(nil), sampler.processes...), sampler.host
}
