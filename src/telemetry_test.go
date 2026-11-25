package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// newTestStore returns a store over a temp cache. Passes always fail (the pcap does not exist and the parser is `false`), which is enough to
// exercise every pass metric and span without real data.
func newTestStore(t *testing.T, extra string) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	text := fmt.Sprintf("[remote]\nssh_host = \"\"\npcap_path = %q\n[parser]\ncommand = \"false\"\n[cache]\ndir = %q\n[extract]\nbatch_window_seconds = 0.05\n%s",
		filepath.Join(dir, "{yyyy}", "{mm}", "{dd}", "missing.pcap.xz"), filepath.Join(dir, "cache"), extra)
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(config)
	if err != nil {
		t.Fatal(err)
	}
	return store, path
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	out := map[string]metricdata.Metrics{}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func sumBy(t *testing.T, m metricdata.Metrics, attr, value string) int64 {
	t.Helper()
	data, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s is not an int64 sum: %T", m.Name, m.Data)
	}
	var total int64
	for _, point := range data.DataPoints {
		if got, _ := point.Attributes.Value(attribute.Key(attr)); got.AsString() == value {
			total += point.Value
		}
	}
	return total
}

func TestInstrumentsAndPassSpans(t *testing.T) {
	store, _ := newTestStore(t, "")
	reader := sdkmetric.NewManualReader()
	recorder := tracetest.NewSpanRecorder()
	store.tel = newTelemetryFrom(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)), sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	store.tel.observe(store)

	hit := store.ParquetPath("", kindTick, "AAA", "2024-01-01")
	os.MkdirAll(filepath.Dir(hit), 0o755)
	os.WriteFile(hit, []byte("x"), 0o644)
	if _, err := store.Request("", kindTick, "AAA", "2024-01-01").Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Request("", kindTick, "AAA", "2024-01-02").Wait(); err == nil {
		t.Fatal("the pass over a missing pcap should fail")
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(recorder.Ended()) < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	metrics := collect(t, reader)
	if sumBy(t, metrics["datafeedfs.cache.requests"], "result", "hit") != 1 || sumBy(t, metrics["datafeedfs.cache.requests"], "result", "miss") != 1 {
		t.Fatalf("requests: %+v", metrics["datafeedfs.cache.requests"].Data)
	}
	if sumBy(t, metrics["datafeedfs.passes"], "status", "failed") != 1 || sumBy(t, metrics["datafeedfs.passes"], "kind", "tick") != 1 {
		t.Fatalf("passes: %+v", metrics["datafeedfs.passes"].Data)
	}
	for _, name := range []string{"datafeedfs.pass.duration", "datafeedfs.pass.symbols", "datafeedfs.passes.running", "datafeedfs.passes.pending", "datafeedfs.passes.failed", "datafeedfs.cache.limit"} {
		if _, ok := metrics[name]; !ok {
			t.Errorf("metric %s was not reported", name)
		}
	}
	if failed := metrics["datafeedfs.passes.failed"].Data.(metricdata.Gauge[int64]).DataPoints[0].Value; failed != 1 {
		t.Errorf("failed gauge = %d, want 1", failed)
	}
	if duration := metrics["datafeedfs.pass.duration"].Data.(metricdata.Histogram[float64]).DataPoints[0]; duration.Count != 1 {
		t.Errorf("pass.duration count = %d", duration.Count)
	}

	names := map[string]sdktrace.ReadOnlySpan{}
	for _, span := range recorder.Ended() {
		names[span.Name()] = span
	}
	pass, pipeline := names["pass"], names["pipeline"]
	if pass == nil || pipeline == nil {
		t.Fatalf("spans: %v", names)
	}
	if pass.Status().Code != codes.Error || pipeline.Parent().SpanID() != pass.SpanContext().SpanID() {
		t.Errorf("pass status %v, pipeline parent %v", pass.Status().Code, pipeline.Parent().SpanID())
	}
	attributes := map[string]string{}
	for _, kv := range pass.Attributes() {
		attributes[string(kv.Key)] = kv.Value.Emit()
	}
	if attributes["kind"] != "tick" || attributes["day"] != "2024-01-02" || attributes["status"] != "failed" || attributes["symbols.requested"] != "1" {
		t.Errorf("pass attributes: %v", attributes)
	}
	if len(store.history) != 1 || store.history[0].Status != "failed" || store.counters.hits.Load() != 1 || store.counters.misses.Load() != 1 {
		t.Errorf("history %+v counters %d/%d", store.history, store.counters.hits.Load(), store.counters.misses.Load())
	}
}

func TestEvictionIsCounted(t *testing.T) {
	store, _ := newTestStore(t, "")
	reader := sdkmetric.NewManualReader()
	store.tel = newTelemetryFrom(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)), sdktrace.NewTracerProvider())
	store.config.Cache.MaxGB, store.config.Cache.EvictTo, store.config.Cache.MinAgeSeconds = 1e-3, 0.5, 1
	store.evictPause = 0
	for i, symbol := range []string{"AAA", "BBB", "CCC"} {
		path := store.ParquetPath("", kindTick, symbol, "2024-01-02")
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, make([]byte, 600_000), 0o644)
		old := time.Now().Add(-time.Duration(10+i) * time.Hour)
		os.Chtimes(path, old, old)
	}
	files, bytes, _ := store.evictOnce()
	if files == 0 || bytes == 0 {
		t.Fatal("nothing evicted")
	}
	metrics := collect(t, reader)
	if got := metrics["datafeedfs.cache.evicted.files"].Data.(metricdata.Sum[int64]).DataPoints[0].Value; got != int64(files) {
		t.Errorf("evicted.files = %d, want %d", got, files)
	}
	if store.counters.evictedBytes.Load() != bytes {
		t.Errorf("evicted bytes counter %d, want %d", store.counters.evictedBytes.Load(), bytes)
	}
	if stats := store.cacheStats.Load(); stats == nil || stats.ByKind[kindTick].Files != 3-files {
		t.Errorf("cache stats after eviction: %+v", stats)
	}
}

// A real OTLP/HTTP round trip: a fake collector receives the traces and metrics the exporters send on shutdown.
func TestOTLPExport(t *testing.T) {
	var mutex sync.Mutex
	bodies := map[string][]byte{}
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		mutex.Lock()
		bodies[r.URL.Path] = append(bodies[r.URL.Path], data...)
		mutex.Unlock()
		if ct := r.Header.Get("Content-Type"); ct != "application/x-protobuf" {
			t.Errorf("content type %q", ct)
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer collector.Close()

	store, _ := newTestStore(t, fmt.Sprintf("[telemetry]\nenabled = true\nendpoint = %q\nservice_name = \"datafeedfs-test\"\nmetrics_interval_seconds = 0.2\n", collector.URL))
	if _, err := store.Request("", kindOrderbook, "AAA", "2024-01-02").Wait(); err == nil {
		t.Fatal("the pass should fail")
	}
	store.tel.Shutdown(10 * time.Second)

	mutex.Lock()
	defer mutex.Unlock()
	var traces tracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(bodies["/v1/traces"], &traces); err != nil || len(traces.ResourceSpans) == 0 {
		t.Fatalf("no trace export (%v), paths: %v", err, keys(bodies))
	}
	var spans []string
	for _, resource := range traces.ResourceSpans {
		service := ""
		for _, kv := range resource.Resource.Attributes {
			if kv.Key == "service.name" {
				service = kv.Value.GetStringValue()
			}
		}
		if service != "datafeedfs-test" {
			t.Errorf("service.name = %q", service)
		}
		for _, scope := range resource.ScopeSpans {
			for _, span := range scope.Spans {
				spans = append(spans, span.Name)
			}
		}
	}
	if !strings.Contains(strings.Join(spans, ","), "pass") || !strings.Contains(strings.Join(spans, ","), "pipeline") {
		t.Errorf("exported spans: %v", spans)
	}
	var metrics metricspb.ExportMetricsServiceRequest
	if err := proto.Unmarshal(bodies["/v1/metrics"], &metrics); err != nil || len(metrics.ResourceMetrics) == 0 {
		t.Fatalf("no metric export (%v), paths: %v", err, keys(bodies))
	}
	found := map[string]bool{}
	for _, resource := range metrics.ResourceMetrics {
		for _, scope := range resource.ScopeMetrics {
			for _, m := range scope.Metrics {
				found[m.Name] = true
			}
		}
	}
	for _, name := range []string{"datafeedfs.passes", "datafeedfs.pass.duration", "datafeedfs.cache.requests", "datafeedfs.passes.failed"} {
		if !found[name] {
			t.Errorf("metric %s missing from the export: %v", name, found)
		}
	}
}

func keys(m map[string][]byte) []string {
	var out []string
	for key := range m {
		out = append(out, key)
	}
	return out
}

func TestTelemetryOffIsInert(t *testing.T) {
	store, _ := newTestStore(t, "")
	if len(store.tel.shutdown) != 0 {
		t.Fatal("providers were created with telemetry disabled")
	}
	store.Request("", kindTick, "AAA", "2024-01-02").Wait() // every call is a no-op and must not panic
	store.tel.Shutdown(time.Second)
}

func TestTelemetryAndUIConfigValidation(t *testing.T) {
	base := "[remote]\npcap_path = \"/x/{yyyy}/{mm}/{dd}\"\n[parser]\ncommand = \"true\"\n"
	for text, want := range map[string]string{
		base + "[telemetry]\nendpoint = \"collector:4318\"\n": "telemetry.endpoint",
		base + "[telemetry]\nmetrics_interval_seconds = -1\n": "metrics_interval_seconds",
		base + "[ui]\nlisten = \"0.0.0.0:8088\"\n":            "ui.token",
		base + "[ui]\nlisten = \"8088\"\n":                    "host:port",
		base + "[cache]\nevict_to = 5\n":                      "evict_to",
	} {
		if _, err := ParseConfig([]byte(text), "c.toml"); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: error %v, want mention of %s", text[len(base):], err, want)
		}
	}
	for _, ok := range []string{"[ui]\nlisten = \"127.0.0.1:8088\"\n", "[ui]\nlisten = \"0.0.0.0:8088\"\ntoken = \"s\"\n", "[telemetry]\nenabled = true\nendpoint = \"http://collector:4318\"\n"} {
		if _, err := ParseConfig([]byte(base+ok), "c.toml"); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
}
