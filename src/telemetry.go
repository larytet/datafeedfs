package main

import (
	"context"
	"log"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Telemetry holds the OpenTelemetry instruments. With [telemetry] enabled = false (the default) the providers are no-ops, so every call below
// is cheap and nothing is exported. Providers are local to the Store, never the process-wide globals.
//
// Metrics (meter "datafeedfs"):
//
//	datafeedfs.cache.requests          counter   {request}  kind, result = hit | miss | joined (a miss already being extracted)
//	datafeedfs.open.wait               histogram s          kind, result = hit | miss: how long open() waited
//	datafeedfs.pass.duration           histogram s          kind, status = ok | failed
//	datafeedfs.pass.symbols            histogram {symbol}   kind: symbols requested in a pass
//	datafeedfs.passes                  counter   {pass}     kind, status
//	datafeedfs.cache.evicted.files     counter   {file}
//	datafeedfs.cache.evicted.bytes     counter   By
//	datafeedfs.passes.running          gauge     {pass}
//	datafeedfs.passes.pending          gauge     {pass}     passes waiting for a slot
//	datafeedfs.passes.failed           gauge     {file}     files whose last pass failed
//	datafeedfs.cache.bytes             gauge     By         kind, format (parquet|csv); refreshed by the cache scan
//	datafeedfs.cache.files             gauge     {file}     kind, format
//	datafeedfs.cache.limit             gauge     By         cache.max_gb, 0 = unlimited
//
// Traces (tracer "datafeedfs"): one span "pass" per extraction pass (kind, day, requested and extracted symbols, status) with a child span
// "pipeline" around ssh cat | decompress | decoder | ingest.
type Telemetry struct {
	meter  metric.Meter
	tracer trace.Tracer

	requests     metric.Int64Counter
	openWait     metric.Float64Histogram
	passDuration metric.Float64Histogram
	passSymbols  metric.Int64Histogram
	passes       metric.Int64Counter
	evictedFiles metric.Int64Counter
	evictedBytes metric.Int64Counter

	shutdown []func(context.Context) error
}

func newTelemetry(config *Config) *Telemetry {
	settings := config.Telemetry
	var meters metric.MeterProvider = metricnoop.NewMeterProvider()
	var tracers trace.TracerProvider = tracenoop.NewTracerProvider()
	var shutdown []func(context.Context) error
	if settings.Enabled {
		resources, err := resource.Merge(resource.Default(), resource.NewSchemaless(attribute.String("service.name", settings.ServiceName)))
		if err != nil {
			log.Printf("telemetry: resource: %v", err)
			resources = resource.Default()
		}
		if settings.metricsOn() {
			options := []otlpmetrichttp.Option{}
			if settings.Endpoint != "" {
				parsed, _ := url.Parse(settings.Endpoint) // validated in config
				options = append(options, otlpmetrichttp.WithEndpoint(parsed.Host))
				if parsed.Scheme == "http" {
					options = append(options, otlpmetrichttp.WithInsecure())
				}
				if path := strings.TrimRight(parsed.Path, "/"); path != "" {
					options = append(options, otlpmetrichttp.WithURLPath(path+"/v1/metrics"))
				}
			}
			if exporter, err := otlpmetrichttp.New(context.Background(), options...); err != nil {
				log.Printf("telemetry: metrics exporter: %v", err)
			} else {
				reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Duration(settings.MetricsIntervalSeconds*float64(time.Second))))
				provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithResource(resources))
				meters, shutdown = provider, append(shutdown, provider.Shutdown)
			}
		}
		if settings.tracesOn() {
			options := []otlptracehttp.Option{}
			if settings.Endpoint != "" {
				parsed, _ := url.Parse(settings.Endpoint)
				options = append(options, otlptracehttp.WithEndpoint(parsed.Host))
				if parsed.Scheme == "http" {
					options = append(options, otlptracehttp.WithInsecure())
				}
				if path := strings.TrimRight(parsed.Path, "/"); path != "" {
					options = append(options, otlptracehttp.WithURLPath(path+"/v1/traces"))
				}
			}
			if exporter, err := otlptracehttp.New(context.Background(), options...); err != nil {
				log.Printf("telemetry: trace exporter: %v", err)
			} else {
				provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)), sdktrace.WithResource(resources), sdktrace.WithSampler(sdktrace.AlwaysSample()))
				tracers, shutdown = provider, append(shutdown, provider.Shutdown)
			}
		}
		log.Printf("telemetry: exporting to %s (metrics %v, traces %v)", firstNonEmpty(settings.Endpoint, "the OTEL_EXPORTER_OTLP_* default"), settings.metricsOn(), settings.tracesOn())
	}
	telemetry := newTelemetryFrom(meters, tracers)
	telemetry.shutdown = shutdown
	return telemetry
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// newTelemetryFrom builds the instruments on the given providers (tests pass in-memory ones).
func newTelemetryFrom(meters metric.MeterProvider, tracers trace.TracerProvider) *Telemetry {
	meter := meters.Meter("datafeedfs")
	telemetry := &Telemetry{meter: meter, tracer: tracers.Tracer("datafeedfs")}
	telemetry.requests, _ = meter.Int64Counter("datafeedfs.cache.requests", metric.WithUnit("{request}"), metric.WithDescription("Requests for a cached file by outcome: hit, miss, or joined (a miss already being extracted)"))
	telemetry.openWait, _ = meter.Float64Histogram("datafeedfs.open.wait", metric.WithUnit("s"), metric.WithDescription("Time open() waited for the file"), metric.WithExplicitBucketBoundaries(0.001, 0.01, 0.1, 1, 10, 60, 300, 900, 1800, 3600, 7200))
	telemetry.passDuration, _ = meter.Float64Histogram("datafeedfs.pass.duration", metric.WithUnit("s"), metric.WithDescription("Duration of an extraction pass over one day's pcap"), metric.WithExplicitBucketBoundaries(1, 5, 30, 120, 600, 1800, 3600, 5400, 7200, 14400))
	telemetry.passSymbols, _ = meter.Int64Histogram("datafeedfs.pass.symbols", metric.WithUnit("{symbol}"), metric.WithDescription("Symbols requested in a pass"), metric.WithExplicitBucketBoundaries(1, 2, 5, 10, 50, 100, 500, 1000))
	telemetry.passes, _ = meter.Int64Counter("datafeedfs.passes", metric.WithUnit("{pass}"), metric.WithDescription("Completed extraction passes"))
	telemetry.evictedFiles, _ = meter.Int64Counter("datafeedfs.cache.evicted.files", metric.WithUnit("{file}"), metric.WithDescription("Cached files evicted by the size cap"))
	telemetry.evictedBytes, _ = meter.Int64Counter("datafeedfs.cache.evicted.bytes", metric.WithUnit("By"), metric.WithDescription("Bytes of cached files evicted by the size cap"))
	return telemetry
}

// observe registers the gauges, read from the store when the SDK collects.
func (telemetry *Telemetry) observe(store *Store) {
	running, _ := telemetry.meter.Int64ObservableGauge("datafeedfs.passes.running", metric.WithUnit("{pass}"), metric.WithDescription("Extraction passes running"))
	pending, _ := telemetry.meter.Int64ObservableGauge("datafeedfs.passes.pending", metric.WithUnit("{pass}"), metric.WithDescription("Passes waiting for a slot or for the batch window"))
	failed, _ := telemetry.meter.Int64ObservableGauge("datafeedfs.passes.failed", metric.WithUnit("{file}"), metric.WithDescription("Files whose last extraction failed"))
	bytes, _ := telemetry.meter.Int64ObservableGauge("datafeedfs.cache.bytes", metric.WithUnit("By"), metric.WithDescription("Size of cached files, as of the last cache scan"))
	files, _ := telemetry.meter.Int64ObservableGauge("datafeedfs.cache.files", metric.WithUnit("{file}"), metric.WithDescription("Number of cached files, as of the last cache scan"))
	limit, _ := telemetry.meter.Int64ObservableGauge("datafeedfs.cache.limit", metric.WithUnit("By"), metric.WithDescription("cache.max_gb in bytes, 0 = unlimited"))
	_, err := telemetry.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		store.mutex.Lock()
		observer.ObserveInt64(running, int64(len(store.runningDays)))
		observer.ObserveInt64(pending, int64(len(store.pending)))
		observer.ObserveInt64(failed, int64(len(store.failures)))
		store.mutex.Unlock()
		observer.ObserveInt64(limit, int64(store.config.Cache.MaxGB*(1<<30)))
		if stats := store.cacheStats.Load(); stats != nil {
			for kind, stat := range stats.ByKind {
				feed, kindName := "", kind
				if store.config.Named() {
					feed, kindName, _ = strings.Cut(kind, "/")
				}
				parquet := feedAttributes(feed, kindName, attribute.String("format", "parquet"))
				observer.ObserveInt64(bytes, stat.Bytes-stat.CSVBytes, parquet)
				observer.ObserveInt64(files, int64(stat.Files-stat.CSVFiles), parquet)
				csv := feedAttributes(feed, kindName, attribute.String("format", "csv"))
				observer.ObserveInt64(bytes, stat.CSVBytes, csv)
				observer.ObserveInt64(files, int64(stat.CSVFiles), csv)
			}
		}
		return nil
	}, running, pending, failed, bytes, files, limit)
	if err != nil {
		log.Printf("telemetry: gauges: %v", err)
	}
}

func kindAttribute(kind string) attribute.KeyValue { return attribute.String("kind", kind) }

// feedAttributes are the kind attribute of an instrument, plus the feed when feeds are named.
func feedAttributes(feed, kind string, more ...attribute.KeyValue) metric.MeasurementOption {
	attributes := []attribute.KeyValue{kindAttribute(kind)}
	if feed != "" {
		attributes = append(attributes, attribute.String("feed", feed))
	}
	return metric.WithAttributes(append(attributes, more...)...)
}

func (telemetry *Telemetry) request(feed, kind, result string) {
	telemetry.requests.Add(context.Background(), 1, feedAttributes(feed, kind, attribute.String("result", result)))
}

func (telemetry *Telemetry) open(feed, kind string, hit bool, waited time.Duration) {
	result := "miss"
	if hit {
		result = "hit"
	}
	telemetry.openWait.Record(context.Background(), waited.Seconds(), feedAttributes(feed, kind, attribute.String("result", result)))
}

func (telemetry *Telemetry) evicted(files int, bytes int64) {
	telemetry.evictedFiles.Add(context.Background(), int64(files))
	telemetry.evictedBytes.Add(context.Background(), bytes)
}

// startPass opens the span of a pass; finish ends it and records the metrics.
func (telemetry *Telemetry) startPass(feed, kind, day string, requested int) (context.Context, trace.Span) {
	return telemetry.tracer.Start(context.Background(), "pass", trace.WithAttributes(attribute.String("feed", feed), kindAttribute(kind), attribute.String("day", day), attribute.Int("symbols.requested", requested)))
}

func (telemetry *Telemetry) startPipeline(parent context.Context, extracted int, wholeDay bool) trace.Span {
	_, span := telemetry.tracer.Start(parent, "pipeline", trace.WithAttributes(attribute.Int("symbols.extracted", extracted), attribute.Bool("whole_day", wholeDay)))
	return span
}

func (telemetry *Telemetry) finishPass(span trace.Span, feed, kind string, requested int, took time.Duration, err error) {
	status := "ok"
	if err != nil {
		status = "failed"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.SetAttributes(attribute.String("status", status))
	span.End()
	attributes := feedAttributes(feed, kind, attribute.String("status", status))
	telemetry.passDuration.Record(context.Background(), took.Seconds(), attributes)
	telemetry.passes.Add(context.Background(), 1, attributes)
	telemetry.passSymbols.Record(context.Background(), int64(requested), feedAttributes(feed, kind))
}

// Shutdown flushes pending metrics and spans.
func (telemetry *Telemetry) Shutdown(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for _, shutdown := range telemetry.shutdown {
		if err := shutdown(ctx); err != nil {
			log.Printf("telemetry: shutdown: %v", err)
		}
	}
}
