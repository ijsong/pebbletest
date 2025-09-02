package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	dbDir        string
	valueSize    uint64
	batchLength  uint
	testDuration time.Duration

	useSync                             bool
	walBytesPerSync                     int
	walMinSyncInterval                  time.Duration
	sstBytesPerSync                     int
	memTableSize                        uint64
	memTableStopWritesThreshold         int
	maxConcurrentCompactions            int
	l0TargetFileSize                    int64
	l0CompactionConcurrency             int
	lbaseMaxBytes                       int64
	enableValueBlocks                   bool
	enableMultiLevelCompactionHeuristic bool

	verbose            bool
	metricsLogInterval time.Duration
	otelAddr           string
)

func init() {
	flag.StringVar(&dbDir, "db-dir", os.Getenv("DB_DIR"), "Directory for the Pebble database")
	flag.Uint64Var(&valueSize, "value-size", 1024, "Size of each value in bytes")
	flag.DurationVar(&testDuration, "test-duration", 10*time.Minute, "Duration of the test run (default: 10 minutes)")
	flag.UintVar(&batchLength, "batch-length", 1, "Number of entries per batch")

	flag.BoolVar(&useSync, "sync", false, "Use synchronous writes")
	flag.IntVar(&walBytesPerSync, "wal-bytes-per-sync", 0, "WAL bytes per sync (default: 0, means no background syncing)")
	flag.DurationVar(&walMinSyncInterval, "wal-min-sync-interval", 0, "Minimum interval between WAL syncs (default: 0)")
	flag.IntVar(&sstBytesPerSync, "sst-bytes-per-sync", 512<<10, "SST bytes per sync (default: 512 KB)")
	flag.Uint64Var(&memTableSize, "memtable-size", 64<<20, "MemTable size in bytes (default: 64 MB)")
	flag.IntVar(&memTableStopWritesThreshold, "memtable-stop-writes-threshold", 4, "MemTable stop writes threshold (default: 4)")
	flag.IntVar(&maxConcurrentCompactions, "max-concurrent-compactions", 3, "Maximum number of concurrent compactions (default: 3)")
	flag.Int64Var(&l0TargetFileSize, "l0-target-file-size", 2<<20, "L0 target file size in bytes (default: 2 MB)")
	flag.IntVar(&l0CompactionConcurrency, "l0-compaction-concurrency", 10, "L0 compaction concurrency (default: 10)")
	flag.Int64Var(&lbaseMaxBytes, "lbase-max-bytes", 64<<20, "LBase max bytes in bytes (default: 64 MB)")
	flag.BoolVar(&enableValueBlocks, "enable-value-blocks", false, "Enable value blocks (default: false)")
	flag.BoolVar(&enableMultiLevelCompactionHeuristic, "enable-multi-level-compaction-heuristic", false, "Enable multi-level compaction heuristic (default: false)")

	flag.BoolVar(&verbose, "verbose", false, "Enable verbose logging")
	flag.DurationVar(&metricsLogInterval, "metrics-log-interval", 3*time.Second, "Interval for logging database metrics (default: 3s)")
	flag.StringVar(&otelAddr, "otel-addr", os.Getenv("OTEL_ADDR"), "OpenTelemetry collector address (optional)")

	flag.Parse()
}

func main() {
	if dbDir == "" {
		slog.Error("db-dir is required")
		os.Exit(1)
	}

	var confTag strings.Builder
	fmt.Fprintf(&confTag, "vs=%v", valueSize)
	fmt.Fprintf(&confTag, "/bl=%v", batchLength)
	fmt.Fprintf(&confTag, "/sw=%v", useSync)
	fmt.Fprintf(&confTag, "/wbps=%v", walBytesPerSync)
	fmt.Fprintf(&confTag, "/wmsi=%v", walMinSyncInterval)
	fmt.Fprintf(&confTag, "/sbps=%v", sstBytesPerSync)
	fmt.Fprintf(&confTag, "/mts=%v", memTableSize)
	fmt.Fprintf(&confTag, "/mtst=%v", memTableStopWritesThreshold)
	fmt.Fprintf(&confTag, "/mcc=%v", maxConcurrentCompactions)
	fmt.Fprintf(&confTag, "/l0tfs=%v", l0TargetFileSize)
	fmt.Fprintf(&confTag, "/l0cc=%v", l0CompactionConcurrency)
	fmt.Fprintf(&confTag, "/lmb=%v", lbaseMaxBytes)
	fmt.Fprintf(&confTag, "/evb=%v", enableValueBlocks)
	fmt.Fprintf(&confTag, "/emlch=%v", enableMultiLevelCompactionHeuristic)

	fields := []any{
		slog.Uint64("value_size", valueSize),
		slog.Uint64("batch_length", uint64(batchLength)),
		slog.Bool("use_sync", useSync),
		slog.Int("wal_bytes_per_sync", walBytesPerSync),
		slog.Duration("wal_min_sync_interval", walMinSyncInterval),
		slog.Int("sst_bytes_per_sync", sstBytesPerSync),
		slog.Uint64("memtable_size", memTableSize),
		slog.Int("memtable_stop_writes_threshold", memTableStopWritesThreshold),
		slog.Int("max_concurrent_compactions", maxConcurrentCompactions),
		slog.Int64("l0_target_file_size", l0TargetFileSize),
		slog.Int("l0_compaction_concurrency", l0CompactionConcurrency),
		slog.Int64("lbase_max_bytes", lbaseMaxBytes),
		slog.Bool("enable_value_blocks", enableValueBlocks),
		slog.Bool("enable_multi_level_compaction_heuristic", enableMultiLevelCompactionHeuristic),
	}
	slog.Info("Starting Pebble test", fields...)
	defer func() {
		slog.Info("Pebble test completed", fields...)
	}()

	closeMetricProvider := startTelemetry()
	defer closeMetricProvider()

	meter := otel.Meter("pebbletest",
		metric.WithInstrumentationAttributes(attribute.String("conf_tag", confTag.String())),
	)

	writeSize, err := meter.Int64Counter("pebbletest.write.size", metric.WithUnit("By"))
	if err != nil {
		slog.Error("failed to create write size counter", slog.String("error", err.Error()))
		os.Exit(1)
	}

	var bucket []float64
	// 50us, 100us, 150us, ... 1000us (step: 50us)
	for dur := 50 * time.Microsecond; dur <= 1000*time.Microsecond; dur += 50 * time.Microsecond {
		bucket = append(bucket, float64(dur.Nanoseconds()))
	}
	// 1100us, 1200us, 1300us, ..., 2000us (step: 100us)
	for dur := 1100 * time.Microsecond; dur <= 2000*time.Microsecond; dur += 100 * time.Microsecond {
		bucket = append(bucket, float64(dur.Nanoseconds()))
	}
	// 4ms, 6ms, ... , 100ms (step: 2ms)
	for dur := 4 * time.Millisecond; dur <= 100*time.Millisecond; dur += 2 * time.Millisecond {
		bucket = append(bucket, float64(dur.Nanoseconds()))
	}
	// 110ms, 120ms, 130ms, ..., 1000ms (step: 10ms)
	for dur := 110 * time.Millisecond; dur <= 1000*time.Millisecond; dur += 10 * time.Millisecond {
		bucket = append(bucket, float64(dur.Nanoseconds()))
	}
	// 1100ms, 1200ms, 1300ms, ..., 5000ms (step: 100ms)
	for dur := 1100 * time.Millisecond; dur <= 5000*time.Millisecond; dur += 100 * time.Millisecond {
		bucket = append(bucket, float64(dur.Nanoseconds()))
	}
	writeDuration := mustInt64Histogram(meter, "pebbletest.write.duration",
		metric.WithUnit("ns"),
		metric.WithExplicitBucketBoundaries(bucket...),
	)
	batchCommitTotalDuration := mustInt64Histogram(meter, "pebbletest.batch_commit.total_duration",
		metric.WithUnit("ns"),
		metric.WithExplicitBucketBoundaries(bucket...),
	)
	batchCommitSemaphoreWaitDuration := mustInt64Histogram(meter, "pebbletest.batch_commit.semaphore_wait_duration",
		metric.WithUnit("ns"),
		metric.WithExplicitBucketBoundaries(bucket...),
	)
	batchCommitWALQueueWaitDuration := mustInt64Histogram(meter, "pebbletest.batch_commit.wal_queue_wait_duration",
		metric.WithUnit("ns"),
		metric.WithExplicitBucketBoundaries(bucket...),
	)
	batchCommitMemTableWriteStallDuration := mustInt64Histogram(meter, "pebbletest.batch_commit.memtable_write_stall_duration",
		metric.WithUnit("ns"),
		metric.WithExplicitBucketBoundaries(bucket...),
	)
	batchCommitL0ReadAmpWriteStallDuration := mustInt64Histogram(meter, "pebbletest.batch_commit.l0_read_amp_write_stall_duration",
		metric.WithUnit("ns"),
		metric.WithExplicitBucketBoundaries(bucket...),
	)
	batchCommitWALRotationDuration := mustInt64Histogram(meter, "pebbletest.batch_commit.wal_rotation_duration",
		metric.WithUnit("ns"),
		metric.WithExplicitBucketBoundaries(bucket...),
	)
	batchCommitCommitWaitDuration := mustInt64Histogram(meter, "pebbletest.batch_commit.commit_wait_duration",
		metric.WithUnit("ns"),
		metric.WithExplicitBucketBoundaries(bucket...),
	)

	opts := &pebble.Options{
		ErrorIfExists:               true,
		BytesPerSync:                sstBytesPerSync,
		WALBytesPerSync:             walBytesPerSync,
		FormatMajorVersion:          pebble.FormatVirtualSSTables,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               lbaseMaxBytes,
		Levels:                      make([]pebble.LevelOptions, 7),
		MaxOpenFiles:                16384,
		MemTableSize:                memTableSize,
		MemTableStopWritesThreshold: memTableStopWritesThreshold,
		MaxConcurrentCompactions: func() int {
			return maxConcurrentCompactions
		},
	}

	// from cockroachdb
	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.TargetByteDeletionRate = 128 << 20 // 128 MB

	opts.WALMinSyncInterval = func() time.Duration {
		return walMinSyncInterval
	}

	opts.Experimental.L0CompactionConcurrency = l0CompactionConcurrency
	opts.Experimental.EnableValueBlocks = func() bool {
		return enableValueBlocks
	}

	// Disable multi-level compaction heuristic for now. See #134423
	// for why this was disabled, and what needs to be changed to reenable it.
	// This issue tracks re-enablement: https://github.com/cockroachdb/pebble/issues/4139
	if !enableMultiLevelCompactionHeuristic {
		opts.Experimental.MultiLevelCompactionHeuristic = pebble.NoMultiLevel{}
	}

	opts.Levels[0].TargetFileSize = l0TargetFileSize
	for i := range opts.Levels {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
	opts.Levels[6].FilterPolicy = nil
	opts.FlushSplitBytes = opts.Levels[0].TargetFileSize

	opts.EnsureDefaults()

	if verbose {
		el := pebble.MakeLoggingEventListener(&logAdaptor{})
		opts.EventListener = &el
		opts.EventListener.FlushBegin = nil
		opts.EventListener.FlushEnd = nil
		opts.EventListener.ManifestCreated = nil
		opts.EventListener.ManifestDeleted = nil
		opts.EventListener.TableCreated = nil
		opts.EventListener.TableDeleted = nil
		opts.EventListener.TableIngested = nil
		opts.EventListener.TableStatsLoaded = nil
		opts.EventListener.TableValidated = nil
		opts.EventListener.WALCreated = nil
		opts.EventListener.WALDeleted = nil
	}

	db, err := pebble.Open(dbDir, opts)
	if err != nil {
		slog.Error("failed to open database", slog.String("err", err.Error()))
		os.Exit(1)
	}
	defer func() {
		if err := db.Close(); err != nil {
			slog.Error("failed to close database", slog.String("err", err.Error()))
		}
	}()

	var wg sync.WaitGroup
	stopc := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		startMetricsLogger(db, stopc)
	}()
	defer func() {
		close(stopc)
		wg.Wait()
	}()

	writeOpts := pebble.NoSync
	if useSync {
		writeOpts = pebble.Sync
	}

	rng := rand.NewChaCha8([32]byte{
		0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf,
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	})

	key := make([]byte, 8)
	value := make([]byte, valueSize)

	var seqNum uint64
	startTime := time.Now()
	for time.Since(startTime) < testDuration {
		batchStartTime := time.Now()
		batch := db.NewBatch()

		for range batchLength {
			seqNum++
			binary.BigEndian.PutUint64(key, seqNum)
			_, _ = rng.Read(value)
			if err := batch.Set(key, value, writeOpts); err != nil {
				slog.Error("failed to set key-value pair", slog.Uint64("key", seqNum), slog.String("err", err.Error()))
				os.Exit(1)
			}
		}

		if err := batch.Commit(writeOpts); err != nil {
			slog.Error("failed to commit batch", slog.String("err", err.Error()))
			os.Exit(1)
		}

		stats := batch.CommitStats()
		batchCommitTotalDuration.Record(context.Background(), stats.TotalDuration.Nanoseconds())
		batchCommitSemaphoreWaitDuration.Record(context.Background(), stats.SemaphoreWaitDuration.Nanoseconds())
		batchCommitWALQueueWaitDuration.Record(context.Background(), stats.WALQueueWaitDuration.Nanoseconds())
		batchCommitMemTableWriteStallDuration.Record(context.Background(), stats.MemTableWriteStallDuration.Nanoseconds())
		batchCommitL0ReadAmpWriteStallDuration.Record(context.Background(), stats.L0ReadAmpWriteStallDuration.Nanoseconds())
		batchCommitWALRotationDuration.Record(context.Background(), stats.WALRotationDuration.Nanoseconds())
		batchCommitCommitWaitDuration.Record(context.Background(), stats.CommitWaitDuration.Nanoseconds())

		if err := batch.Close(); err != nil {
			slog.Error("failed to close batch", slog.String("err", err.Error()))
			os.Exit(1)
		}
		writeDuration.Record(context.Background(), time.Since(batchStartTime).Nanoseconds())
		writeSize.Add(context.Background(), int64(valueSize)*int64(batchLength))
	}
}

func startTelemetry() func() {
	if otelAddr == "" {
		return func() {}
	}

	closer, err := startMetricProvider(context.Background(), "pebbletest", otelAddr)
	if err != nil {
		slog.Error("failed to start metric provider", slog.String("error", err.Error()))
		os.Exit(1)
	}
	closeMetricProvider := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = closer(ctx)
	}
	slog.Info("metric provider started", slog.String("addr", otelAddr))

	const deprecatedRuntimeFeature = "OTEL_GO_X_DEPRECATED_RUNTIME_METRICS"
	old := os.Getenv(deprecatedRuntimeFeature)
	defer func() {
		_ = os.Setenv(deprecatedRuntimeFeature, old)
	}()

	_ = os.Setenv(deprecatedRuntimeFeature, "false")
	if err := runtime.Start(); err != nil {
		slog.Error("failed to start runtime metrics", slog.String("error", err.Error()))
		os.Exit(1)
	}

	_ = os.Setenv(deprecatedRuntimeFeature, "true")
	if err := runtime.Start(); err != nil {
		slog.Error("failed to start runtime metrics", slog.String("error", err.Error()))
		os.Exit(1)
	}

	return closeMetricProvider
}

func startMetricProvider(ctx context.Context, serviceName string, addr string) (func(context.Context) error, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics exporter: %w", err)
	}

	// It customizes the bucket size of the process.runtime.go.gc.pause_ns.
	var boundaries []float64
	// 50us, 100us, 150us, ..., 1ms
	for dur := 50 * time.Microsecond; dur <= time.Millisecond; dur += 50 * time.Microsecond {
		boundaries = append(boundaries, float64(dur))
	}
	// 5ms, 10ms, 15ms, 20ms, 25ms, 30ms, ..., 95ms
	for dur := 5 * time.Millisecond; dur < 100*time.Millisecond; dur += 5 * time.Millisecond {
		boundaries = append(boundaries, float64(dur))
	}
	// 100ms, 200ms, 300ms, ..., 1000ms
	for dur := 100 * time.Millisecond; dur <= 1000*time.Millisecond; dur += 100 * time.Millisecond {
		boundaries = append(boundaries, float64(dur))
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{
				Name: "process.runtime.go.gc.pause_ns",
			},
			sdkmetric.Stream{
				Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
					Boundaries: boundaries,
				},
			},
		)),
	)
	otel.SetMeterProvider(meterProvider)

	closer := func(ctx context.Context) error {
		return errors.Join(
			meterProvider.Shutdown(ctx),
			conn.Close(),
		)
	}

	return closer, nil
}

func mustInt64Histogram(meter metric.Meter, name string, options ...metric.Int64HistogramOption) metric.Int64Histogram {
	hist, err := meter.Int64Histogram(name, options...)
	if err != nil {
		slog.Error("failed to create histogram", slog.String("name", name), slog.String("error", err.Error()))
		os.Exit(1)
	}
	return hist
}

func startMetricsLogger(db *pebble.DB, stopc <-chan struct{}) {
	if metricsLogInterval <= 0 {
		return
	}
	ticker := time.NewTicker(metricsLogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			var sb strings.Builder
			fmt.Fprintf(&sb, "DB Metrics\n%s", db.Metrics())
			slog.Info(sb.String())
		case <-stopc:
			return
		}
	}
}

type logAdaptor struct{}

var _ pebble.Logger = (*logAdaptor)(nil)

func (*logAdaptor) Infof(format string, args ...any) {
	slog.Info(fmt.Sprintf(format, args...))
}

func (*logAdaptor) Errorf(format string, args ...any) {
	slog.Error(fmt.Sprintf(format, args...))
}

func (*logAdaptor) Fatalf(format string, args ...any) {
	slog.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}
