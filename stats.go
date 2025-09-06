package pebbletest

import (
	"context"
	"time"

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

type stats struct {
	writeSize metric.Int64Counter

	batchCommitCount                       metric.Int64Counter
	batchCommitTotalDuration               metric.Int64Counter
	batchCommitSemaphoreWaitDuration       metric.Int64Counter
	batchCommitWALQueueWaitDuration        metric.Int64Counter
	batchCommitMemTableWriteStallDuration  metric.Int64Counter
	batchCommitL0ReadAmpWriteStallDuration metric.Int64Counter
	batchCommitWALRotationDuration         metric.Int64Counter
	batchCommitCommitWaitDuration          metric.Int64Counter

	compactionCount metric.Int64Counter
	writeStallCount metric.Int64Counter
}

func NewMetricProvider(addr, testID string) (close func(), _ error) {
	if addr == "" {
		return func() {}, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	res, err := resource.New(context.Background(),
		resource.WithFromEnv(),
		resource.WithHost(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName("pebbletest"),
			attribute.String("test-id", testID),
		),
	)
	if err != nil {
		return nil, err
	}

	metricExporter, err := otlpmetricgrpc.New(context.Background(), otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, err
	}

	mpOpts := []sdkmetric.Option{
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	}
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
	mpOpts = append(mpOpts,
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

	meterProvider := sdkmetric.NewMeterProvider(mpOpts...)
	otel.SetMeterProvider(meterProvider)

	err = runtime.Start()
	if err != nil {
		return nil, err
	}

	closer := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_ = meterProvider.Shutdown(ctx)
		_ = conn.Close()
	}

	return closer, nil
}

func newStats() (s *stats, err error) {
	meter := otel.Meter("pebbletest")

	s = &stats{}
	s.writeSize, err = meter.Int64Counter("pebbletest.write.size", metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}

	s.batchCommitCount, err = meter.Int64Counter("pebbletest.batch_commit.count", metric.WithUnit("{count}"))
	if err != nil {
		return nil, err
	}

	s.batchCommitTotalDuration, err = meter.Int64Counter("pebbletest.batch_commit.total_duration", metric.WithUnit("ns"))
	if err != nil {
		return nil, err
	}

	s.batchCommitSemaphoreWaitDuration, err = meter.Int64Counter("pebbletest.batch_commit.semaphore_wait_duration", metric.WithUnit("ns"))
	if err != nil {
		return nil, err
	}

	s.batchCommitWALQueueWaitDuration, err = meter.Int64Counter("pebbletest.batch_commit.wal_queue_wait_duration", metric.WithUnit("ns"))
	if err != nil {
		return nil, err
	}

	s.batchCommitMemTableWriteStallDuration, err = meter.Int64Counter("pebbletest.batch_commit.memtable_write_stall_duration", metric.WithUnit("ns"))
	if err != nil {
		return nil, err
	}

	s.batchCommitL0ReadAmpWriteStallDuration, err = meter.Int64Counter("pebbletest.batch_commit.l0_read_amp_write_stall_duration", metric.WithUnit("ns"))
	if err != nil {
		return nil, err
	}

	s.batchCommitWALRotationDuration, err = meter.Int64Counter("pebbletest.batch_commit.wal_rotation_duration", metric.WithUnit("ns"))
	if err != nil {
		return nil, err
	}

	s.batchCommitCommitWaitDuration, err = meter.Int64Counter("pebbletest.batch_commit.commit_wait_duration", metric.WithUnit("ns"))
	if err != nil {
		return nil, err
	}

	s.compactionCount, err = meter.Int64Counter("pebbletest.compaction.count", metric.WithUnit("{count}"))
	if err != nil {
		return nil, err
	}

	s.writeStallCount, err = meter.Int64Counter("pebbletest.write_stall.count", metric.WithUnit("{count}"))
	if err != nil {
		return nil, err
	}

	return s, nil
}
