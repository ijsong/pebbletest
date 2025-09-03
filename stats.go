package pebbletest

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
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
}

func newMetricProvider(addr string) (close func(), _ error) {
	if addr == "" {
		return func() {}, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("pebbletest"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	metricExporter, err := otlpmetricgrpc.New(context.Background(), otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)

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

	return s, nil
}
