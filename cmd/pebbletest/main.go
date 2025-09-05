package main

import (
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/ijsong/pebbletest"
)

func init() {
	_ = pflag.String("pebble-dir", "", "Path for pebble db")
	_ = pflag.String("pebble-options", "", "Path for pebble option file")
	_ = pflag.Bool("sync-wal", true, "Sync WAL")
	_ = pflag.Duration("metrics-log-interval", 3*time.Second, "Metrics log interval")
	_ = pflag.Bool("verbose-event-logger", false, "Verbose event logger")
	_ = pflag.String("value-size", "4KiB", "Value size")
	_ = pflag.Int("batch-length", 1, "Batch length")
	_ = pflag.Duration("test-duration", 0, "Test duration")
	_ = pflag.String("otel-addr", "", "OpenTelemetry collector address (e.g. localhost:4317)")
	_ = pflag.String("pprof-addr", "", "Pprof listen address (e.g. localhost:6060)")
	_ = pflag.String("test-id", "", "Metric label to identify different test instances")
	pflag.Parse()

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		panic("failed to bind flags: " + err.Error())
	}
}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	stopMetricProvider, err := pebbletest.NewMetricProvider(viper.GetString("otel-addr"), viper.GetString("test-id"))
	if err != nil {
		logger.Fatal("failed to create metric provider", zap.Error(err))
	}
	defer stopMetricProvider()

	buf, err := os.ReadFile(viper.GetString("pebble-options"))
	if err != nil {
		logger.Fatal("failed to read pebble options", zap.Error(err))
	}

	valueSize, err := humanize.ParseBytes(viper.GetString("value-size"))
	if err != nil {
		logger.Fatal("failed to parse value size", zap.Error(err))
	}

	pt, err := pebbletest.New(
		pebbletest.WithDBDir(viper.GetString("pebble-dir")),
		pebbletest.WithDBOptions(string(buf)),
		pebbletest.WithSyncWAL(viper.GetBool("sync-wal")),
		pebbletest.WithDBMetricsLogInterval(viper.GetDuration("metrics-log-interval")),
		pebbletest.WithVerboseEventLogger(viper.GetBool("verbose-event-logger")),
		pebbletest.WithValueSize(valueSize),
		pebbletest.WithBatchLength(viper.GetInt("batch-length")),
		pebbletest.WithTestDuration(viper.GetDuration("test-duration")),
		pebbletest.WithLogger(logger),
		pebbletest.WithPprofAddr(viper.GetString("pprof-addr")),
	)
	if err != nil {
		logger.Fatal("failed to create pebble test", zap.Error(err))
	}

	var wg sync.WaitGroup
	quit := make(chan struct{})
	defer func() {
		close(quit)
		wg.Wait()
	}()
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case sig := <-sigC:
			logger.Info("signal received, shutting down", zap.String("signal", sig.String()))
			pt.Stop()
			return
		case <-quit:
			return
		}
	}()

	err = pt.Start()
	if err != nil {
		logger.Error("pebble test failed", zap.Error(err))
	}
}
