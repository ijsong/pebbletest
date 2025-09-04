package pebbletest

import (
	"errors"
	"math"
	"time"

	"go.uber.org/zap"
)

type config struct {
	dbDir                string
	dbOptions            string
	syncWAL              bool
	dbMetricsLogInterval time.Duration
	verboseEventLogger   bool
	valueSize            uint64
	batchLength          int
	testDuration         time.Duration
	logger               *zap.Logger
	pprofAddr            string
}

func newConfig(opts []Option) (config, error) {
	var c config
	for _, opt := range opts {
		opt.apply(&c)
	}
	if err := c.validate(); err != nil {
		return config{}, err
	}

	if c.testDuration <= 0 {
		c.testDuration = time.Duration(math.MaxInt64)
	}
	if c.logger == nil {
		c.logger = zap.NewNop()
	}
	if c.pprofAddr == "" {
		c.pprofAddr = ":6060"
	}
	return c, nil
}

func (c *config) validate() error {
	if c.dbDir == "" {
		return errors.New("dbDir is required")
	}
	if c.batchLength < 1 {
		return errors.New("batchLength must be greater than 0")
	}
	return nil
}

type Option interface {
	apply(*config)
}

type funcOption struct {
	f func(*config)
}

func newFuncOption(f func(*config)) *funcOption {
	return &funcOption{f: f}
}

func (fo *funcOption) apply(cfg *config) {
	fo.f(cfg)
}

func WithDBDir(dbDir string) Option {
	return newFuncOption(func(c *config) {
		c.dbDir = dbDir
	})
}

func WithDBOptions(optString string) Option {
	return newFuncOption(func(c *config) {
		c.dbOptions = optString
	})
}

func WithSyncWAL(sync bool) Option {
	return newFuncOption(func(c *config) {
		c.syncWAL = sync
	})
}

func WithDBMetricsLogInterval(d time.Duration) Option {
	return newFuncOption(func(c *config) {
		c.dbMetricsLogInterval = d
	})
}

func WithVerboseEventLogger(verbose bool) Option {
	return newFuncOption(func(c *config) {
		c.verboseEventLogger = verbose
	})
}

func WithValueSize(size uint64) Option {
	return newFuncOption(func(c *config) {
		c.valueSize = size
	})
}

func WithBatchLength(length int) Option {
	return newFuncOption(func(c *config) {
		c.batchLength = length
	})
}

func WithTestDuration(d time.Duration) Option {
	return newFuncOption(func(c *config) {
		c.testDuration = d
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(c *config) {
		c.logger = logger
	})
}

func WithPprofAddr(addr string) Option {
	return newFuncOption(func(c *config) {
		c.pprofAddr = addr
	})
}
