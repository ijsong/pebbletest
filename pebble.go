package pebbletest

import (
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"go.uber.org/zap"
)

func ParseOptions(optString string) (*pebble.Options, error) {
	opts := &pebble.Options{
		Levels: make([]pebble.LevelOptions, 7),
	}

	err := opts.Parse(string(optString), nil)
	if err != nil {
		return nil, err
	}

	for i := range opts.Levels {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10
		l.IndexBlockSize = 256 << 10
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
	opts.Levels[6].FilterPolicy = nil
	opts.EnsureDefaults()

	return opts, nil
}

type logAdaptor struct {
	logger *zap.SugaredLogger
}

var _ pebble.Logger = (*logAdaptor)(nil)

func (a *logAdaptor) Infof(format string, args ...any) {
	a.logger.Infof(format, args...)
}

func (a *logAdaptor) Errorf(format string, args ...any) {
	a.logger.Errorf(format, args...)
}

func (a *logAdaptor) Fatalf(format string, args ...any) {
	a.logger.Fatalf(format, args...)
}
