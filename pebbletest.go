package pebbletest

import (
	"context"
	"encoding/binary"
	"math/rand/v2"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type PebbleTest struct {
	config

	db       *pebble.DB
	running  atomic.Bool
	valueSet [][]byte

	stats *stats
}

func New(opts ...Option) (*PebbleTest, error) {
	c, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	pt := &PebbleTest{
		config: c,
	}

	rng := rand.NewChaCha8([32]byte{
		0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf,
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	})
	pt.valueSet = make([][]byte, pt.batchLength*10)
	for i := range pt.valueSet {
		pt.valueSet[i] = make([]byte, pt.valueSize)
		_, _ = rng.Read(pt.valueSet[i])
	}

	pt.stats, err = newStats()
	if err != nil {
		return nil, err
	}

	pebbleOpts, err := ParseOptions(pt.dbOptions)
	if err != nil {
		return nil, err
	}

	el := pebble.MakeLoggingEventListener(&logAdaptor{
		logger: c.logger.Sugar(),
	})
	pebbleOpts.EventListener = &el
	pebbleOpts.EventListener.ManifestCreated = nil
	pebbleOpts.EventListener.ManifestDeleted = nil
	pebbleOpts.EventListener.TableCreated = nil
	pebbleOpts.EventListener.TableDeleted = nil
	pebbleOpts.EventListener.TableIngested = nil
	pebbleOpts.EventListener.TableStatsLoaded = nil
	pebbleOpts.EventListener.TableValidated = nil
	pebbleOpts.EventListener.WALCreated = nil
	pebbleOpts.EventListener.WALDeleted = nil

	if !pt.verboseEventLogger {
		pebbleOpts.EventListener.FlushBegin = nil
		pebbleOpts.EventListener.FlushEnd = nil
		pebbleOpts.EventListener.CompactionBegin = nil
		pebbleOpts.EventListener.WriteStallBegin = nil
	}
	compactionEndHook := el.CompactionEnd
	pebbleOpts.EventListener.CompactionEnd = func(info pebble.CompactionInfo) {
		if pt.verboseEventLogger {
			compactionEndHook(info)
		}
		pt.stats.compactionCount.Add(context.Background(), 1, metric.WithAttributes(
			attribute.String("reason", info.Reason),
		))
	}

	writeStallEndHook := el.WriteStallEnd
	pebbleOpts.EventListener.WriteStallEnd = func() {
		if pt.verboseEventLogger {
			writeStallEndHook()
		}
		pt.stats.writeStallCount.Add(context.Background(), 1)
	}

	pebbleOpts.ErrorIfExists = true

	db, err := pebble.Open(pt.dbDir, pebbleOpts)
	if err != nil {
		return nil, err
	}
	pt.db = db
	pt.running.Store(true)

	return pt, nil
}

func (pt *PebbleTest) Start() error {
	lis, err := net.Listen("tcp", pt.pprofAddr)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = http.Serve(lis, nil)
	}()

	stopc := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		pt.startMetricsLogger(pt.db, stopc)
	}()

	defer func() {
		close(stopc)
		_ = lis.Close()
		wg.Wait()
	}()

	return pt.start()
}

func (pt *PebbleTest) Stop() {
	pt.running.Store(false)
}

func (pt *PebbleTest) start() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writeOpts := pebble.NoSync
	if pt.syncWAL {
		writeOpts = pebble.Sync
	}

	key := make([]byte, 8)

	var seqNum uint64
	startTime := time.Now()
	for pt.running.Load() && time.Since(startTime) < pt.testDuration {
		batch := pt.db.NewBatch()

		for range pt.batchLength {
			seqNum++
			binary.BigEndian.PutUint64(key, seqNum)
			value := pt.valueSet[seqNum%uint64(len(pt.valueSet))]
			if err := batch.Set(key, value, writeOpts); err != nil {
				return err
			}
		}

		if err := batch.Commit(writeOpts); err != nil {
			return err
		}

		s := batch.CommitStats()
		pt.stats.batchCommitCount.Add(ctx, 1)
		pt.stats.batchCommitTotalDuration.Add(ctx, s.TotalDuration.Nanoseconds())
		pt.stats.batchCommitSemaphoreWaitDuration.Add(ctx, s.SemaphoreWaitDuration.Nanoseconds())
		pt.stats.batchCommitWALQueueWaitDuration.Add(ctx, s.WALQueueWaitDuration.Nanoseconds())
		pt.stats.batchCommitMemTableWriteStallDuration.Add(ctx, s.MemTableWriteStallDuration.Nanoseconds())
		pt.stats.batchCommitL0ReadAmpWriteStallDuration.Add(ctx, s.L0ReadAmpWriteStallDuration.Nanoseconds())
		pt.stats.batchCommitWALRotationDuration.Add(ctx, s.WALRotationDuration.Nanoseconds())
		pt.stats.batchCommitCommitWaitDuration.Add(ctx, s.CommitWaitDuration.Nanoseconds())

		pt.stats.writeSize.Add(context.Background(), int64(pt.valueSize)*int64(pt.batchLength))

		if err := batch.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (pt *PebbleTest) startMetricsLogger(db *pebble.DB, stopc <-chan struct{}) {
	if pt.dbMetricsLogInterval <= 0 {
		return
	}

	ticker := time.NewTicker(pt.dbMetricsLogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pt.logger.Info("DB Metrics\n" + db.Metrics().String())
		case <-stopc:
			return
		}
	}
}
