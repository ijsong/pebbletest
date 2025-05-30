// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"context"
	"encoding/binary"
	"io"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/crc"
	"github.com/prometheus/client_golang/prometheus"
)

var walSyncLabels = pprof.Labels("pebble", "wal-sync")
var errClosedWriter = errors.New("pebble/record: closed LogWriter")

type block struct {
	// buf[:written] has already been filled with fragments. Updated atomically.
	written atomic.Int32
	// buf[:flushed] has already been flushed to w.
	flushed int32
	buf     [blockSize]byte
}

type flusher interface {
	Flush() error
}

type syncer interface {
	Sync() error
}

const (
	syncConcurrencyBits = 12

	// SyncConcurrency is the maximum number of concurrent sync operations that
	// can be performed. Note that a sync operation is initiated either by a call
	// to SyncRecord or by a call to Close. Exported as this value also limits
	// the commit concurrency in commitPipeline.
	SyncConcurrency = 1 << syncConcurrencyBits
)

type syncSlot struct {
	wg  *sync.WaitGroup
	err *error
}

// syncQueue is a lock-free fixed-size single-producer, single-consumer
// queue. The single-producer can push to the head, and the single-consumer can
// pop multiple values from the tail. Popping calls Done() on each of the
// available *sync.WaitGroup elements.
type syncQueue struct {
	// headTail packs together a 32-bit head index and a 32-bit tail index. Both
	// are indexes into slots modulo len(slots)-1.
	//
	// tail = index of oldest data in queue
	// head = index of next slot to fill
	//
	// Slots in the range [tail, head) are owned by consumers.  A consumer
	// continues to own a slot outside this range until it nils the slot, at
	// which point ownership passes to the producer.
	//
	// The head index is stored in the most-significant bits so that we can
	// atomically add to it and the overflow is harmless.
	headTail atomic.Uint64

	// slots is a ring buffer of values stored in this queue. The size must be a
	// power of 2. A slot is in use until the tail index has moved beyond it.
	slots [SyncConcurrency]syncSlot

	// blocked is an atomic boolean which indicates whether syncing is currently
	// blocked or can proceed. It is used by the implementation of
	// min-sync-interval to block syncing until the min interval has passed.
	blocked atomic.Bool
}

const dequeueBits = 32

// unpack extracts the head and tail indices from a 64-bit unsigned integer.
func (q *syncQueue) unpack(ptrs uint64) (head, tail uint32) {
	const mask = 1<<dequeueBits - 1
	head = uint32((ptrs >> dequeueBits) & mask)
	tail = uint32(ptrs & mask)
	return head, tail
}

func (q *syncQueue) push(wg *sync.WaitGroup, err *error) {
	ptrs := q.headTail.Load()
	head, tail := q.unpack(ptrs)
	if (tail+uint32(len(q.slots)))&(1<<dequeueBits-1) == head {
		panic("pebble: queue is full")
	}

	slot := &q.slots[head&uint32(len(q.slots)-1)]
	slot.wg = wg
	slot.err = err

	// Increment head. This passes ownership of slot to dequeue and acts as a
	// store barrier for writing the slot.
	q.headTail.Add(1 << dequeueBits)
}

func (q *syncQueue) setBlocked() {
	q.blocked.Store(true)
}

func (q *syncQueue) clearBlocked() {
	q.blocked.Store(false)
}

func (q *syncQueue) empty() bool {
	head, tail, _ := q.load()
	return head == tail
}

// load returns the head, tail of the queue for what should be synced to the
// caller. It can return a head, tail of zero if syncing is blocked due to
// min-sync-interval. It additionally returns the real length of this queue,
// regardless of whether syncing is blocked.
func (q *syncQueue) load() (head, tail, realLength uint32) {
	ptrs := q.headTail.Load()
	head, tail = q.unpack(ptrs)
	realLength = head - tail
	if q.blocked.Load() {
		return 0, 0, realLength
	}
	return head, tail, realLength
}

// REQUIRES: queueSemChan is non-nil.
func (q *syncQueue) pop(head, tail uint32, err error, queueSemChan chan struct{}) error {
	if tail == head {
		// Queue is empty.
		return nil
	}

	for ; tail != head; tail++ {
		slot := &q.slots[tail&uint32(len(q.slots)-1)]
		wg := slot.wg
		if wg == nil {
			return errors.Errorf("nil waiter at %d", errors.Safe(tail&uint32(len(q.slots)-1)))
		}
		*slot.err = err
		slot.wg = nil
		slot.err = nil
		// We need to bump the tail count before releasing the queueSemChan
		// semaphore as releasing the semaphore can cause a blocked goroutine to
		// acquire the semaphore and enqueue before we've "freed" space in the
		// queue.
		q.headTail.Add(1)
		wg.Done()
		// Is always non-nil in production, unless using wal package for WAL
		// failover.
		if queueSemChan != nil {
			<-queueSemChan
		}
	}

	return nil
}

// pendingSyncs abstracts out the handling of pending sync requests. In
// standalone mode the implementation is a thin wrapper around syncQueue. In
// the mode where the LogWriter can be subject to failover, there is no queue
// kept in the LogWriter and the signaling to those waiting for sync is
// handled in the wal package.
//
// To avoid heap allocations due to the use of this interface, the parameters
// and return values follow some strict rules:
//   - The PendingSync parameter can be reused by the caller after push returns.
//     The implementation should be a pointer backed by a struct that is already
//     heap allocated, which the caller can reuse for the next push call.
//   - The pendingSyncSnapshot return value must be backed by the pendingSyncs
//     implementation, so calling snapshotForPop again will cause the previous
//     snapshot to be overwritten.
type pendingSyncs interface {
	push(PendingSync)
	setBlocked()
	clearBlocked()
	empty() bool
	snapshotForPop() pendingSyncsSnapshot
	pop(snap pendingSyncsSnapshot, err error) error
}

type pendingSyncsSnapshot interface {
	empty() bool
}

// PendingSync abstracts the sync specification for a record queued on the
// LogWriter. The only implementations are provided in this package since
// syncRequested is not exported.
type PendingSync interface {
	syncRequested() bool
}

// The implementation of pendingSyncs in standalone mode.
type pendingSyncsWithSyncQueue struct {
	syncQueue
	syncQueueLen    *base.GaugeSampleMetric
	snapshotBacking syncQueueSnapshot
	// See the comment for LogWriterConfig.QueueSemChan.
	queueSemChan chan struct{}
}

var _ pendingSyncs = &pendingSyncsWithSyncQueue{}

func (q *pendingSyncsWithSyncQueue) push(ps PendingSync) {
	ps2 := ps.(*pendingSyncForSyncQueue)
	q.syncQueue.push(ps2.wg, ps2.err)
}

func (q *pendingSyncsWithSyncQueue) snapshotForPop() pendingSyncsSnapshot {
	head, tail, realLength := q.syncQueue.load()
	q.snapshotBacking = syncQueueSnapshot{
		head: head,
		tail: tail,
	}
	q.syncQueueLen.AddSample(int64(realLength))
	return &q.snapshotBacking
}

func (q *pendingSyncsWithSyncQueue) pop(snap pendingSyncsSnapshot, err error) error {
	s := snap.(*syncQueueSnapshot)
	return q.syncQueue.pop(s.head, s.tail, err, q.queueSemChan)
}

// The implementation of pendingSyncsSnapshot in standalone mode.
type syncQueueSnapshot struct {
	head, tail uint32
}

func (s *syncQueueSnapshot) empty() bool {
	return s.head == s.tail
}

// The implementation of pendingSync in standalone mode.
type pendingSyncForSyncQueue struct {
	wg  *sync.WaitGroup
	err *error
}

func (ps *pendingSyncForSyncQueue) syncRequested() bool {
	return ps.wg != nil
}

// The implementation of pendingSyncs in failover mode.
type pendingSyncsWithHighestSyncIndex struct {
	// The highest "index" queued that is requesting a sync. Initialized
	// to NoSyncIndex, and reset to NoSyncIndex after the sync.
	index           atomic.Int64
	snapshotBacking PendingSyncIndex
	// blocked is an atomic boolean which indicates whether syncing is currently
	// blocked or can proceed. It is used by the implementation of
	// min-sync-interval to block syncing until the min interval has passed.
	blocked                   atomic.Bool
	externalSyncQueueCallback ExternalSyncQueueCallback
}

// NoSyncIndex is the value of PendingSyncIndex when a sync is not requested.
const NoSyncIndex = -1

func (si *pendingSyncsWithHighestSyncIndex) init(
	externalSyncQueueCallback ExternalSyncQueueCallback,
) {
	si.index.Store(NoSyncIndex)
	si.externalSyncQueueCallback = externalSyncQueueCallback
}

func (si *pendingSyncsWithHighestSyncIndex) push(ps PendingSync) {
	ps2 := ps.(*PendingSyncIndex)
	si.index.Store(ps2.Index)
}

func (si *pendingSyncsWithHighestSyncIndex) setBlocked() {
	si.blocked.Store(true)
}

func (si *pendingSyncsWithHighestSyncIndex) clearBlocked() {
	si.blocked.Store(false)
}

func (si *pendingSyncsWithHighestSyncIndex) empty() bool {
	return si.load() == NoSyncIndex
}

func (si *pendingSyncsWithHighestSyncIndex) snapshotForPop() pendingSyncsSnapshot {
	si.snapshotBacking = PendingSyncIndex{Index: si.load()}
	return &si.snapshotBacking
}

func (si *pendingSyncsWithHighestSyncIndex) load() int64 {
	index := si.index.Load()
	if index != NoSyncIndex && si.blocked.Load() {
		index = NoSyncIndex
	}
	return index
}

func (si *pendingSyncsWithHighestSyncIndex) pop(snap pendingSyncsSnapshot, err error) error {
	index := snap.(*PendingSyncIndex)
	if index.Index == NoSyncIndex {
		return nil
	}
	// Set to NoSyncIndex if a higher index has not queued.
	si.index.CompareAndSwap(index.Index, NoSyncIndex)
	si.externalSyncQueueCallback(*index, err)
	return nil
}

// PendingSyncIndex implements both pendingSyncsSnapshot and PendingSync.
type PendingSyncIndex struct {
	// Index is some state meaningful to the user of LogWriter. The LogWriter
	// itself only examines whether Index is equal to NoSyncIndex.
	Index int64
}

func (s *PendingSyncIndex) empty() bool {
	return s.Index == NoSyncIndex
}

func (s *PendingSyncIndex) syncRequested() bool {
	return s.Index != NoSyncIndex
}

// flusherCond is a specialized condition variable that allows its condition to
// change and readiness be signalled without holding its associated mutex. In
// particular, when a waiter is added to syncQueue atomically, this condition
// variable can be signalled without holding flusher.Mutex.
type flusherCond struct {
	mu   *sync.Mutex
	q    pendingSyncs
	cond sync.Cond
}

func (c *flusherCond) init(mu *sync.Mutex, q pendingSyncs) {
	c.mu = mu
	c.q = q
	// Yes, this is a bit circular, but that is intentional. flusherCond.cond.L
	// points flusherCond so that when cond.L.Unlock is called flusherCond.Unlock
	// will be called and we can check the !syncQueue.empty() condition.
	c.cond.L = c
}

func (c *flusherCond) Signal() {
	// Pass-through to the cond var.
	c.cond.Signal()
}

func (c *flusherCond) Wait() {
	// Pass-through to the cond var. Note that internally the cond var implements
	// Wait as:
	//
	//   t := notifyListAdd()
	//   L.Unlock()
	//   notifyListWait(t)
	//   L.Lock()
	//
	// We've configured the cond var to call flusherReady.Unlock() which allows
	// us to check the !syncQueue.empty() condition without a danger of missing a
	// notification. Any call to flusherReady.Signal() after notifyListAdd() is
	// called will cause the subsequent notifyListWait() to return immediately.
	c.cond.Wait()
}

func (c *flusherCond) Lock() {
	c.mu.Lock()
}

func (c *flusherCond) Unlock() {
	c.mu.Unlock()
	if !c.q.empty() {
		// If the current goroutine is about to block on sync.Cond.Wait, this call
		// to Signal will prevent that. The comment in Wait above explains a bit
		// about what is going on here, but it is worth reiterating:
		//
		//   flusherCond.Wait()
		//     sync.Cond.Wait()
		//       t := notifyListAdd()
		//       flusherCond.Unlock()    <-- we are here
		//       notifyListWait(t)
		//       flusherCond.Lock()
		//
		// The call to Signal here results in:
		//
		//     sync.Cond.Signal()
		//       notifyListNotifyOne()
		//
		// The call to notifyListNotifyOne() will prevent the call to
		// notifyListWait(t) from blocking.
		c.cond.Signal()
	}
}

type durationFunc func() time.Duration

// syncTimer is an interface for timers, modeled on the closure callback mode
// of time.Timer. See time.AfterFunc and LogWriter.afterFunc. syncTimer is used
// by tests to mock out the timer functionality used to implement
// min-sync-interval.
type syncTimer interface {
	Reset(time.Duration) bool
	Stop() bool
}

// LogWriter writes records to an underlying io.Writer. In order to support WAL
// file reuse, a LogWriter's records are tagged with the WAL's file
// number. When reading a log file a record from a previous incarnation of the
// file will return the error ErrInvalidLogNum.
type LogWriter struct {
	// w is the underlying writer.
	w io.Writer
	// c is w as a closer.
	c io.Closer
	// s is w as a syncer.
	s syncer
	// logNum is the low 32-bits of the log's file number.
	logNum uint32
	// blockNum is the zero based block number for the current block.
	blockNum int64
	// err is any accumulated error. It originates in flusher.err, and is
	// updated to reflect flusher.err when a block is full and getting enqueued.
	// Therefore, there is a lag between when flusher.err has a non-nil error,
	// and when that non-nil error is reflected in LogWriter.err. On close, it
	// is set to errClosedWriter to inform accidental future calls to
	// SyncRecord*.
	err error
	// block is the current block being written. Protected by flusher.Mutex.
	block *block
	free  struct {
		sync.Mutex
		blocks []*block
	}

	flusher struct {
		sync.Mutex
		// Flusher ready is a condition variable that is signalled when there are
		// blocks to flush, syncing has been requested, or the LogWriter has been
		// closed. For signalling of a sync, it is safe to call without holding
		// flusher.Mutex.
		ready flusherCond
		// Set to true when the flush loop should be closed.
		close bool
		// Closed when the flush loop has terminated.
		closed chan struct{}
		// Accumulated flush error.
		err error
		// minSyncInterval is the minimum duration between syncs.
		minSyncInterval durationFunc
		fsyncLatency    prometheus.Histogram
		pending         []*block
		// Pushing and popping from pendingSyncs does not require flusher mutex to
		// be held.
		pendingSyncs pendingSyncs
		metrics      *LogWriterMetrics
	}

	// afterFunc is a hook to allow tests to mock out the timer functionality
	// used for min-sync-interval. In normal operation this points to
	// time.AfterFunc.
	afterFunc func(d time.Duration, f func()) syncTimer

	// Backing for both pendingSyncs implementations.
	pendingSyncsBackingQ     pendingSyncsWithSyncQueue
	pendingSyncsBackingIndex pendingSyncsWithHighestSyncIndex

	pendingSyncForSyncQueueBacking pendingSyncForSyncQueue
}

// LogWriterConfig is a struct used for configuring new LogWriters
type LogWriterConfig struct {
	WALMinSyncInterval durationFunc
	WALFsyncLatency    prometheus.Histogram
	// QueueSemChan is an optional channel to pop from when popping from
	// LogWriter.flusher.syncQueue. It functions as a semaphore that prevents
	// the syncQueue from overflowing (which will cause a panic). All production
	// code ensures this is non-nil.
	QueueSemChan chan struct{}

	// ExternalSyncQueueCallback is set to non-nil when the LogWriter is used
	// as part of a WAL implementation that can failover between LogWriters.
	//
	// In this case, QueueSemChan is always nil, and SyncRecordGeneralized must
	// be used with a PendingSync parameter that is implemented by
	// PendingSyncIndex. When an index is synced (which implies all earlier
	// indices are also synced), this callback is invoked. The caller must not
	// hold any mutex when invoking this callback, since the lock ordering
	// requirement in this case is that any higher layer locks (in the wal
	// package) precede the lower layer locks (in the record package). These
	// callbacks are serialized since they are invoked from the flushLoop.
	ExternalSyncQueueCallback ExternalSyncQueueCallback
}

// ExternalSyncQueueCallback is to be run when a PendingSync has been
// processed, either successfully or with an error.
type ExternalSyncQueueCallback func(doneSync PendingSyncIndex, err error)

// initialAllocatedBlocksCap is the initial capacity of the various slices
// intended to hold LogWriter blocks. The LogWriter may allocate more blocks
// than this threshold allows.
const initialAllocatedBlocksCap = 32

// blockPool pools *blocks to avoid allocations. Blocks are only added to the
// Pool when a LogWriter is closed. Before that, free blocks are maintained
// within a LogWriter's own internal free list `w.free.blocks`.
var blockPool = sync.Pool{
	New: func() any { return &block{} },
}

// NewLogWriter returns a new LogWriter.
//
// The io.Writer may also be used as an io.Closer and syncer. No other methods
// will be called on the writer.
func NewLogWriter(
	w io.Writer, logNum base.DiskFileNum, logWriterConfig LogWriterConfig,
) *LogWriter {
	c, _ := w.(io.Closer)
	s, _ := w.(syncer)
	r := &LogWriter{
		w: w,
		c: c,
		s: s,
		// NB: we truncate the 64-bit log number to 32-bits. This is ok because a)
		// we are very unlikely to reach a file number of 4 billion and b) the log
		// number is used as a validation check and using only the low 32-bits is
		// sufficient for that purpose.
		logNum: uint32(logNum),
		afterFunc: func(d time.Duration, f func()) syncTimer {
			return time.AfterFunc(d, f)
		},
	}
	m := &LogWriterMetrics{}
	if logWriterConfig.ExternalSyncQueueCallback != nil {
		r.pendingSyncsBackingIndex.init(logWriterConfig.ExternalSyncQueueCallback)
		r.flusher.pendingSyncs = &r.pendingSyncsBackingIndex
	} else {
		r.pendingSyncsBackingQ = pendingSyncsWithSyncQueue{
			syncQueueLen: &m.SyncQueueLen,
			queueSemChan: logWriterConfig.QueueSemChan,
		}
		r.flusher.pendingSyncs = &r.pendingSyncsBackingQ
	}

	r.free.blocks = make([]*block, 0, initialAllocatedBlocksCap)
	r.block = blockPool.Get().(*block)
	r.flusher.ready.init(&r.flusher.Mutex, r.flusher.pendingSyncs)
	r.flusher.closed = make(chan struct{})
	r.flusher.pending = make([]*block, 0, cap(r.free.blocks))
	r.flusher.metrics = m

	f := &r.flusher
	f.minSyncInterval = logWriterConfig.WALMinSyncInterval
	f.fsyncLatency = logWriterConfig.WALFsyncLatency

	go func() {
		pprof.Do(context.Background(), walSyncLabels, r.flushLoop)
	}()
	return r
}

func (w *LogWriter) flushLoop(context.Context) {
	f := &w.flusher
	f.Lock()

	// Initialize idleStartTime to when the loop starts.
	idleStartTime := time.Now()
	var syncTimer syncTimer
	defer func() {
		// Capture the idle duration between the last piece of work and when the
		// loop terminated.
		f.metrics.WriteThroughput.IdleDuration += time.Since(idleStartTime)
		if syncTimer != nil {
			syncTimer.Stop()
		}
		close(f.closed)
		f.Unlock()
	}()

	// The flush loop performs flushing of full and partial data blocks to the
	// underlying writer (LogWriter.w), syncing of the writer, and notification
	// to sync requests that they have completed.
	//
	// - flusher.ready is a condition variable that is signalled when there is
	//   work to do. Full blocks are contained in flusher.pending. The current
	//   partial block is in LogWriter.block. And sync operations are held in
	//   flusher.syncQ.
	//
	// - The decision to sync is determined by whether there are any sync
	//   requests present in flusher.syncQ and whether enough time has elapsed
	//   since the last sync. If not enough time has elapsed since the last sync,
	//   flusher.syncQ.blocked will be set to 1. If syncing is blocked,
	//   syncQueue.empty() will return true and syncQueue.load() will return 0,0
	//   (i.e. an empty list).
	//
	// - flusher.syncQ.blocked is cleared by a timer that is initialized when
	//   blocked is set to 1. When blocked is 1, no syncing will take place, but
	//   flushing will continue to be performed. The on/off toggle for syncing
	//   does not need to be carefully synchronized with the rest of processing
	//   -- all we need to ensure is that after any transition to blocked=1 there
	//   is eventually a transition to blocked=0. syncTimer performs this
	//   transition. Note that any change to min-sync-interval will not take
	//   effect until the previous timer elapses.
	//
	// - Picking up the syncing work to perform requires coordination with
	//   picking up the flushing work. Specifically, flushing work is queued
	//   before syncing work. The guarantee of this code is that when a sync is
	//   requested, any previously queued flush work will be synced. This
	//   motivates reading the syncing work (f.syncQ.load()) before picking up
	//   the flush work (w.block.written.Load()).

	// The list of full blocks that need to be written. This is copied from
	// f.pending on every loop iteration, though the number of elements is
	// usually small (most frequently 1). In the case of the WAL LogWriter, the
	// number of blocks is bounded by the size of the WAL's corresponding
	// memtable (MemtableSize/BlockSize). With the default 64 MiB memtables,
	// this works out to at most 2048 elements if the entirety of the memtable's
	// contents are queued.
	pending := make([]*block, 0, cap(f.pending))
	for {
		for {
			// Grab the portion of the current block that requires flushing. Note that
			// the current block can be added to the pending blocks list after we release
			// the flusher lock, but it won't be part of pending.
			written := w.block.written.Load()
			if len(f.pending) > 0 || written > w.block.flushed || !f.pendingSyncs.empty() {
				break
			}
			if f.close {
				// If the writer is closed, pretend the sync timer fired immediately so
				// that we can process any queued sync requests.
				f.pendingSyncs.clearBlocked()
				if !f.pendingSyncs.empty() {
					break
				}
				return
			}
			f.ready.Wait()
			continue
		}
		// Found work to do, so no longer idle.
		//
		// NB: it is safe to read pending before loading from the syncQ since
		// mutations to pending require the w.flusher mutex, which is held here.
		// There is no risk that someone will concurrently add to pending, so the
		// following sequence, which would pick up a syncQ entry without the
		// corresponding data, is impossible:
		//
		// Thread enqueueing       This thread
		//                         1. read pending
		// 2. add block to pending
		// 3. add to syncQ
		//                         4. read syncQ
		workStartTime := time.Now()
		idleDuration := workStartTime.Sub(idleStartTime)
		pending = append(pending[:0], f.pending...)
		f.pending = f.pending[:0]
		f.metrics.PendingBufferLen.AddSample(int64(len(pending)))

		// Grab the list of sync waiters. Note that syncQueue.load() will return
		// 0,0 while we're waiting for the min-sync-interval to expire. This
		// allows flushing to proceed even if we're not ready to sync.
		snap := f.pendingSyncs.snapshotForPop()

		// Grab the portion of the current block that requires flushing. Note that
		// the current block can be added to the pending blocks list after we
		// release the flusher lock, but it won't be part of pending. This has to
		// be ordered after we get the list of sync waiters from syncQ in order to
		// prevent a race where a waiter adds itself to syncQ, but this thread
		// picks up the entry in syncQ and not the buffered data.
		written := w.block.written.Load()
		data := w.block.buf[w.block.flushed:written]
		w.block.flushed = written

		fErr := f.err
		f.Unlock()
		// If flusher has an error, we propagate it to waiters. Note in spite of
		// error we consume the pending list above to free blocks for writers.
		if fErr != nil {
			// NB: pop may invoke ExternalSyncQueueCallback, which is why we have
			// called f.Unlock() above. We will acquire the lock again below.
			f.pendingSyncs.pop(snap, fErr)
			// Update the idleStartTime if work could not be done, so that we don't
			// include the duration we tried to do work as idle. We don't bother
			// with the rest of the accounting, which means we will undercount.
			idleStartTime = time.Now()
			f.Lock()
			continue
		}
		synced, syncLatency, bytesWritten, err := w.flushPending(data, pending, snap)
		f.Lock()
		if synced && f.fsyncLatency != nil {
			f.fsyncLatency.Observe(float64(syncLatency))
		}
		f.err = err
		if f.err != nil {
			f.pendingSyncs.clearBlocked()
			// Update the idleStartTime if work could not be done, so that we don't
			// include the duration we tried to do work as idle. We don't bother
			// with the rest of the accounting, which means we will undercount.
			idleStartTime = time.Now()
			continue
		}

		if synced && f.minSyncInterval != nil {
			// A sync was performed. Make sure we've waited for the min sync
			// interval before syncing again.
			if min := f.minSyncInterval(); min > 0 {
				f.pendingSyncs.setBlocked()
				if syncTimer == nil {
					syncTimer = w.afterFunc(min, func() {
						f.pendingSyncs.clearBlocked()
						f.ready.Signal()
					})
				} else {
					syncTimer.Reset(min)
				}
			}
		}
		// Finished work, and started idling.
		idleStartTime = time.Now()
		workDuration := idleStartTime.Sub(workStartTime)
		f.metrics.WriteThroughput.Bytes += bytesWritten
		f.metrics.WriteThroughput.WorkDuration += workDuration
		f.metrics.WriteThroughput.IdleDuration += idleDuration
	}
}

func (w *LogWriter) flushPending(
	data []byte, pending []*block, snap pendingSyncsSnapshot,
) (synced bool, syncLatency time.Duration, bytesWritten int64, err error) {
	defer func() {
		// Translate panics into errors. The errors will cause flushLoop to shut
		// down, but allows us to do so in a controlled way and avoid swallowing
		// the stack that created the panic if panic'ing itself hits a panic
		// (e.g. unlock of unlocked mutex).
		if r := recover(); r != nil {
			err = errors.Newf("%v", r)
		}
	}()

	for _, b := range pending {
		bytesWritten += blockSize - int64(b.flushed)
		if err = w.flushBlock(b); err != nil {
			break
		}
	}
	if n := len(data); err == nil && n > 0 {
		bytesWritten += int64(n)
		_, err = w.w.Write(data)
	}

	synced = !snap.empty()
	if synced {
		if err == nil && w.s != nil {
			syncLatency, err = w.syncWithLatency()
		} else {
			synced = false
		}
		f := &w.flusher
		if popErr := f.pendingSyncs.pop(snap, err); popErr != nil {
			return synced, syncLatency, bytesWritten, firstError(err, popErr)
		}
	}

	return synced, syncLatency, bytesWritten, err
}

func (w *LogWriter) syncWithLatency() (time.Duration, error) {
	start := time.Now()
	err := w.s.Sync()
	syncLatency := time.Since(start)
	return syncLatency, err
}

func (w *LogWriter) flushBlock(b *block) error {
	if _, err := w.w.Write(b.buf[b.flushed:]); err != nil {
		return err
	}
	b.written.Store(0)
	b.flushed = 0
	w.free.Lock()
	w.free.blocks = append(w.free.blocks, b)
	w.free.Unlock()
	return nil
}

// queueBlock queues the current block for writing to the underlying writer,
// allocates a new block and reserves space for the next header.
func (w *LogWriter) queueBlock() {
	// Allocate a new block, blocking until one is available. We do this first
	// because w.block is protected by w.flusher.Mutex.
	w.free.Lock()
	if len(w.free.blocks) == 0 {
		w.free.blocks = append(w.free.blocks, blockPool.Get().(*block))
	}
	nextBlock := w.free.blocks[len(w.free.blocks)-1]
	w.free.blocks = w.free.blocks[:len(w.free.blocks)-1]
	w.free.Unlock()

	f := &w.flusher
	f.Lock()
	f.pending = append(f.pending, w.block)
	w.block = nextBlock
	f.ready.Signal()
	w.err = w.flusher.err
	f.Unlock()

	w.blockNum++
}

// Close flushes and syncs any unwritten data and closes the writer.
// Where required, external synchronisation is provided by commitPipeline.mu.
func (w *LogWriter) Close() error {
	return w.closeInternal(PendingSyncIndex{Index: NoSyncIndex})
}

// CloseWithLastQueuedRecord is like Close, but optionally accepts a
// lastQueuedRecord, that the caller will be notified about when synced.
func (w *LogWriter) CloseWithLastQueuedRecord(lastQueuedRecord PendingSyncIndex) error {
	return w.closeInternal(lastQueuedRecord)
}

func (w *LogWriter) closeInternal(lastQueuedRecord PendingSyncIndex) error {
	f := &w.flusher

	// Emit an EOF trailer signifying the end of this log. This helps readers
	// differentiate between a corrupted entry in the middle of a log from
	// garbage at the tail from a recycled log file.
	w.emitEOFTrailer()

	// Signal the flush loop to close.
	f.Lock()
	f.close = true
	f.ready.Signal()
	f.Unlock()

	// Wait for the flush loop to close. The flush loop will not close until all
	// pending data has been written or an error occurs.
	<-f.closed

	// Sync any flushed data to disk. NB: flushLoop will sync after flushing the
	// last buffered data only if it was requested via syncQ, so we need to sync
	// here to ensure that all the data is synced.
	err := w.flusher.err
	var syncLatency time.Duration
	if err == nil && w.s != nil {
		syncLatency, err = w.syncWithLatency()
	}
	f.Lock()
	if err == nil && f.fsyncLatency != nil {
		f.fsyncLatency.Observe(float64(syncLatency))
	}
	free := w.free.blocks
	f.Unlock()

	// NB: the caller of closeInternal may not care about a non-nil cerr below
	// if all queued writes have been successfully written and synced.
	if lastQueuedRecord.Index != NoSyncIndex {
		w.pendingSyncsBackingIndex.externalSyncQueueCallback(lastQueuedRecord, err)
	}
	if w.c != nil {
		cerr := w.c.Close()
		w.c = nil
		err = firstError(err, cerr)
	}

	for _, b := range free {
		b.flushed = 0
		b.written.Store(0)
		blockPool.Put(b)
	}

	w.err = errClosedWriter
	return err
}

// firstError returns the first non-nil error of err0 and err1, or nil if both
// are nil.
func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}

// WriteRecord writes a complete record. Returns the offset just past the end
// of the record.
// External synchronisation provided by commitPipeline.mu.
func (w *LogWriter) WriteRecord(p []byte) (int64, error) {
	logSize, err := w.SyncRecord(p, nil, nil)
	return logSize, err
}

// SyncRecord writes a complete record. If wg != nil the record will be
// asynchronously persisted to the underlying writer and done will be called on
// the wait group upon completion. Returns the offset just past the end of the
// record.
// External synchronisation provided by commitPipeline.mu.
func (w *LogWriter) SyncRecord(
	p []byte, wg *sync.WaitGroup, err *error,
) (logSize int64, err2 error) {
	w.pendingSyncForSyncQueueBacking = pendingSyncForSyncQueue{
		wg:  wg,
		err: err,
	}
	return w.SyncRecordGeneralized(p, &w.pendingSyncForSyncQueueBacking)
}

// SyncRecordGeneralized is a version of SyncRecord that accepts a
// PendingSync.
func (w *LogWriter) SyncRecordGeneralized(p []byte, ps PendingSync) (logSize int64, err2 error) {
	if w.err != nil {
		return -1, w.err
	}

	// The `i == 0` condition ensures we handle empty records. Such records can
	// possibly be generated for VersionEdits stored in the MANIFEST. While the
	// MANIFEST is currently written using Writer, it is good to support the same
	// semantics with LogWriter.
	for i := 0; i == 0 || len(p) > 0; i++ {
		p = w.emitFragment(i, p)
	}

	if ps.syncRequested() {
		// If we've been asked to persist the record, add the WaitGroup to the sync
		// queue and signal the flushLoop. Note that flushLoop will write partial
		// blocks to the file if syncing has been requested. The contract is that
		// any record written to the LogWriter to this point will be flushed to the
		// OS and synced to disk.
		f := &w.flusher
		f.pendingSyncs.push(ps)
		f.ready.Signal()
	}

	offset := w.blockNum*blockSize + int64(w.block.written.Load())
	// Note that we don't return w.err here as a concurrent call to Close would
	// race with our read. That's ok because the only error we could be seeing is
	// one to syncing for which the caller can receive notification of by passing
	// in a non-nil err argument.
	return offset, nil
}

// Size returns the current size of the file.
// External synchronisation provided by commitPipeline.mu.
func (w *LogWriter) Size() int64 {
	return w.blockNum*blockSize + int64(w.block.written.Load())
}

func (w *LogWriter) emitEOFTrailer() {
	// Write a recyclable chunk header with a different log number.  Readers
	// will treat the header as EOF when the log number does not match.
	b := w.block
	i := b.written.Load()
	binary.LittleEndian.PutUint32(b.buf[i+0:i+4], 0) // CRC
	binary.LittleEndian.PutUint16(b.buf[i+4:i+6], 0) // Size
	b.buf[i+6] = recyclableFullChunkType
	binary.LittleEndian.PutUint32(b.buf[i+7:i+11], w.logNum+1) // Log number
	b.written.Store(i + int32(recyclableHeaderSize))
}

func (w *LogWriter) emitFragment(n int, p []byte) (remainingP []byte) {
	b := w.block
	i := b.written.Load()
	first := n == 0
	last := blockSize-i-recyclableHeaderSize >= int32(len(p))

	if last {
		if first {
			b.buf[i+6] = recyclableFullChunkType
		} else {
			b.buf[i+6] = recyclableLastChunkType
		}
	} else {
		if first {
			b.buf[i+6] = recyclableFirstChunkType
		} else {
			b.buf[i+6] = recyclableMiddleChunkType
		}
	}

	binary.LittleEndian.PutUint32(b.buf[i+7:i+11], w.logNum)

	r := copy(b.buf[i+recyclableHeaderSize:], p)
	j := i + int32(recyclableHeaderSize+r)
	binary.LittleEndian.PutUint32(b.buf[i+0:i+4], crc.New(b.buf[i+6:j]).Value())
	binary.LittleEndian.PutUint16(b.buf[i+4:i+6], uint16(r))
	b.written.Store(j)

	if blockSize-b.written.Load() < recyclableHeaderSize {
		// There is no room for another fragment in the block, so fill the
		// remaining bytes with zeros and queue the block for flushing.
		clear(b.buf[b.written.Load():])
		w.queueBlock()
	}
	return p[r:]
}

// Metrics must typically be called after Close, since the callee will no
// longer modify the returned LogWriterMetrics. It is also current if there is
// nothing left to flush in the flush loop, but that is an implementation
// detail that callers should not rely on.
func (w *LogWriter) Metrics() LogWriterMetrics {
	w.flusher.Lock()
	defer w.flusher.Unlock()
	m := *w.flusher.metrics
	return m
}

// LogWriterMetrics contains misc metrics for the log writer.
type LogWriterMetrics struct {
	WriteThroughput  base.ThroughputMetric
	PendingBufferLen base.GaugeSampleMetric
	SyncQueueLen     base.GaugeSampleMetric
}

// Merge merges metrics from x. Requires that x is non-nil.
func (m *LogWriterMetrics) Merge(x *LogWriterMetrics) error {
	m.WriteThroughput.Merge(x.WriteThroughput)
	m.PendingBufferLen.Merge(x.PendingBufferLen)
	m.SyncQueueLen.Merge(x.SyncQueueLen)
	return nil
}
