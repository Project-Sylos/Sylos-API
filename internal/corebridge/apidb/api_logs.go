package apidb

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

const (
	prefixAPILog     = "apilog:"
	apiLogBatchSize  = 64
	apiLogFlushEvery = time.Second
)

// APILogRecord is one install-wide API/process log line.
type APILogRecord struct {
	ID        string    `json:"id"`
	At        time.Time `json:"at"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Component string    `json:"component,omitempty"`
}

func keyAPILog(unixNano int64, id string) []byte {
	return fmt.Appendf(nil, "%s%020d:%s", prefixAPILog, unixNano, id)
}

// AppendAPILog enqueues one API/process log for buffered persistence into sylos.api/.
func (d *DB) AppendAPILog(level, message, component string) error {
	if d == nil || d.db == nil {
		return nil
	}
	rec := APILogRecord{
		ID:        uuid.New().String(),
		At:        time.Now().UTC(),
		Level:     level,
		Message:   message,
		Component: component,
	}
	if d.logBuf != nil {
		d.logBuf.Add(rec)
		return nil
	}
	return d.AppendAPILogs([]APILogRecord{rec})
}

// AppendAPILogs writes log records in one Badger transaction.
func (d *DB) AppendAPILogs(recs []APILogRecord) error {
	if d == nil || d.db == nil || len(recs) == 0 {
		return nil
	}
	return d.db.Update(func(txn *badger.Txn) error {
		for i := range recs {
			rec := &recs[i]
			if rec.ID == "" {
				rec.ID = uuid.New().String()
			}
			if rec.At.IsZero() {
				rec.At = time.Now().UTC()
			}
			b, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			if err := txn.Set(keyAPILog(rec.At.UnixNano(), rec.ID), b); err != nil {
				return err
			}
		}
		return nil
	})
}

type apiLogBuffer struct {
	db        *DB
	entries   []APILogRecord
	batchSize int
	interval  time.Duration
	mu        sync.Mutex
	cond      *sync.Cond
	flushing  bool
	draining  bool
	stopCh    chan struct{}
	done      chan struct{}
	stopped   int32
}

func newAPILogBuffer(db *DB, batchSize int, interval time.Duration) *apiLogBuffer {
	if batchSize <= 0 {
		batchSize = apiLogBatchSize
	}
	if interval <= 0 {
		interval = apiLogFlushEvery
	}
	lb := &apiLogBuffer{
		db:        db,
		entries:   make([]APILogRecord, 0, batchSize*2),
		batchSize: batchSize,
		interval:  interval,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
	lb.cond = sync.NewCond(&lb.mu)
	go lb.flushLoop()
	return lb
}

func (lb *apiLogBuffer) takeBatch() (batch []APILogRecord, hadWork bool) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.flushing || len(lb.entries) == 0 {
		return nil, false
	}
	n := len(lb.entries)
	if n > lb.batchSize {
		n = lb.batchSize
	}
	batch = make([]APILogRecord, n)
	copy(batch, lb.entries[:n])
	copy(lb.entries[:], lb.entries[n:])
	newLen := len(lb.entries) - n
	var zero APILogRecord
	for i := newLen; i < len(lb.entries); i++ {
		lb.entries[i] = zero
	}
	lb.entries = lb.entries[:newLen]
	lb.flushing = true
	lb.cond.Broadcast()
	return batch, true
}

func (lb *apiLogBuffer) setFlushingDone() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.flushing = false
	lb.cond.Broadcast()
}

func (lb *apiLogBuffer) drainAll() {
	for {
		batch, hadWork := lb.takeBatch()
		if !hadWork || len(batch) == 0 {
			return
		}
		_ = lb.db.AppendAPILogs(batch)
		lb.setFlushingDone()
	}
}

func (lb *apiLogBuffer) flushLoop() {
	defer close(lb.done)
	ticker := time.NewTicker(lb.interval)
	defer ticker.Stop()
	for {
		select {
		case <-lb.stopCh:
			lb.drainAll()
			return
		case <-ticker.C:
			batch, hadWork := lb.takeBatch()
			if !hadWork {
				continue
			}
			_ = lb.db.AppendAPILogs(batch)
			lb.setFlushingDone()
		}
	}
}

func (lb *apiLogBuffer) Add(rec APILogRecord) {
	lb.mu.Lock()
	for len(lb.entries) >= lb.batchSize*2 && !lb.draining {
		lb.draining = true
		lb.mu.Unlock()
		lb.drainAll()
		lb.mu.Lock()
		lb.draining = false
		lb.cond.Broadcast()
	}
	lb.entries = append(lb.entries, rec)
	shouldFlush := len(lb.entries) >= lb.batchSize
	lb.mu.Unlock()
	if shouldFlush {
		batch, hadWork := lb.takeBatch()
		if hadWork {
			_ = lb.db.AppendAPILogs(batch)
			lb.setFlushingDone()
		}
	}
}

func (lb *apiLogBuffer) Stop() {
	if !atomic.CompareAndSwapInt32(&lb.stopped, 0, 1) {
		return
	}
	close(lb.stopCh)
	<-lb.done
}
