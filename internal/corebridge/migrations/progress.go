package migrations

import (
	"context"
	"math/rand"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"github.com/oklog/ulid/v2"
)

func (m *Manager) SubscribeProgress(ctx context.Context, id string) (<-chan ProgressEvent, func(), error) {
	m.mu.Lock()
	record, ok := m.migrations[id]
	if !ok {
		m.mu.Unlock()
		return nil, nil, ErrMigrationNotFound
	}

	ch := make(chan ProgressEvent, 16)
	// Generate ULID for subscriber ID (lexicographically sortable, time-ordered)
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	subID := ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
	if _, exists := m.subscribers[id]; !exists {
		m.subscribers[id] = make(map[string]chan ProgressEvent)
	}
	m.subscribers[id][subID] = ch
	snapshot := m.recordToStatus(record)
	m.mu.Unlock()

	ch <- ProgressEvent{
		Event:     "snapshot",
		Timestamp: time.Now().UTC(),
		Migration: snapshot,
	}

	cancel := func() {
		m.removeSubscriber(id, subID)
	}

	if ctx != nil {
		go func() {
			<-ctx.Done()
			cancel()
		}()
	}

	return ch, cancel, nil
}

func (m *Manager) publishProgress(id, event string, srcStats, dstStats *queue.QueueStats) {
	m.mu.RLock()
	record, ok := m.migrations[id]
	if !ok {
		m.mu.RUnlock()
		return
	}

	status := m.recordToStatus(record)
	subscribers := m.subscribers[id]
	channels := make([]chan ProgressEvent, 0, len(subscribers))
	for _, ch := range subscribers {
		channels = append(channels, ch)
	}
	m.mu.RUnlock()

	if len(channels) == 0 {
		return
	}

	update := ProgressEvent{
		Event:     event,
		Timestamp: time.Now().UTC(),
		Migration: status,
	}
	if srcStats != nil {
		update.Source = queueStatsSnapshotFrom(*srcStats)
	}
	if dstStats != nil {
		update.Destination = queueStatsSnapshotFrom(*dstStats)
	}

	// Migration Engine persists runtime state in DB; metadata remains minimal.

	for _, ch := range channels {
		select {
		case ch <- update:
		default:
		}
	}
}

func (m *Manager) removeSubscriber(migrationID, subscriberID string) {
	m.mu.Lock()
	subs, ok := m.subscribers[migrationID]
	if !ok {
		m.mu.Unlock()
		return
	}

	ch, ok := subs[subscriberID]
	if ok {
		delete(subs, subscriberID)
	}
	if len(subs) == 0 {
		delete(m.subscribers, migrationID)
	}
	m.mu.Unlock()

	if ok {
		close(ch)
	}
}

func (m *Manager) closeSubscribers(migrationID string) {
	m.mu.Lock()
	subs := m.subscribers[migrationID]
	delete(m.subscribers, migrationID)
	m.mu.Unlock()

	for _, ch := range subs {
		close(ch)
	}
}

func queueStatsSnapshotFrom(stats queue.QueueStats) QueueStatsSnapshot {
	return QueueStatsSnapshot{
		Round:        stats.Round,
		Pending:      stats.Pending,
		InProgress:   stats.InProgress,
		TotalTracked: stats.TotalTracked,
		Workers:      stats.Workers,
	}
}
