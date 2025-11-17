package migrations

import (
	"context"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/services"
	"github.com/rs/xid"
)

func (m *Manager) SubscribeProgress(ctx context.Context, id string) (<-chan ProgressEvent, func(), error) {
	m.mu.Lock()
	record, ok := m.migrations[id]
	if !ok {
		m.mu.Unlock()
		return nil, nil, ErrMigrationNotFound
	}

	ch := make(chan ProgressEvent, 16)
	subID := xid.New().String()
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

func (m *Manager) RunMigration(record *MigrationRecord, srcDef, dstDef services.ServiceDefinition, srcFolder, dstFolder fsservices.Folder, opts MigrationOptions, executeFn func(string, services.ServiceDefinition, services.ServiceDefinition, fsservices.Folder, fsservices.Folder, MigrationOptions) (*migration.Result, error)) {
	m.logger.Info().
		Str("migration_id", record.ID).
		Str("source", srcDef.ID).
		Str("destination", dstDef.ID).
		Msg("starting migration")

	heartbeat := time.NewTicker(5 * time.Second)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-heartbeat.C:
				m.publishProgress(record.ID, "running", nil, nil)
			case <-done:
				heartbeat.Stop()
				return
			}
		}
	}()

	result, err := executeFn(record.ID, srcDef, dstDef, srcFolder, dstFolder, opts)
	close(done)

	if err != nil {
		m.mu.Lock()
		record.Status = MigrationStatusFailed
		record.Error = err.Error()
		finished := time.Now().UTC()
		record.CompletedAt = &finished
		m.mu.Unlock()

		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Msg("migration failed")

		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}

	finished := time.Now().UTC()

	m.mu.Lock()
	record.Status = MigrationStatusCompleted
	record.CompletedAt = &finished
	record.Result = result
	m.mu.Unlock()

	m.logger.Info().
		Str("migration_id", record.ID).
		Msg("migration completed successfully")

	srcStats := result.Runtime.Src
	dstStats := result.Runtime.Dst
	m.publishProgress(record.ID, "completed", &srcStats, &dstStats)
	m.closeSubscribers(record.ID)
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
