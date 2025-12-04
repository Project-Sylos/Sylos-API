package migrations

import (
	"context"
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/services"
	fstypes "github.com/Project-Sylos/Sylos-FS/pkg/types"
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

func (m *Manager) RunMigration(record *MigrationRecord, srcDef, dstDef services.ServiceDefinition, srcFolder, dstFolder fstypes.Folder, opts MigrationOptions, spectraConfigOverridePath string) {
	m.logger.Info().
		Str("migration_id", record.ID).
		Str("source", srcDef.ID).
		Str("destination", dstDef.ID).
		Msg("starting migration")

	// Ensure plan is seeded before starting migration (do this in goroutine to avoid blocking HTTP handler)
	plan := m.rootsMgr.GetPlan(record.ID)
	if plan != nil && !plan.Seeded {
		_, _, _, err := m.rootsMgr.SeedPlanIfReady(record.ID)
		if err != nil {
			m.mu.Lock()
			record.Status = MigrationStatusFailed
			record.Error = fmt.Sprintf("failed to seed migration plan: %v", err)
			finished := time.Now().UTC()
			record.CompletedAt = &finished
			m.mu.Unlock()

			m.logger.Error().
				Err(err).
				Str("migration_id", record.ID).
				Msg("failed to seed migration plan")

			m.publishProgress(record.ID, "failed", nil, nil)
			m.closeSubscribers(record.ID)
			return
		}
		// Re-fetch plan to get updated database path
		plan = m.rootsMgr.GetPlan(record.ID)
		if plan != nil && plan.Seeded && plan.DatabasePath != "" {
			opts.DatabasePath = plan.DatabasePath
		}
	}

	// Start migration with controller for programmatic shutdown
	controller, err := m.ExecuteMigrationWithController(record.ID, srcDef, dstDef, srcFolder, dstFolder, opts, m.resolveDBPath, func(def services.ServiceDefinition, rootID, connID string) (fstypes.FSAdapter, func(), error) {
		return m.serviceMgr.AcquireAdapterWithOverride(def, rootID, connID, spectraConfigOverridePath)
	})
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
			Msg("failed to start migration")

		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}

	// Note: We do NOT call cleanup functions here. Once migration.StartMigration() is called,
	// the migration engine takes ownership of the adapters and handles cleanup itself.
	// Calling cleanup here would cause double-close issues since adapters share the same
	// underlying Spectra SDK instance.

	// Store controller and DB instance in record
	// The DB instance allows us to query logs/metrics without opening a new connection
	// (BoltDB only allows one connection at a time)
	// Get the DB instance from the controller (BoltDB operations are thread-safe)
	boltDB := controller.GetDB()
	m.mu.Lock()
	record.Controller = controller
	record.DB = boltDB
	m.mu.Unlock()

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

	// Wait for migration to complete or be shutdown
	result, err := controller.Wait()
	close(done)

	// Check if migration was suspended (clean shutdown via killswitch)
	if err != nil && err.Error() == "migration suspended by force shutdown" {
		// Migration was cleanly suspended - result contains stats up to shutdown point
		finished := time.Now().UTC()

		m.mu.Lock()
		record.Status = MigrationStatusSuspended
		record.CompletedAt = &finished
		record.Result = &result
		record.Controller = nil // Clear controller reference
		m.mu.Unlock()

		m.logger.Info().
			Str("migration_id", record.ID).
			Msg("migration suspended (killswitch activated)")

		srcStats := result.Runtime.Src
		dstStats := result.Runtime.Dst
		m.publishProgress(record.ID, "suspended", &srcStats, &dstStats)
		m.closeSubscribers(record.ID)
		return
	}

	if err != nil {
		m.mu.Lock()
		record.Status = MigrationStatusFailed
		record.Error = err.Error()
		finished := time.Now().UTC()
		record.CompletedAt = &finished
		record.Controller = nil // Clear controller reference
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
	record.Result = &result
	record.Controller = nil // Clear controller reference
	m.mu.Unlock()

	// Migration Engine will update its YAML config with completion status and rounds
	// No need to update our minimal metadata here

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

	// Migration Engine will update its YAML config with round changes
	// No need to update our minimal metadata here

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
