package migrations

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	corebridgeDB "codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
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

	// Get or resolve database path
	dbPath := opts.DatabasePath
	if dbPath == "" {
		var err error
		dbPath, err = m.resolveDBPath("", record.ID)
		if err != nil {
			m.mu.Lock()
			record.Status = MigrationStatusFailed
			record.Error = fmt.Sprintf("failed to resolve database path: %v", err)
			finished := time.Now().UTC()
			record.CompletedAt = &finished
			m.mu.Unlock()

			m.logger.Error().
				Err(err).
				Str("migration_id", record.ID).
				Msg("failed to resolve database path")

			m.publishProgress(record.ID, "failed", nil, nil)
			m.closeSubscribers(record.ID)
			return
		}
	}

	// API owns DB lifecycle - open DB in pool BEFORE starting migration
	dbInstance, err := m.dbPool.Open(record.ID, dbPath)
	if err != nil {
		m.mu.Lock()
		record.Status = MigrationStatusFailed
		record.Error = fmt.Sprintf("failed to open database: %v", err)
		finished := time.Now().UTC()
		record.CompletedAt = &finished
		m.mu.Unlock()

		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Str("db_path", dbPath).
			Msg("failed to open database in pool")

		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}

	// Get adapters from RootPlan (acquired during root selection)
	// Re-fetch plan to ensure we have the latest state including adapters
	plan = m.rootsMgr.GetPlan(record.ID)
	if plan == nil {
		m.mu.Lock()
		record.Status = MigrationStatusFailed
		record.Error = "root plan not found"
		finished := time.Now().UTC()
		record.CompletedAt = &finished
		m.mu.Unlock()

		m.logger.Error().
			Str("migration_id", record.ID).
			Msg("root plan not found")

		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}

	if plan.SourceAdapter == nil || plan.DestinationAdapter == nil {
		m.mu.Lock()
		record.Status = MigrationStatusFailed
		record.Error = "adapters not available in root plan"
		finished := time.Now().UTC()
		record.CompletedAt = &finished
		m.mu.Unlock()

		m.logger.Error().
			Str("migration_id", record.ID).
			Msg("adapters not available in root plan")

		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}

	// Start migration with controller for programmatic shutdown
	// Pass the pre-opened DB instance and pre-acquired adapters (API owns lifecycle)
	controller, err := m.ExecuteMigrationWithController(record.ID, srcDef, dstDef, srcFolder, dstFolder, opts, dbInstance, m.resolveDBPath, plan.SourceAdapter, plan.DestinationAdapter)
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
	// Use the DB instance from the pool (API owns lifecycle, not the controller)
	// The DB instance allows us to query logs/metrics without opening a new connection
	// (BoltDB only allows one connection at a time)
	m.mu.Lock()
	record.Controller = controller
	record.DB = dbInstance // Use DB from pool, not from controller
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

	// Extract adapters from plan for cleanup
	plan = m.rootsMgr.GetPlan(record.ID)
	var srcAdapter, dstAdapter fstypes.FSAdapter
	if plan != nil {
		srcAdapter = plan.SourceAdapter
		dstAdapter = plan.DestinationAdapter
	}

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

		// Clear adapter references from RootPlan (calls release functions)
		m.rootsMgr.ClearAdapters(record.ID)

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

		// Clear adapter references from RootPlan (calls release functions)
		m.rootsMgr.ClearAdapters(record.ID)

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

	// Close adapters (API owns lifecycle)
	m.closeAdapters(srcAdapter, dstAdapter, record.ID)

	// Clear adapter references from RootPlan
	m.rootsMgr.ClearAdapters(record.ID)

	// Migration Engine will update its YAML config with completion status and rounds
	// No need to update our minimal metadata here

	m.logger.Info().
		Str("migration_id", record.ID).
		Msg("migration completed successfully")

	// Trigger ETL after traversal completes
	// Get config path to check/update status
	configPath := corebridgeDB.ConfigPathFromDatabasePath(dbPath)
	if configPath != "" {
		// Load YAML config to check current status
		yamlCfg, err := migration.LoadMigrationConfig(configPath)
		if err == nil {
			currentStatus := strings.TrimSpace(yamlCfg.State.Status)

			// Only trigger ETL if not already in Awaiting-Path-Review
			if currentStatus != "Awaiting-Path-Review" {
				// Update status to Preparing-Path-Review
				yamlCfg.State.Status = "Preparing-Path-Review"
				if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
					m.logger.Error().
						Err(err).
						Str("migration_id", record.ID).
						Msg("failed to update status to Preparing-Path-Review")
				} else {
					m.logger.Info().
						Str("migration_id", record.ID).
						Str("old_status", currentStatus).
						Str("new_status", "Preparing-Path-Review").
						Msg("traversal completed, triggering ETL")

					// Run ETL directly using the existing BoltDB instance
					// The DB instance is still in the pool and available via record.DB
					m.runETL(record, dbPath, configPath, dbInstance)
				}
			} else {
				m.logger.Debug().
					Str("migration_id", record.ID).
					Msg("already in Awaiting-Path-Review, skipping ETL")
			}
		}
	}

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
