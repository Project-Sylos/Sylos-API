package migrations

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/database"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/roots"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/services"
	"github.com/Project-Sylos/Sylos-API/pkg/config"
	"github.com/rs/zerolog"
)

// MigrationOptions represents options for a migration
type MigrationOptions struct {
	MigrationID             string
	DatabasePath            string
	RemoveExistingDB        bool
	UsePreseededDB          bool
	SourceConnectionID      string
	DestinationConnectionID string
	WorkerCount             int
	MaxRetries              int
	CoordinatorLead         int
	LogAddress              string
	LogLevel                string
	EnableLoggingTerminal   bool
	StartupDelaySec         int
	ProgressTickMillis      int
	Verification            VerificationOptions
}

// VerificationOptions represents verification options
type VerificationOptions struct {
	AllowPending  bool
	AllowFailed   bool
	AllowNotOnSrc bool
}

// StartMigrationRequest represents a request to start a migration
type StartMigrationRequest struct {
	MigrationID string
	Options     MigrationOptions
}

// Migration represents a migration
type Migration struct {
	ID            string
	SourceID      string
	DestinationID string
	StartedAt     time.Time
	Status        string
}

// Status represents migration status
type Status struct {
	Migration
	CompletedAt *time.Time
	Error       string
	Result      *ResultView
}

// ResultView represents a result view
type ResultView struct {
	RootSummary  RootSummaryView
	Runtime      RuntimeStatsView
	Verification VerificationView
}

// RootSummaryView represents root summary
type RootSummaryView struct {
	SrcRoots int
	DstRoots int
}

// RuntimeStatsView represents runtime statistics
type RuntimeStatsView struct {
	Duration string
	Src      QueueStatsView
	Dst      QueueStatsView
}

// VerificationView represents verification view
type VerificationView struct {
	SrcTotal    int
	DstTotal    int
	SrcPending  int
	DstPending  int
	SrcFailed   int
	DstFailed   int
	DstNotOnSrc int
}

// QueueStatsView represents queue statistics
type QueueStatsView struct {
	Name         string
	Round        int
	Pending      int
	InProgress   int
	TotalTracked int
	Workers      int
}

// QueueStatsSnapshot represents a snapshot of queue statistics
type QueueStatsSnapshot struct {
	Round        int
	Pending      int
	InProgress   int
	TotalTracked int
	Workers      int
}

// ProgressEvent represents a progress event
type ProgressEvent struct {
	Event       string
	Timestamp   time.Time
	Migration   Status
	Source      QueueStatsSnapshot
	Destination QueueStatsSnapshot
}

var ErrMigrationNotFound = fmt.Errorf("migration not found")

// Manager handles migration-related operations
type Manager struct {
	logger        zerolog.Logger
	cfg           config.Config
	migrations    map[string]*MigrationRecord
	subscribers   map[string]map[string]chan ProgressEvent
	mu            sync.RWMutex
	serviceMgr    *services.ServiceManager
	rootsMgr      *roots.Manager
	resolveDBPath func(path, migrationID string) (string, error)
}

type MigrationRecord struct {
	ID            string
	SourceID      string
	DestinationID string
	Status        string
	StartedAt     time.Time
	CompletedAt   *time.Time
	Result        *migration.Result
	Error         string
}

const (
	MigrationStatusRunning   = "running"
	MigrationStatusCompleted = "completed"
	MigrationStatusFailed    = "failed"
)

func NewManager(logger zerolog.Logger, cfg config.Config, serviceMgr *services.ServiceManager, rootsMgr *roots.Manager, resolveDBPath func(path, migrationID string) (string, error)) *Manager {
	return &Manager{
		logger:        logger,
		cfg:           cfg,
		migrations:    make(map[string]*MigrationRecord),
		subscribers:   make(map[string]map[string]chan ProgressEvent),
		serviceMgr:    serviceMgr,
		rootsMgr:      rootsMgr,
		resolveDBPath: resolveDBPath,
	}
}

func (m *Manager) StartMigration(ctx context.Context, req StartMigrationRequest) (Migration, error) {
	migrationID := req.MigrationID
	if migrationID == "" {
		migrationID = req.Options.MigrationID
	}
	if migrationID == "" {
		return Migration{}, fmt.Errorf("migration id is required")
	}

	opts := req.Options
	opts.MigrationID = migrationID

	// If databasePath is provided, we can start migration directly from uploaded DB
	// Otherwise, we need roots to be set
	if opts.DatabasePath != "" {
		// Validate that the DB file exists
		if _, err := os.Stat(opts.DatabasePath); os.IsNotExist(err) {
			return Migration{}, fmt.Errorf("database file not found: %s", opts.DatabasePath)
		}

		// Check if DB has valid schema and get roots from it
		database, err := db.NewDB(opts.DatabasePath)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to open database: %w", err)
		}
		defer database.Close()

		if err := database.ValidateCoreSchema(); err != nil {
			return Migration{}, fmt.Errorf("database schema invalid: %w", err)
		}

		// Get status to check if DB has roots
		status, err := migration.InspectMigrationStatus(database)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to inspect database: %w", err)
		}

		if status.IsEmpty() {
			return Migration{}, fmt.Errorf("database is empty, roots must be set first")
		}

		// For uploaded DBs, we need to infer source/destination from the DB or require them in options
		// For now, use generic values - the migration will work with whatever is in the DB
		srcDef := services.ServiceDefinition{
			ID:   "uploaded-db-source",
			Name: "Uploaded DB Source",
			Type: services.ServiceTypeLocal,
		}
		dstDef := services.ServiceDefinition{
			ID:   "uploaded-db-destination",
			Name: "Uploaded DB Destination",
			Type: services.ServiceTypeLocal,
		}

		// Create minimal folder descriptors (not used when resuming from existing DB)
		srcFolder := fsservices.Folder{Id: "root", LocationPath: "/"}
		dstFolder := fsservices.Folder{Id: "root", LocationPath: "/"}

		opts.UsePreseededDB = true
		opts.RemoveExistingDB = false
		if opts.LogAddress == "" {
			opts.LogAddress = m.cfg.Runtime.LogAddress
		}
		if opts.LogLevel == "" {
			opts.LogLevel = m.cfg.Runtime.LogLevel
		}

		record := &MigrationRecord{
			ID:            migrationID,
			SourceID:      srcDef.ID,
			DestinationID: dstDef.ID,
			Status:        MigrationStatusRunning,
			StartedAt:     time.Now().UTC(),
		}

		m.mu.Lock()
		if _, exists := m.migrations[migrationID]; exists {
			m.mu.Unlock()
			return Migration{}, fmt.Errorf("migration %s already exists", migrationID)
		}
		m.migrations[migrationID] = record
		if _, ok := m.subscribers[migrationID]; !ok {
			m.subscribers[migrationID] = make(map[string]chan ProgressEvent)
		}
		m.mu.Unlock()

		m.publishProgress(record.ID, "started", nil, nil)

		// Migration Engine SDK will spawn log terminal automatically when SkipListener is false
		go m.RunMigration(
			record,
			srcDef,
			dstDef,
			srcFolder,
			dstFolder,
			opts,
			func(id string, src, dst services.ServiceDefinition, srcF, dstF fsservices.Folder, opts MigrationOptions) (*migration.Result, error) {
				return m.ExecuteMigration(id, src, dst, srcF, dstF, opts, m.resolveDBPath, m.serviceMgr.AcquireAdapter)
			},
		)

		return Migration{
			ID:            record.ID,
			SourceID:      record.SourceID,
			DestinationID: record.DestinationID,
			StartedAt:     record.StartedAt,
			Status:        record.Status,
		}, nil
	}

	// Original flow: require roots to be set
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil {
		return Migration{}, fmt.Errorf("roots not set for migration %s and no databasePath provided", migrationID)
	}

	if !plan.HasSource || !plan.HasDestination {
		return Migration{}, fmt.Errorf("roots not fully configured for migration %s", migrationID)
	}

	if !plan.Seeded {
		if _, _, _, err := m.rootsMgr.SeedPlanIfReady(migrationID); err != nil {
			return Migration{}, err
		}
		plan = m.rootsMgr.GetPlan(migrationID)
		if plan == nil || !plan.Seeded {
			return Migration{}, fmt.Errorf("roots not fully configured for migration %s", migrationID)
		}
	}

	opts.DatabasePath = plan.DatabasePath
	opts.UsePreseededDB = true
	opts.RemoveExistingDB = false
	if opts.SourceConnectionID == "" {
		opts.SourceConnectionID = plan.SourceConnectionID
	}
	if opts.DestinationConnectionID == "" {
		opts.DestinationConnectionID = plan.DestinationConnectionID
	}
	if opts.LogAddress == "" {
		opts.LogAddress = m.cfg.Runtime.LogAddress
	}
	if opts.LogLevel == "" {
		opts.LogLevel = m.cfg.Runtime.LogLevel
	}

	record := &MigrationRecord{
		ID:            migrationID,
		SourceID:      plan.SourceDefinition.ID,
		DestinationID: plan.DestinationDefinition.ID,
		Status:        MigrationStatusRunning,
		StartedAt:     time.Now().UTC(),
	}

	m.mu.Lock()
	if _, exists := m.migrations[migrationID]; exists {
		m.mu.Unlock()
		return Migration{}, fmt.Errorf("migration %s already exists", migrationID)
	}
	m.migrations[migrationID] = record
	if _, ok := m.subscribers[migrationID]; !ok {
		m.subscribers[migrationID] = make(map[string]chan ProgressEvent)
	}
	m.mu.Unlock()

	m.publishProgress(record.ID, "started", nil, nil)

	// Migration Engine SDK will spawn log terminal automatically when SkipListener is false
	go m.RunMigration(
		record,
		plan.SourceDefinition,
		plan.DestinationDefinition,
		plan.SourceRoot,
		plan.DestinationRoot,
		opts,
		func(id string, src, dst services.ServiceDefinition, srcF, dstF fsservices.Folder, opts MigrationOptions) (*migration.Result, error) {
			return m.ExecuteMigration(id, src, dst, srcF, dstF, opts, m.resolveDBPath, m.serviceMgr.AcquireAdapter)
		},
	)

	return Migration{
		ID:            record.ID,
		SourceID:      record.SourceID,
		DestinationID: record.DestinationID,
		StartedAt:     record.StartedAt,
		Status:        record.Status,
	}, nil
}

func (m *Manager) GetMigrationStatus(ctx context.Context, id string) (Status, error) {
	m.mu.RLock()
	record, ok := m.migrations[id]
	m.mu.RUnlock()

	if ok {
		return m.recordToStatus(record), nil
	}

	// In-memory record not found - try to query database directly
	// This allows checking status of migrations that were started before server restart
	dbPath, err := m.resolveDBPath("", id)
	if err != nil {
		return Status{}, ErrMigrationNotFound
	}

	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return Status{}, ErrMigrationNotFound
	}

	// Open and inspect database
	dbStatus, err := database.InspectMigrationStatusFromDB(ctx, m.logger, dbPath)
	if err != nil {
		return Status{}, ErrMigrationNotFound
	}

	// Convert migration.MigrationStatus to API Status format
	// We need to infer some fields that aren't in MigrationStatus
	status := Status{
		Migration: Migration{
			ID:        id,
			StartedAt: time.Now().UTC(), // We don't have this in DB, use current time as fallback
			Status:    MigrationStatusCompleted,
		},
	}

	if dbStatus.HasPending() {
		status.Status = MigrationStatusRunning
	} else if dbStatus.HasFailures() {
		status.Status = MigrationStatusFailed
	}

	// Create result view from migration status
	if !dbStatus.IsEmpty() {
		status.Result = &ResultView{
			Verification: VerificationView{
				SrcTotal:   dbStatus.SrcTotal,
				DstTotal:   dbStatus.DstTotal,
				SrcPending: dbStatus.SrcPending,
				DstPending: dbStatus.DstPending,
				SrcFailed:  dbStatus.SrcFailed,
				DstFailed:  dbStatus.DstFailed,
			},
		}
	}

	return status, nil
}

func (m *Manager) InspectMigrationStatus(ctx context.Context, migrationID string) (migration.MigrationStatus, error) {
	// Resolve database path from migration ID
	dbPath, err := m.resolveDBPath("", migrationID)
	if err != nil {
		return migration.MigrationStatus{}, ErrMigrationNotFound
	}

	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return migration.MigrationStatus{}, ErrMigrationNotFound
	}

	return database.InspectMigrationStatusFromDB(ctx, m.logger, dbPath)
}

func (m *Manager) GetRecord(migrationID string) *MigrationRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.migrations[migrationID]
}

func (m *Manager) SetRecord(migrationID string, record *MigrationRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.migrations[migrationID] = record
}

func (m *Manager) recordToStatus(r *MigrationRecord) Status {
	status := Status{
		Migration: Migration{
			ID:            r.ID,
			SourceID:      r.SourceID,
			DestinationID: r.DestinationID,
			StartedAt:     r.StartedAt,
			Status:        r.Status,
		},
	}

	if r.CompletedAt != nil {
		status.CompletedAt = r.CompletedAt
	}
	if r.Error != "" {
		status.Error = r.Error
	}
	if r.Result != nil {
		status.Result = resultToView(r.Result)
	}

	return status
}
