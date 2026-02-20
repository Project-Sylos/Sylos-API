package migrations

import (
	"fmt"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	corebridgeDB "codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	"codeberg.org/Sylos/Sylos-API/pkg/config"
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
	SkipListener            *bool
	StartupDelaySec         int
	ProgressTickMillis      int
	Verification            VerificationOptions
}

// VerificationOptions represents verification options
type VerificationOptions struct {
	AllowPending  bool
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
	logger                  zerolog.Logger
	cfg                     config.Config
	migrations              map[string]*MigrationRecord
	subscribers             map[string]map[string]chan ProgressEvent
	mu                      sync.RWMutex
	serviceMgr              *services.ServiceManager
	rootsMgr                *roots.Manager
	metadataMgr             *metadata.Manager
	resolveDBPath           func(path, migrationID string) (string, error)
	dbPool                  *DBPool                                                       // API-owned database connection pool
	duckdbPool              *corebridgeDB.DuckDBPool                                       // DuckDB connection pool
	startBgTaskFunc         func(migrationID string, taskType string, path string) string  // Callback to start background tasks
	startBgTaskCompleteFunc func(migrationID, taskID string)                               // Callback to complete background tasks
	startBgTaskFailFunc     func(migrationID, taskID string, err error)                    // Callback to fail background tasks
}

// MigrationRecord holds the runtime state of a migration
type MigrationRecord struct {
	ID            string
	SourceID      string
	DestinationID string
	Status        string
	StartedAt     time.Time
	CompletedAt   *time.Time
	Result        *migration.Result
	Error         string
	Controller    *migration.MigrationController // Controller for programmatic shutdown
	DB            *db.DB                         // DB instance for querying logs/metrics (shared with migration engine)
	DuckDBPath    string                         // Path to migration DB (for path review)
}

const (
	MigrationStatusRunning   = "running"
	MigrationStatusCompleted = "completed"
	MigrationStatusSuspended = "suspended"
	MigrationStatusFailed    = "failed"
)

// RunParamsFromRoots holds prepared context for starting a migration from a roots plan.
type RunParamsFromRoots struct {
	DbPath             string
	ConfigPath         string
	Opts               MigrationOptions
	SpectraConfigPath  string
}

// RunParamsFromConfig holds prepared context for starting a migration from a pre-built config (e.g. uploaded DB + YAML or LoadMigrationFromConfigPath).
type RunParamsFromConfig struct {
	Cfg  migration.Config
	Opts MigrationOptions
}

// RunParams is the prepared context for runMigrationEngine. Exactly one of FromRoots or FromConfig is set.
type RunParams struct {
	FromRoots  *RunParamsFromRoots
	FromConfig *RunParamsFromConfig
}
