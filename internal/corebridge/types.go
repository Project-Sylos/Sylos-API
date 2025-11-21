package corebridge

import (
	"context"
	"errors"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
)

var (
	ErrMigrationNotFound = errors.New("migration not found")
	ErrServiceNotFound   = errors.New("service not found")
)

type ServiceType string

const (
	ServiceTypeLocal   ServiceType = "local"
	ServiceTypeSpectra ServiceType = "spectra"
)

const (
	RootRoleSource      = "source"
	RootRoleDestination = "destination"
)

type Source struct {
	ID          string            `json:"id"`
	DisplayName string            `json:"displayName"`
	Type        ServiceType       `json:"type"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type ListChildrenRequest struct {
	ServiceID  string
	Identifier string
	Role       string // "source" or "destination" - used to map "spectra" to the correct world
}

type FolderDescriptor struct {
	ID           string `json:"id"`
	ParentID     string `json:"parentId,omitempty"`
	ParentPath   string `json:"parentPath,omitempty"`
	DisplayName  string `json:"displayName,omitempty"`
	LocationPath string `json:"locationPath,omitempty"`
	LastUpdated  string `json:"lastUpdated,omitempty"`
	DepthLevel   int    `json:"depthLevel,omitempty"`
	Type         string `json:"type,omitempty"`
}

type ServiceSelection struct {
	ServiceID    string           `json:"serviceId"`
	ConnectionID string           `json:"connectionId,omitempty"`
	Root         FolderDescriptor `json:"root"`
}

type VerificationOptions struct {
	AllowPending  bool `json:"allowPending"`
	AllowFailed   bool `json:"allowFailed"`
	AllowNotOnSrc bool `json:"allowNotOnSrc"`
}

type MigrationOptions struct {
	MigrationID             string              `json:"migrationId,omitempty"`
	DatabasePath            string              `json:"databasePath,omitempty"`
	RemoveExistingDB        bool                `json:"removeExistingDatabase,omitempty"`
	UsePreseededDB          bool                `json:"usePreseededDatabase,omitempty"`
	SourceConnectionID      string              `json:"sourceConnectionId,omitempty"`
	DestinationConnectionID string              `json:"destinationConnectionId,omitempty"`
	WorkerCount             int                 `json:"workerCount,omitempty"`
	MaxRetries              int                 `json:"maxRetries,omitempty"`
	CoordinatorLead         int                 `json:"coordinatorLead,omitempty"`
	LogAddress              string              `json:"logAddress,omitempty"`
	LogLevel                string              `json:"logLevel,omitempty"`
	EnableLoggingTerminal   bool                `json:"enableLoggingTerminal,omitempty"`
	StartupDelaySec         int                 `json:"startupDelaySeconds,omitempty"`
	ProgressTickMillis      int                 `json:"progressTickMillis,omitempty"`
	Verification            VerificationOptions `json:"verification,omitempty"`
}

type StartMigrationRequest struct {
	MigrationID string           `json:"migrationId,omitempty"`
	Options     MigrationOptions `json:"options"`
}

type SetRootRequest struct {
	MigrationID  string           `json:"migrationId,omitempty"`
	Role         string           `json:"role"`
	ServiceID    string           `json:"serviceId"`
	ConnectionID string           `json:"connectionId,omitempty"`
	Root         FolderDescriptor `json:"root"`
}

type SetRootResponse struct {
	MigrationID             string                     `json:"migrationId"`
	Role                    string                     `json:"role"`
	Ready                   bool                       `json:"ready"`
	DatabasePath            string                     `json:"databasePath,omitempty"`
	RootSummary             *migration.RootSeedSummary `json:"rootSummary,omitempty"`
	SourceConnectionID      string                     `json:"sourceConnectionId,omitempty"`
	DestinationConnectionID string                     `json:"destinationConnectionId,omitempty"`
}

type Migration struct {
	ID            string    `json:"id"`
	SourceID      string    `json:"sourceId"`
	DestinationID string    `json:"destinationId"`
	StartedAt     time.Time `json:"startedAt"`
	Status        string    `json:"status"`
}

type Status struct {
	Migration
	CompletedAt *time.Time  `json:"completedAt,omitempty"`
	Error       string      `json:"error,omitempty"`
	Result      *ResultView `json:"result,omitempty"`
}

type ResultView struct {
	RootSummary  RootSummaryView  `json:"rootSummary"`
	Runtime      RuntimeStatsView `json:"runtime"`
	Verification VerificationView `json:"verification"`
}

type RootSummaryView struct {
	SrcRoots int `json:"srcRoots"`
	DstRoots int `json:"dstRoots"`
}

type QueueStatsView struct {
	Name         string `json:"name"`
	Round        int    `json:"round"`
	Pending      int    `json:"pending"`
	InProgress   int    `json:"inProgress"`
	TotalTracked int    `json:"totalTracked"`
	Workers      int    `json:"workers"`
}

type RuntimeStatsView struct {
	Duration string         `json:"duration"`
	Src      QueueStatsView `json:"src"`
	Dst      QueueStatsView `json:"dst"`
}

type VerificationView struct {
	SrcTotal    int `json:"srcTotal"`
	DstTotal    int `json:"dstTotal"`
	SrcPending  int `json:"srcPending"`
	DstPending  int `json:"dstPending"`
	SrcFailed   int `json:"srcFailed"`
	DstFailed   int `json:"dstFailed"`
	DstNotOnSrc int `json:"dstNotOnSrc"`
}

type QueueStatsSnapshot struct {
	Round        int `json:"round"`
	Pending      int `json:"pending"`
	InProgress   int `json:"inProgress"`
	TotalTracked int `json:"totalTracked"`
	Workers      int `json:"workers"`
}

type ProgressEvent struct {
	Event       string             `json:"event"`
	Timestamp   time.Time          `json:"timestamp"`
	Migration   Status             `json:"migration"`
	Source      QueueStatsSnapshot `json:"source"`
	Destination QueueStatsSnapshot `json:"destination"`
}

type UploadMigrationDBRequest struct {
	Filename  string `json:"filename"`
	Overwrite bool   `json:"overwrite,omitempty"`
}

type UploadMigrationDBResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Path    string `json:"path,omitempty"`
}

type MigrationDBInfo struct {
	Filename   string    `json:"filename"`
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	ModifiedAt time.Time `json:"modifiedAt"`
}

// MigrationMetadata represents minimal metadata for a migration
// The Migration Engine's YAML config file contains all the detailed state
type MigrationMetadata struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	ConfigPath string    `json:"configPath"` // Path to the Migration Engine's YAML config file
	CreatedAt  time.Time `json:"createdAt"`
}

type Bridge interface {
	ListSources(ctx context.Context) ([]Source, error)
	ListChildren(ctx context.Context, req ListChildrenRequest) (fsservices.ListResult, error)
	SetRoot(ctx context.Context, req SetRootRequest) (SetRootResponse, error)
	StartMigration(ctx context.Context, req StartMigrationRequest) (Migration, error)
	GetMigrationStatus(ctx context.Context, id string) (Status, error)
	InspectMigrationStatus(ctx context.Context, migrationID string) (migration.MigrationStatus, error)
	InspectMigrationStatusFromDB(ctx context.Context, dbPath string) (migration.MigrationStatus, error)
	UploadMigrationDB(ctx context.Context, filename string, data []byte, overwrite bool) (UploadMigrationDBResponse, error)
	ListMigrationDBs(ctx context.Context) ([]MigrationDBInfo, error)
	SubscribeProgress(ctx context.Context, id string) (<-chan ProgressEvent, func(), error)
	ToggleLogTerminal(ctx context.Context, enable bool, logAddress string) error
	ListAllMigrations(ctx context.Context) ([]MigrationMetadata, error)
	LoadMigration(ctx context.Context, migrationID string) (Migration, error)
	StopMigration(ctx context.Context, migrationID string) (Status, error)
}

const (
	MigrationStatusRunning   = "running"
	MigrationStatusCompleted = "completed"
	MigrationStatusSuspended = "suspended"
	MigrationStatusFailed    = "failed"
)
