package corebridge

import (
	"context"
	"errors"
)

var (
	ErrMigrationNotFound    = errors.New("migration not found")
	ErrServiceNotFound      = errors.New("service not found")
	ErrDatabaseNotAvailable = errors.New("database not available")
)

type Bridge interface {
	ListSources(ctx context.Context) ([]Source, error)
	ListChildren(ctx context.Context, req ListChildrenRequest) (ListChildrenResponse, error)
	CreateBrowseFolder(ctx context.Context, serviceID string, req CreateBrowseFolderRequest) (FolderDescriptor, error)
	DeleteBrowseNodes(ctx context.Context, serviceID string, req DeleteBrowseNodesRequest) (DeleteBrowseNodesResponse, error)
	ListDrives(ctx context.Context, serviceID string) ([]DriveInfo, error)
	GetStorageInfo(ctx context.Context, req GetStorageInfoRequest) (StorageInfo, error)
	MountDrive(ctx context.Context, serviceID string, req MountDriveRequest) (DriveInfo, error)
	SetRoot(ctx context.Context, req SetRootRequest) (SetRootResponse, error)
	StartMigration(ctx context.Context, req StartMigrationRequest) (Migration, error)
	GetMigrationStatus(ctx context.Context, id string) (Status, error)
	UploadByType(ctx context.Context, migrationID, uploadType string, data []byte, overwrite bool) (UploadMigrationDBResponse, error)
	ListMigrationDBs(ctx context.Context) ([]MigrationDBInfo, error)
	SubscribeProgress(ctx context.Context, id string) (<-chan ProgressEvent, func(), error)
	ToggleLogTerminal(ctx context.Context, enable bool, logAddress string) error
	ListAllMigrations(ctx context.Context, req ListMigrationsRequest) (ListMigrationsResponse, error)
	LoadMigration(ctx context.Context, migrationID string) (Migration, error)
	StopMigration(ctx context.Context, migrationID string) (Status, error)
	CheckPendingWork(ctx context.Context, migrationID string) (PendingWorkResponse, error)
	ChangePhase(ctx context.Context, migrationID string, phase string, req StartMigrationRequest) (Migration, error)
	GetBackgroundTasks(ctx context.Context, migrationID string) ([]BackgroundTask, error)
	GetRunningBackgroundTasks(ctx context.Context, migrationID string) ([]BackgroundTask, error)
	GetBackgroundTask(ctx context.Context, migrationID, taskID string) (*BackgroundTask, error)
	TriggerRetrySweep(ctx context.Context, migrationID string, config SweepConfigRequest) (SweepResponse, error)
	GetDeleteSummary(ctx context.Context, migrationID string) (DeleteSummaryResponse, error)
}

const (
	MigrationStatusRunning   = "running"
	MigrationStatusCompleted = "completed"
	MigrationStatusSuspended = "suspended"
	MigrationStatusFailed    = "failed"
)
