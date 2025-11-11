package corebridge

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

var (
	ErrMigrationNotFound = errors.New("migration not found")
)

type Source struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	Description string `json:"description,omitempty"`
}

type StartMigrationRequest struct {
	SourceID string                 `json:"sourceId"`
	Options  map[string]interface{} `json:"options,omitempty"`
}

type Migration struct {
	ID        string    `json:"id"`
	SourceID  string    `json:"sourceId"`
	StartedAt time.Time `json:"startedAt"`
	Status    string    `json:"status"`
}

type Status struct {
	ID            string    `json:"id"`
	SourceID      string    `json:"sourceId"`
	Status        string    `json:"status"`
	Progress      int       `json:"progress"`
	LastUpdatedAt time.Time `json:"lastUpdatedAt"`
}

type Bridge interface {
	ListSources(ctx context.Context) ([]Source, error)
	StartMigration(ctx context.Context, req StartMigrationRequest) (Migration, error)
	GetMigrationStatus(ctx context.Context, id string) (Status, error)
}

type MockBridge struct {
	logger     zerolog.Logger
	migrations map[string]Status
	mu         sync.RWMutex
}

func NewMockBridge(logger zerolog.Logger) Bridge {
	return &MockBridge{
		logger:     logger,
		migrations: make(map[string]Status),
	}
}

func (b *MockBridge) ListSources(ctx context.Context) ([]Source, error) {
	return []Source{
		{ID: "local-filesystem", DisplayName: "Local File System"},
		{ID: "s3-bucket", DisplayName: "Amazon S3 Bucket"},
	}, nil
}

func (b *MockBridge) StartMigration(ctx context.Context, req StartMigrationRequest) (Migration, error) {
	now := time.Now().UTC()
	id := now.Format("20060102-150405")

	status := Status{
		ID:            id,
		SourceID:      req.SourceID,
		Status:        "running",
		Progress:      0,
		LastUpdatedAt: now,
	}

	b.mu.Lock()
	b.migrations[id] = status
	b.mu.Unlock()

	b.logger.Info().
		Str("migration_id", id).
		Str("source_id", req.SourceID).
		Msg("mock migration started")

	return Migration{
		ID:        id,
		SourceID:  req.SourceID,
		StartedAt: now,
		Status:    status.Status,
	}, nil
}

func (b *MockBridge) GetMigrationStatus(ctx context.Context, id string) (Status, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	status, ok := b.migrations[id]
	if !ok {
		return Status{}, ErrMigrationNotFound
	}

	return status, nil
}
