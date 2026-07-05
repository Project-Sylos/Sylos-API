package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/connections"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-FS/pkg/cloud"
	fslib "codeberg.org/Sylos/Sylos-FS/pkg/fs"
	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
	"github.com/oklog/ulid/v2"
)

func (m *Manager) ListProviders(_ context.Context) ([]corebridge.ProviderDescriptor, error) {
	out := make([]corebridge.ProviderDescriptor, 0, len(m.cfg.Providers))
	for providerID, cfg := range m.cfg.Providers {
		if !cfg.Enabled {
			continue
		}
		out = append(out, corebridge.ProviderDescriptor{
			ID:          providerID,
			DisplayName: cfg.DisplayName,
			ServiceID:   cfg.ServiceID,
			AuthType:    "oauth_ui_tokens",
			Scopes:      cfg.Scopes,
		})
	}
	return out, nil
}

func (m *Manager) CreateProviderConnection(ctx context.Context, providerID, migrationID string) (corebridge.ConnectionResponse, error) {
	cfg, ok := m.cfg.Providers[providerID]
	if !ok || !cfg.Enabled {
		return corebridge.ConnectionResponse{}, fmt.Errorf("provider %q not enabled", providerID)
	}
	connectionID := ulid.Make().String()
	m.connMgr.Set(connectionID, connections.Record{
		ProviderID:  providerID,
		ServiceID:   cfg.ServiceID,
		MigrationID: migrationID,
	})
	return corebridge.ConnectionResponse{ConnectionID: connectionID, ProviderID: providerID}, nil
}

func (m *Manager) PostProviderTokens(ctx context.Context, providerID, connectionID string, req corebridge.OAuthTokenRequest) (corebridge.ConnectionStatus, error) {
	rec, ok := m.connMgr.Get(connectionID)
	if !ok {
		return corebridge.ConnectionStatus{}, fmt.Errorf("connection %q not found", connectionID)
	}
	if rec.ProviderID != providerID {
		return corebridge.ConnectionStatus{}, fmt.Errorf("connection provider mismatch")
	}
	if req.RefreshToken == "" {
		return corebridge.ConnectionStatus{}, fmt.Errorf("refresh_token is required")
	}

	credsJSON, err := buildStoredCredentials(providerID, req)
	if err != nil {
		return corebridge.ConnectionStatus{}, err
	}

	var masterKey []byte
	migrationDir := ""
	if rec.MigrationID != "" {
		migrationDir, err = m.migrationDirFor(rec.MigrationID)
		if err != nil {
			return corebridge.ConnectionStatus{}, err
		}
		absDir, err := filepath.Abs(database.GetMigrationDir(m.cfg.Runtime.DataDir, rec.MigrationID))
		if err != nil {
			return corebridge.ConnectionStatus{}, err
		}
		mig, err := m.engineMgr.GetMigration(rec.MigrationID, absDir)
		if err != nil {
			return corebridge.ConnectionStatus{}, err
		}
		if mig != nil {
			masterKey, err = mig.EnsureEnvelopeMasterKey()
			if err != nil {
				return corebridge.ConnectionStatus{}, err
			}
		}
	}
	if len(masterKey) == 0 {
		masterKey, err = credentials.GenerateMasterKey()
		if err != nil {
			return corebridge.ConnectionStatus{}, err
		}
	}

	_, err = m.serviceMgr.RegisterCloudConnection(fslib.CloudConnectionOptions{
		ProviderID:      providerID,
		ConnectionID:    connectionID,
		MigrationDir:    migrationDir,
		MasterKey:       masterKey,
		CredentialsJSON: credsJSON,
		AccessToken:     req.AccessToken,
		ExpiresInSec:    req.ExpiresIn,
	})
	if err != nil {
		return corebridge.ConnectionStatus{}, err
	}

	expiresAt := time.Time{}
	if req.ExpiresIn > 0 && req.AccessToken != "" {
		expiresAt = time.Now().Add(time.Duration(req.ExpiresIn) * time.Second)
	}
	rec.AccessToken = req.AccessToken
	rec.ExpiresAt = expiresAt
	m.connMgr.Set(connectionID, rec)

	return corebridge.ConnectionStatus{
		ConnectionID: connectionID,
		ProviderID:   providerID,
		Valid:        true,
		ExpiresAt:    expiresAt,
	}, nil
}

func buildStoredCredentials(providerID string, req corebridge.OAuthTokenRequest) ([]byte, error) {
	stored := cloud.StoredCredentialsFromOAuth(providerID, req.RefreshToken, req.ClientID, req.ClientSecret, req.Scopes)
	return json.Marshal(stored)
}

func (m *Manager) ProviderConnectionStatus(ctx context.Context, providerID, connectionID string) (corebridge.ConnectionStatus, error) {
	rec, ok := m.connMgr.Get(connectionID)
	if !ok {
		return corebridge.ConnectionStatus{ConnectionID: connectionID, ProviderID: providerID, Valid: false}, nil
	}
	valid := m.serviceMgr.HasCloudConnection(connectionID)
	return corebridge.ConnectionStatus{
		ConnectionID: connectionID,
		ProviderID:   providerID,
		Valid:        valid,
		ExpiresAt:    rec.ExpiresAt,
	}, nil
}

func (m *Manager) RevokeProviderConnection(ctx context.Context, providerID, connectionID string) error {
	migrationDir := ""
	if rec, ok := m.connMgr.Get(connectionID); ok && rec.MigrationID != "" {
		dir, err := m.migrationDirFor(rec.MigrationID)
		if err == nil {
			migrationDir = dir
		}
	}
	m.connMgr.Delete(connectionID)
	return m.serviceMgr.RevokeCloudConnection(connectionID, migrationDir)
}

func (m *Manager) ListProviderRoots(ctx context.Context, providerID, connectionID string) ([]cloud.Root, error) {
	return m.serviceMgr.ListCloudRoots(ctx, providerID, connectionID)
}

func (m *Manager) ListProviderChildren(ctx context.Context, providerID, connectionID, identifier, rootType, driveID string, offset, limit int, foldersOnly bool) (corebridge.ListChildrenResponse, error) {
	_ = providerID
	result, pagination, err := m.serviceMgr.ListCloudChildren(ctx, connectionID, identifier, rootType, driveID, offset, limit, foldersOnly)
	if err != nil {
		return corebridge.ListChildrenResponse{}, err
	}
	return corebridge.ListChildrenResponse{
		Folders: result.Folders,
		Files:   result.Files,
		Pagination: corebridge.PaginationInfo{
			Offset:       pagination.Offset,
			Limit:        pagination.Limit,
			Total:        pagination.Total,
			TotalFolders: pagination.TotalFolders,
			TotalFiles:   pagination.TotalFiles,
			HasMore:      pagination.HasMore,
		},
	}, nil
}
