package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/connections"
	oauthpkg "codeberg.org/Sylos/Sylos-API/internal/corebridge/oauth"
	"codeberg.org/Sylos/Sylos-FS/pkg/cloud"
	fslib "codeberg.org/Sylos/Sylos-FS/pkg/fs"
	"github.com/oklog/ulid/v2"
)

func (m *Manager) ListProviders(_ context.Context) ([]corebridge.ProviderDescriptor, error) {
	out := make([]corebridge.ProviderDescriptor, 0, len(m.cfg.Providers))
	for providerID, cfg := range m.cfg.Providers {
		if !cfg.Enabled {
			continue
		}
		out = append(out, corebridge.ProviderDescriptor{
			ID:            providerID,
			DisplayName:   cfg.DisplayName,
			ServiceID:     cfg.ServiceID,
			AuthType:      "oauth_server_exchange",
			Scopes:        cfg.Scopes,
			OAuthClientID: m.oauthClientID(providerID),
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

	var mig *migration.Migration
	if rec.MigrationID != "" {
		mig, err = m.getEngineMigration(rec.MigrationID)
		if err != nil && err != corebridge.ErrMigrationNotFound {
			return corebridge.ConnectionStatus{}, err
		}
	}

	_, err = m.serviceMgr.RegisterCloudConnection(fslib.CloudConnectionOptions{
		ProviderID:      providerID,
		ConnectionID:    connectionID,
		CredentialsJSON: credsJSON,
		AccessToken:     req.AccessToken,
		ExpiresInSec:    req.ExpiresIn,
	})
	if err != nil {
		return corebridge.ConnectionStatus{}, err
	}

	if mig != nil {
		if err := m.persistOAuthCredentials(mig, connectionID, credsJSON); err != nil {
			return corebridge.ConnectionStatus{}, fmt.Errorf("persist oauth credentials: %w", err)
		}
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

func (m *Manager) ExchangeProviderOAuthCode(ctx context.Context, providerID, connectionID string, req corebridge.OAuthExchangeRequest) (corebridge.ConnectionStatus, error) {
	if req.Code == "" || req.RedirectURI == "" {
		return corebridge.ConnectionStatus{}, fmt.Errorf("code and redirect_uri are required")
	}

	creds, err := m.oauthProviderCredentials(providerID)
	if err != nil {
		return corebridge.ConnectionStatus{}, err
	}

	tokens, err := oauthpkg.ExchangeAuthCode(providerID, creds, req.Code, req.RedirectURI)
	if err != nil {
		return corebridge.ConnectionStatus{}, err
	}
	if tokens.RefreshToken == "" {
		return corebridge.ConnectionStatus{}, fmt.Errorf("provider did not return a refresh token")
	}

	scopes := req.Scopes
	if tokens.Scope != "" {
		scopes = splitScopes(tokens.Scope)
	}

	return m.PostProviderTokens(ctx, providerID, connectionID, corebridge.OAuthTokenRequest{
		AccessToken:  tokens.AccessToken,
		RefreshToken: tokens.RefreshToken,
		ExpiresIn:    tokens.ExpiresIn,
		Scopes:       scopes,
		ClientID:     creds.ClientID,
		ClientSecret: creds.ClientSecret,
	})
}

func splitScopes(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ' ' || r == ','
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
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
	migrationID := ""
	if rec, ok := m.connMgr.Get(connectionID); ok && rec.MigrationID != "" {
		migrationID = rec.MigrationID
	}
	m.connMgr.Delete(connectionID)
	if err := m.serviceMgr.RevokeCloudConnection(connectionID, ""); err != nil {
		return err
	}
	if migrationID != "" {
		if mig, err := m.getEngineMigration(migrationID); err == nil && mig != nil {
			_ = mig.DeleteOAuthCredentials(connectionID)
		}
	}
	return nil
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

func (m *Manager) CreateProviderFolder(ctx context.Context, providerID, connectionID string, req corebridge.CreateBrowseFolderRequest) (corebridge.FolderDescriptor, error) {
	def, err := m.serviceMgr.GetServiceDefinitionByProvider(providerID)
	if err != nil {
		return corebridge.FolderDescriptor{}, corebridge.ErrServiceNotFound
	}
	req.ConnectionID = connectionID
	return m.CreateBrowseFolder(ctx, def.ID, req)
}

func (m *Manager) DeleteProviderNodes(ctx context.Context, providerID, connectionID string, req corebridge.DeleteBrowseNodesRequest) (corebridge.DeleteBrowseNodesResponse, error) {
	def, err := m.serviceMgr.GetServiceDefinitionByProvider(providerID)
	if err != nil {
		return corebridge.DeleteBrowseNodesResponse{}, corebridge.ErrServiceNotFound
	}
	req.ConnectionID = connectionID
	return m.DeleteBrowseNodes(ctx, def.ID, req)
}
