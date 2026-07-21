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
	sftpfs "codeberg.org/Sylos/Sylos-FS/pkg/fs/sftp"
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
			AuthType:      authTypeForProvider(providerID),
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

func authTypeForProvider(providerID string) string {
	if providerID == cloud.ProviderSFTP {
		return "credentials_form"
	}
	return "oauth_server_exchange"
}

func (m *Manager) ProbeSFTPHostKey(_ context.Context, providerID string, req corebridge.SFTPHostKeyProbeRequest) (corebridge.SFTPHostKeyProbeResponse, error) {
	if providerID != cloud.ProviderSFTP {
		return corebridge.SFTPHostKeyProbeResponse{}, fmt.Errorf("provider %q does not support host key probe", providerID)
	}
	result, err := sftpfs.FetchServerHostKey(req.Host, req.Port)
	if err != nil {
		return corebridge.SFTPHostKeyProbeResponse{}, err
	}
	resp := corebridge.SFTPHostKeyProbeResponse{
		HostKey:     result.HostKey,
		Fingerprint: result.Fingerprint,
	}
	if m.apiDB != nil {
		if pinned, ok, lookupErr := m.apiDB.LookupSFTPKnownHost(req.Host, req.Port); lookupErr == nil && ok {
			resp.Trusted = true
			if pinned.HostKey != result.HostKey {
				resp.HostKeyChanged = true
			} else {
				resp.HostKey = pinned.HostKey
				resp.Fingerprint = pinned.Fingerprint
			}
		}
	}
	return resp, nil
}

func (m *Manager) PostProviderCredentials(ctx context.Context, providerID, connectionID string, req corebridge.SFTPCredentialsRequest) (corebridge.ConnectionStatus, error) {
	if providerID != cloud.ProviderSFTP {
		return corebridge.ConnectionStatus{}, fmt.Errorf("provider %q does not accept form credentials", providerID)
	}
	rec, ok := m.connMgr.Get(connectionID)
	if !ok {
		return corebridge.ConnectionStatus{}, fmt.Errorf("connection %q not found", connectionID)
	}
	if rec.ProviderID != providerID {
		return corebridge.ConnectionStatus{}, fmt.Errorf("connection provider mismatch")
	}

	stored := cloud.StoredCredentialsFromSFTP(
		req.Host,
		req.Username,
		req.Password,
		req.PrivateKey,
		req.KeyPassphrase,
		req.HostKey,
		req.Port,
	)
	if err := stored.ValidateSFTP(); err != nil {
		return corebridge.ConnectionStatus{}, err
	}
	credsJSON, err := json.Marshal(stored)
	if err != nil {
		return corebridge.ConnectionStatus{}, err
	}

	var mig *migration.Migration
	if rec.MigrationID != "" {
		mig, err = m.GetMigration(context.Background(), rec.MigrationID)
		if err != nil && err != corebridge.ErrMigrationNotFound {
			return corebridge.ConnectionStatus{}, err
		}
	}

	_, err = m.serviceMgr.FS.RegisterCloudConnection(fslib.CloudConnectionOptions{
		ProviderID:      providerID,
		ConnectionID:    connectionID,
		CredentialsJSON: credsJSON,
	})
	if err != nil {
		return corebridge.ConnectionStatus{}, err
	}

	if m.apiDB != nil {
		fp, fpErr := sftpfs.FingerprintHostKey(req.HostKey)
		if fpErr != nil {
			m.logger.Warn().Err(fpErr).Str("host", req.Host).Msg("fingerprint sftp host key for known hosts")
		} else if pinErr := m.apiDB.UpsertSFTPKnownHost(req.Host, req.Port, req.HostKey, fp); pinErr != nil {
			m.logger.Warn().Err(pinErr).Str("host", req.Host).Msg("persist sftp known host")
		}
	}

	if mig != nil {
		if err := m.persistOAuthCredentials(mig, connectionID, credsJSON); err != nil {
			return corebridge.ConnectionStatus{}, fmt.Errorf("persist sftp credentials: %w", err)
		}
	}

	m.attachAccountIdentity(ctx, &rec, connectionID)
	m.connMgr.Set(connectionID, rec)

	return m.connectionStatusFromRecord(providerID, connectionID, rec, true), nil
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
		mig, err = m.GetMigration(context.Background(), rec.MigrationID)
		if err != nil && err != corebridge.ErrMigrationNotFound {
			return corebridge.ConnectionStatus{}, err
		}
	}

	_, err = m.serviceMgr.FS.RegisterCloudConnection(fslib.CloudConnectionOptions{
		ProviderID:         providerID,
		ConnectionID:       connectionID,
		CredentialsJSON:    credsJSON,
		AccessToken:        req.AccessToken,
		ExpiresInSec:       req.ExpiresIn,
		PersistCredentials: m.cloudCredentialsPersistHook(mig, connectionID),
	})
	if err != nil {
		return corebridge.ConnectionStatus{}, err
	}

	if mig != nil {
		if err := m.persistOAuthCredentials(mig, connectionID, credsJSON); err != nil {
			return corebridge.ConnectionStatus{}, fmt.Errorf("persist oauth credentials: %w", err)
		}
	} else if rec.MigrationID != "" {
		m.logger.Warn().
			Str("connection_id", connectionID).
			Str("migration_id", rec.MigrationID).
			Msg("oauth tokens not persisted yet; will store on set-root when migration DB exists")
	}

	expiresAt := time.Time{}
	if req.ExpiresIn > 0 && req.AccessToken != "" {
		expiresAt = time.Now().Add(time.Duration(req.ExpiresIn) * time.Second)
	}
	rec.AccessToken = req.AccessToken
	rec.ExpiresAt = expiresAt
	m.attachAccountIdentity(ctx, &rec, connectionID)
	m.connMgr.Set(connectionID, rec)

	return m.connectionStatusFromRecord(providerID, connectionID, rec, true), nil
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
	if rec.ProviderID != providerID {
		return corebridge.ConnectionStatus{ConnectionID: connectionID, ProviderID: providerID, Valid: false}, nil
	}
	valid := m.cloudConnectionValid(providerID, connectionID)
	if valid && rec.AccountEmail == "" && rec.AccountDisplayName == "" {
		m.attachAccountIdentity(ctx, &rec, connectionID)
		m.connMgr.Set(connectionID, rec)
	}
	return m.connectionStatusFromRecord(providerID, connectionID, rec, valid), nil
}

func (m *Manager) attachAccountIdentity(ctx context.Context, rec *connections.Record, connectionID string) {
	if rec == nil {
		return
	}
	identity, err := m.serviceMgr.FS.CloudAccountIdentity(ctx, connectionID)
	if err != nil || (identity.Email == "" && identity.DisplayName == "") {
		return
	}
	rec.AccountEmail = identity.Email
	rec.AccountDisplayName = identity.DisplayName
}

func (m *Manager) connectionStatusFromRecord(
	providerID, connectionID string,
	rec connections.Record,
	valid bool,
) corebridge.ConnectionStatus {
	return corebridge.ConnectionStatus{
		ConnectionID:       connectionID,
		ProviderID:         providerID,
		Valid:              valid,
		ExpiresAt:          rec.ExpiresAt,
		AccountEmail:       rec.AccountEmail,
		AccountDisplayName: rec.AccountDisplayName,
	}
}

func (m *Manager) cloudConnectionValid(providerID, connectionID string) bool {
	if !m.serviceMgr.FS.HasConnection(connectionID) {
		return false
	}
	if connProvider, ok := m.serviceMgr.FS.CloudConnectionProvider(connectionID); ok && connProvider != providerID {
		return false
	}
	return true
}

func (m *Manager) RevokeProviderConnection(ctx context.Context, providerID, connectionID string) error {
	migrationID := ""
	if rec, ok := m.connMgr.Get(connectionID); ok && rec.MigrationID != "" {
		migrationID = rec.MigrationID
	}
	m.connMgr.Delete(connectionID)
	if err := m.serviceMgr.FS.RevokeCloudConnection(connectionID, ""); err != nil {
		return err
	}
	if migrationID != "" {
		if mig, err := m.GetMigration(context.Background(), migrationID); err == nil && mig != nil {
			_ = mig.DeleteOAuthCredentials(connectionID)
		}
	}
	return nil
}

func (m *Manager) ListProviderRoots(ctx context.Context, providerID, connectionID string) ([]cloud.Root, error) {
	if rec, ok := m.connMgr.Get(connectionID); ok && rec.ProviderID != providerID {
		return nil, fmt.Errorf("connection %s belongs to provider %q, not %q", connectionID, rec.ProviderID, providerID)
	}
	return m.serviceMgr.FS.ListCloudRoots(ctx, providerID, connectionID)
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
