package manager

import (
	"context"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

func (m *Manager) ListSFTPSavedHosts(_ context.Context) ([]corebridge.SFTPSavedHostSummary, error) {
	if m.apiDB == nil {
		return nil, fmt.Errorf("api database unavailable")
	}
	rows, err := m.apiDB.ListSFTPSavedHosts()
	if err != nil {
		return nil, err
	}
	out := make([]corebridge.SFTPSavedHostSummary, 0, len(rows))
	for _, row := range rows {
		out = append(out, sftpSavedHostSummary(row))
	}
	return out, nil
}

func (m *Manager) GetSFTPSavedHost(_ context.Context, id string) (corebridge.SFTPSavedHostDetail, error) {
	if m.apiDB == nil {
		return corebridge.SFTPSavedHostDetail{}, fmt.Errorf("api database unavailable")
	}
	row, err := m.apiDB.GetSFTPSavedHost(id)
	if err != nil {
		return corebridge.SFTPSavedHostDetail{}, err
	}
	return sftpSavedHostDetail(row), nil
}

func (m *Manager) UpsertSFTPSavedHost(_ context.Context, req corebridge.SFTPSavedHostUpsertRequest) (corebridge.SFTPSavedHostSummary, error) {
	if m.apiDB == nil {
		return corebridge.SFTPSavedHostSummary{}, fmt.Errorf("api database unavailable")
	}
	row, err := m.apiDB.UpsertSFTPSavedHost(apidb.SFTPSavedHost{
		ID:            strings.TrimSpace(req.ID),
		DisplayName:   req.DisplayName,
		Host:          req.Host,
		Port:          req.Port,
		Username:      req.Username,
		AuthMethod:    req.AuthMethod,
		Password:      req.Password,
		PrivateKey:    req.PrivateKey,
		KeyPassphrase: req.KeyPassphrase,
		HostKey:       req.HostKey,
	})
	if err != nil {
		return corebridge.SFTPSavedHostSummary{}, err
	}
	return sftpSavedHostSummary(row), nil
}

func (m *Manager) DeleteSFTPSavedHost(_ context.Context, id string) error {
	if m.apiDB == nil {
		return fmt.Errorf("api database unavailable")
	}
	return m.apiDB.DeleteSFTPSavedHost(id)
}

func sftpSavedHostSummary(row apidb.SFTPSavedHost) corebridge.SFTPSavedHostSummary {
	return corebridge.SFTPSavedHostSummary{
		ID:          row.ID,
		DisplayName: row.DisplayName,
		Host:        row.Host,
		Port:        row.Port,
		Username:    row.Username,
		AuthMethod:  row.AuthMethod,
		UpdatedAt:   row.UpdatedAt,
		LastUsedAt:  row.LastUsedAt,
	}
}

func sftpSavedHostDetail(row apidb.SFTPSavedHost) corebridge.SFTPSavedHostDetail {
	return corebridge.SFTPSavedHostDetail{
		SFTPSavedHostSummary: sftpSavedHostSummary(row),
		Password:             row.Password,
		PrivateKey:           row.PrivateKey,
		KeyPassphrase:        row.KeyPassphrase,
		HostKey:              row.HostKey,
	}
}
