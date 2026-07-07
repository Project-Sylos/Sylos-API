package manager

import (
	"encoding/json"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-FS/pkg/cloud"
)

// persistOAuthCredentials stores plaintext OAuth refresh credentials in the encrypted migration DB.
func (m *Manager) persistOAuthCredentials(mig *migration.Migration, connectionID string, credsJSON []byte) error {
	if mig == nil || connectionID == "" || len(credsJSON) == 0 {
		return nil
	}
	return mig.UpsertOAuthCredentials(connectionID, credsJSON)
}

// loadStoredCloudCredentials reads OAuth refresh credentials from the migration DB.
func (m *Manager) loadStoredCloudCredentials(_ string, mig *migration.Migration, binding migration.FSCredentialBinding) (cloud.StoredCredentials, error) {
	if binding.ConnectionID == "" {
		return cloud.StoredCredentials{}, fmt.Errorf("connection id required")
	}
	if mig == nil {
		return cloud.StoredCredentials{}, fmt.Errorf("no stored credentials for connection %q", binding.ConnectionID)
	}
	raw, err := mig.GetOAuthCredentials(binding.ConnectionID)
	if err != nil || len(raw) == 0 {
		return cloud.StoredCredentials{}, fmt.Errorf("no stored credentials for connection %q", binding.ConnectionID)
	}
	var stored cloud.StoredCredentials
	if err := json.Unmarshal(raw, &stored); err != nil {
		return cloud.StoredCredentials{}, fmt.Errorf("decode stored credentials: %w", err)
	}
	return stored, nil
}
