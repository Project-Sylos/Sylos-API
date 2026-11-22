package oauthbuiltin

import (
	"os"
	"strings"

	"codeberg.org/Sylos/Sylos-API/pkg/oauthobscure"
)

// ApplyObscuredEnvFile loads oauth.env + key and fills empty ldflag package vars.
// Missing files return nil (release binaries without the files nearby).
func ApplyObscuredEnvFile(envPath, keyPath string) error {
	if _, err := os.Stat(envPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if _, err := os.Stat(keyPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	key, err := oauthobscure.LoadKeyFile(keyPath)
	if err != nil {
		return err
	}
	vals, _, err := oauthobscure.LoadEnvFile(envPath, key)
	if err != nil {
		return err
	}
	setIfEmpty := func(dst *string, src string) {
		if strings.TrimSpace(*dst) == "" && strings.TrimSpace(src) != "" {
			*dst = src
		}
	}
	setIfEmpty(&DropboxClientID, vals.DropboxClientID)
	setIfEmpty(&DropboxClientSecret, vals.DropboxClientSecret)
	setIfEmpty(&BoxClientID, vals.BoxClientID)
	setIfEmpty(&BoxClientSecret, vals.BoxClientSecret)
	setIfEmpty(&OneDriveClientID, vals.OneDriveClientID)
	setIfEmpty(&OneDriveClientSecret, vals.OneDriveClientSecret)
	setIfEmpty(&OneDriveTenantID, vals.OneDriveTenantID)
	setIfEmpty(&SharePointClientID, vals.SharePointClientID)
	setIfEmpty(&SharePointClientSecret, vals.SharePointClientSecret)
	setIfEmpty(&SharePointTenantID, vals.SharePointTenantID)
	return nil
}
