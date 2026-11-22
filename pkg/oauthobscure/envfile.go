package oauthobscure

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// EnvValues holds revealed OAuth app fields from oauth.env.
type EnvValues struct {
	DropboxClientID        string
	DropboxClientSecret    string
	BoxClientID            string
	BoxClientSecret        string
	OneDriveClientID       string
	OneDriveClientSecret   string
	OneDriveTenantID       string
	SharePointClientID     string
	SharePointClientSecret string
	SharePointTenantID     string
}

var envKeys = []string{
	"SYLOS_DROPBOX_CLIENT_ID",
	"SYLOS_DROPBOX_CLIENT_SECRET",
	"SYLOS_BOX_CLIENT_ID",
	"SYLOS_BOX_CLIENT_SECRET",
	"SYLOS_ONEDRIVE_CLIENT_ID",
	"SYLOS_ONEDRIVE_CLIENT_SECRET",
	"SYLOS_ONEDRIVE_TENANT_ID",
	"SYLOS_SHAREPOINT_CLIENT_ID",
	"SYLOS_SHAREPOINT_CLIENT_SECRET",
	"SYLOS_SHAREPOINT_TENANT_ID",
}

// LoadEnvFile parses KEY?=value / KEY=value lines and reveals enc: values with key.
func LoadEnvFile(path string, key []byte) (EnvValues, map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return EnvValues{}, nil, err
	}
	defer f.Close()

	raw := make(map[string]string)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := splitEnvAssignment(line)
		if !ok {
			continue
		}
		raw[k] = v
	}
	if err := sc.Err(); err != nil {
		return EnvValues{}, nil, err
	}

	revealed := make(map[string]string, len(raw))
	for _, name := range envKeys {
		v := strings.TrimSpace(raw[name])
		if v == "" {
			continue
		}
		plain, err := Reveal(key, v)
		if err != nil {
			return EnvValues{}, nil, fmt.Errorf("%s: %w", name, err)
		}
		revealed[name] = plain
	}

	return EnvValues{
		DropboxClientID:        revealed["SYLOS_DROPBOX_CLIENT_ID"],
		DropboxClientSecret:    revealed["SYLOS_DROPBOX_CLIENT_SECRET"],
		BoxClientID:            revealed["SYLOS_BOX_CLIENT_ID"],
		BoxClientSecret:        revealed["SYLOS_BOX_CLIENT_SECRET"],
		OneDriveClientID:       revealed["SYLOS_ONEDRIVE_CLIENT_ID"],
		OneDriveClientSecret:   revealed["SYLOS_ONEDRIVE_CLIENT_SECRET"],
		OneDriveTenantID:       revealed["SYLOS_ONEDRIVE_TENANT_ID"],
		SharePointClientID:     revealed["SYLOS_SHAREPOINT_CLIENT_ID"],
		SharePointClientSecret: revealed["SYLOS_SHAREPOINT_CLIENT_SECRET"],
		SharePointTenantID:     revealed["SYLOS_SHAREPOINT_TENANT_ID"],
	}, raw, nil
}

func splitEnvAssignment(line string) (key, value string, ok bool) {
	for _, sep := range []string{"?=", "="} {
		i := strings.Index(line, sep)
		if i <= 0 {
			continue
		}
		key = strings.TrimSpace(line[:i])
		value = strings.TrimSpace(line[i+len(sep):])
		if key == "" {
			return "", "", false
		}
		return key, value, true
	}
	return "", "", false
}

// EncryptEnvFileInPlace rewrites path so every non-empty value is enc:… (already-enc values are left alone).
func EncryptEnvFileInPlace(path string, key []byte) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(raw), "\n")
	var out []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			out = append(out, line)
			continue
		}
		k, v, ok := splitEnvAssignment(trimmed)
		if !ok || strings.TrimSpace(v) == "" {
			out = append(out, line)
			continue
		}
		if strings.HasPrefix(v, obscuredPrefix) {
			out = append(out, line)
			continue
		}
		enc, err := Obscure(key, v)
		if err != nil {
			return fmt.Errorf("%s: %w", k, err)
		}
		// Preserve ?= vs = from the original line.
		sep := "="
		if strings.Contains(line, "?=") {
			sep = "?="
		}
		out = append(out, k+sep+enc)
	}
	text := strings.Join(out, "\n")
	if !strings.HasSuffix(text, "\n") && len(out) > 0 {
		text += "\n"
	}
	return os.WriteFile(path, []byte(text), 0o644)
}
