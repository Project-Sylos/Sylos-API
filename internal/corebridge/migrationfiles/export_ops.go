package migrationfiles

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// ExportMigrationOpsZip writes a zip of dataDir/{id}/{id}.ops/ contents (files at zip root)
// plus a small sylos-export.json marker. OAuth credential rows are stripped from a temp
// copy so exports never ship usable tokens. The live ops directory is left unchanged.
func ExportMigrationOpsZip(dataDir, migrationID string, w io.Writer) (filename string, err error) {
	migrationID = NormalizeMigrationID(migrationID)
	if migrationID == "" {
		return "", fmt.Errorf("migration ID is required")
	}
	opsDir := filepath.Join(MigrationDir(dataDir, migrationID), migrationID+".ops")
	info, err := os.Stat(opsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("ops directory not found for migration %q", migrationID)
		}
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("ops path is not a directory")
	}

	tmpRoot, err := os.MkdirTemp("", "sylos-ops-export-*")
	if err != nil {
		return "", fmt.Errorf("create export temp dir: %w", err)
	}
	defer os.RemoveAll(tmpRoot)

	tmpOps := filepath.Join(tmpRoot, migrationID+".ops")
	if err := filepath.Walk(opsDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(opsDir, path)
		if err != nil {
			return err
		}
		target := filepath.Join(tmpOps, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode().Perm())
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, in)
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	}); err != nil {
		return "", fmt.Errorf("copy ops for export: %w", err)
	}
	store, err := opsdb.Open(opsdb.Options{Dir: tmpOps})
	if err != nil {
		return "", fmt.Errorf("open export ops copy: %w", err)
	}
	if _, err := store.DeleteAllOAuthCreds(); err != nil {
		_ = store.Close()
		return "", fmt.Errorf("strip oauth from export: %w", err)
	}
	if err := store.Close(); err != nil {
		return "", fmt.Errorf("close export ops copy: %w", err)
	}

	zw := zip.NewWriter(w)
	defer func() {
		if cerr := zw.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	meta := fmt.Sprintf("{\"format\":\"sylos-ops\",\"schema_version\":\"0.1\",\"migration_id\":%q}\n", migrationID)
	mw, err := zw.Create("sylos-export.json")
	if err != nil {
		return "", err
	}
	if _, err := io.WriteString(mw, meta); err != nil {
		return "", err
	}

	err = filepath.Walk(tmpOps, func(path string, fi os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(tmpOps, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		name := filepath.ToSlash(rel)
		if fi.IsDir() {
			_, err := zw.Create(name + "/")
			return err
		}
		hdr, err := zip.FileInfoHeader(fi)
		if err != nil {
			return err
		}
		hdr.Name = name
		hdr.Method = zip.Deflate
		out, err := zw.CreateHeader(hdr)
		if err != nil {
			return err
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, in)
		closeErr := in.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
	if err != nil {
		return "", err
	}
	return migrationID + ".ops.zip", nil
}

// OpsDirExists reports whether dataDir/{id}/{id}.ops exists.
func OpsDirExists(dataDir, migrationID string) bool {
	id := NormalizeMigrationID(migrationID)
	if id == "" {
		return false
	}
	info, err := os.Stat(filepath.Join(dataDir, id, id+".ops"))
	return err == nil && info.IsDir()
}

// StripZipNameToMigrationID derives an id from an uploaded zip filename.
func StripZipNameToMigrationID(name string) string {
	base := filepath.Base(strings.TrimSpace(name))
	base = strings.TrimSuffix(base, filepath.Ext(base)) // .zip
	return NormalizeMigrationID(base)
}
