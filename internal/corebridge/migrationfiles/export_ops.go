package migrationfiles

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ExportMigrationOpsZip writes a zip of dataDir/{id}/{id}.ops/ contents (files at zip root)
// plus a small sylos-export.json marker. Caller owns closing w after return... actually we write fully.
func ExportMigrationOpsZip(dataDir, migrationID string, w io.Writer) (filename string, err error) {
	migrationID = NormalizeMigrationID(migrationID)
	if migrationID == "" {
		return "", fmt.Errorf("migration ID is required")
	}
	opsDir := filepath.Join(dataDir, migrationID, migrationID+".ops")
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

	err = filepath.Walk(opsDir, func(path string, fi os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(opsDir, path)
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
