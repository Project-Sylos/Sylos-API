package migrationfiles

import (
	"archive/zip"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"github.com/rs/zerolog"
)

func TestNormalizeMigrationID(t *testing.T) {
	cases := map[string]string{
		"abc.ops":         "abc",
		"abc.ops.zip":     "abc",
		"abc":             "abc",
		"path/x.ops/":     "x",
		"":                "",
	}
	for in, want := range cases {
		if got := NormalizeMigrationID(in); got != want {
			t.Fatalf("%q: got %q want %q", in, got, want)
		}
	}
}

func TestUploadMigrationOpsCopiesContents(t *testing.T) {
	src := t.TempDir()
	store, err := opsdb.Open(opsdb.Options{Dir: src})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	err = filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		w, err := zw.Create(filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, err = w.Write(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	dataDir := t.TempDir()
	resp, err := UploadMigrationOps(context.Background(), zerolog.Nop(), dataDir, "mig1.ops", buf.Bytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatalf("upload failed: %s", resp.Error)
	}
	opsDir := filepath.Join(dataDir, "mig1", "mig1.ops")
	if !looksLikeBadgerDir(opsDir) {
		t.Fatalf("missing ops at %s", opsDir)
	}
	dbPath := filepath.Join(dataDir, "mig1", "mig1.db")
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("db marker: %v", err)
	}

	resp2, err := UploadMigrationOps(context.Background(), zerolog.Nop(), dataDir, "mig1", buf.Bytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Success || resp2.Error != "file already present on API" {
		t.Fatalf("expected conflict, got %#v", resp2)
	}
}

func TestExportThenImportRoundTrip(t *testing.T) {
	dataDir := t.TempDir()
	srcOps := filepath.Join(dataDir, "src", "src.ops")
	if err := os.MkdirAll(srcOps, 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := opsdb.Open(opsdb.Options{Dir: srcOps})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "src", "src.db"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	name, err := ExportMigrationOpsZip(dataDir, "src", &buf)
	if err != nil {
		t.Fatal(err)
	}
	if name != "src.ops.zip" {
		t.Fatalf("filename %q", name)
	}

	resp, err := UploadMigrationOps(context.Background(), zerolog.Nop(), dataDir, "dst.ops.zip", buf.Bytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatalf("import: %s", resp.Error)
	}
	if !looksLikeBadgerDir(filepath.Join(dataDir, "dst", "dst.ops")) {
		t.Fatal("dst ops missing")
	}
}
