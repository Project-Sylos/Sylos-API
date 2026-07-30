package manager

import (
	"context"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

func TestGetMigrationUsesRuntimeMemoryWithoutAPIDB(t *testing.T) {
	mig := &migration.Migration{}
	m := &Manager{
		runtimeByID: map[string]*runtimeMigration{
			"live": {Migration: mig},
		},
	}

	got, err := m.GetMigration(context.Background(), "live")
	if err != nil {
		t.Fatal(err)
	}
	if got != mig {
		t.Fatal("did not return in-memory migration")
	}
}
