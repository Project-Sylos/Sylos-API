package manager

import (
	"context"
	"strings"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

func TestRetryFinalizeRejectsWrongPhase(t *testing.T) {
	mig := &migration.Migration{}
	m := &Manager{
		runtimeByID: map[string]*runtimeMigration{
			"m1": {Migration: mig},
		},
	}
	resp, err := m.RetryFinalize(context.Background(), "m1", corebridge.SweepConfigRequest{
		MemoryLimitGB: 4,
		Threads:       2,
	})
	if err == nil {
		t.Fatal("expected error for wrong phase")
	}
	if resp.Success {
		t.Fatal("expected unsuccessful response")
	}
	if !strings.Contains(resp.Error, "finalize-failed") {
		t.Fatalf("error=%q", resp.Error)
	}
}

func TestApplyRunResultToRuntimeGenericFailure(t *testing.T) {
	rec := &runtimeMigration{Status: "copy-in-progress"}
	applyRunResultToRuntime(rec, &migration.Migration{}, context.Canceled)
	if rec.Status != corebridge.MigrationStatusFailed {
		t.Fatalf("status=%s", rec.Status)
	}
	if rec.Error == "" {
		t.Fatal("expected error text")
	}
}
