package migrationops

import (
	"errors"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

func TestRunPathReviewBatchReturnsBusy(t *testing.T) {
	_, _, err := runPathReviewBatch([]string{"a", "b"}, func(nodeID string) (migration.PathReviewActionResult, error) {
		if nodeID == "b" {
			return migration.PathReviewActionResult{}, errReviewBusyForTest()
		}
		return migration.PathReviewActionResult{AffectedCount: 1}, nil
	}, "noop")
	if !errors.Is(err, migration.ErrReviewOpBusy) {
		t.Fatalf("err=%v want ErrReviewOpBusy", err)
	}
}

func errReviewBusyForTest() error {
	return migration.ErrReviewOpBusy
}
