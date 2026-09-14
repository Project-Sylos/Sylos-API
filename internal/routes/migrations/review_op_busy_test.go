package migrations

import (
	"errors"
	"fmt"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

func TestReviewOpBusySentinel(t *testing.T) {
	err := fmt.Errorf("%w: an operation is already running on this path (or a parent/child); try again once it finishes", migration.ErrReviewOpBusy)
	if !errors.Is(err, migration.ErrReviewOpBusy) {
		t.Fatalf("lost sentinel: %v", err)
	}
	msg := err.Error()
	if i := len("review operation busy: "); len(msg) > i {
		msg = msg[i:]
	}
	if msg != "an operation is already running on this path (or a parent/child); try again once it finishes" {
		// writeReviewOpBusy strips at first ": "
		stripped := err.Error()
		if j := indexColonSpace(stripped); j >= 0 {
			stripped = stripped[j+2:]
		}
		if stripped != "an operation is already running on this path (or a parent/child); try again once it finishes" {
			t.Fatalf("strip got %q", stripped)
		}
	}
}

func indexColonSpace(s string) int {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == ':' && s[i+1] == ' ' {
			return i
		}
	}
	return -1
}
