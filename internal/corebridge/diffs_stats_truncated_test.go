package corebridge_test

import (
	"encoding/json"
	"testing"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

func TestDiffsStatsResponseMarshalsTruncated(t *testing.T) {
	raw, err := json.Marshal(corebridge.DiffsStatsResponse{
		Total:        100000,
		TotalFolders: 10,
		TotalFiles:   99990,
		Truncated:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["truncated"] != true {
		t.Fatalf("truncated=%v want true in %s", m["truncated"], raw)
	}
	if m["total"] != float64(100000) {
		t.Fatalf("total=%v", m["total"])
	}
}

func TestDiffsStatsResponseOmitsTruncatedWhenFalse(t *testing.T) {
	raw, err := json.Marshal(corebridge.DiffsStatsResponse{Total: 3, TotalFiles: 3})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["truncated"]; ok {
		t.Fatalf("truncated should be omitted when false: %s", raw)
	}
}
