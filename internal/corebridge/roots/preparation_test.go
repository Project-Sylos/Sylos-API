package roots

import "testing"

func TestNormalizeRootChildren_sourceRequiresIncluded(t *testing.T) {
	_, err := NormalizeRootChildren([]RootChildPlan{
		{ID: "a", Name: "one", Type: "folder", Excluded: true},
		{ID: "b", Name: "two", Type: "file", Excluded: true},
	}, nil, true)
	if err == nil {
		t.Fatal("expected error when all source children excluded")
	}
}

func TestNormalizeRootChildren_ignoresOffPathExcludedIDs(t *testing.T) {
	out, err := NormalizeRootChildren([]RootChildPlan{
		{ID: "a", Name: "keep", Type: "folder"},
		{ID: "b", Name: "drop", Type: "folder"},
	}, []string{"b", "uncle-not-in-list"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("len=%d", len(out))
	}
	var dropExcluded, keepExcluded bool
	for _, c := range out {
		switch c.ID {
		case "a":
			keepExcluded = c.Excluded
		case "b":
			dropExcluded = c.Excluded
		}
	}
	if keepExcluded {
		t.Fatal("keep should not be excluded")
	}
	if !dropExcluded {
		t.Fatal("drop should be excluded")
	}
}

func TestNormalizeRootChildren_destNeverExclude(t *testing.T) {
	out, err := NormalizeRootChildren([]RootChildPlan{
		{ID: "a", Name: "x", Type: "folder", Excluded: true, DstOnly: true},
	}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Excluded {
		t.Fatal("dest child must not carry excluded")
	}
	if !out[0].DstOnly {
		t.Fatal("dstOnly should remain")
	}
}
