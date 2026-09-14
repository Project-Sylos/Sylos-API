package apidb

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
)

func builtinDefinitions() []filter.Ruleset {
	return append(PrepackagedRulesetDefinitions(), DefaultRulesetDefinitions()...)
}

func definitionByID(t *testing.T, id string) filter.Ruleset {
	t.Helper()
	for _, def := range builtinDefinitions() {
		if def.RulesetID == id {
			return def
		}
	}
	t.Fatalf("ruleset %q not found", id)
	return filter.Ruleset{}
}

func assertOutcome(t *testing.T, id string, node filter.NodeInput, want string) {
	t.Helper()
	c, err := filter.Compile(definitionByID(t, id))
	if err != nil {
		t.Fatalf("compile %s: %v", id, err)
	}
	got := c.EvaluateDetermining(node, time.Now())
	if got.Outcome != want {
		t.Fatalf("%s / %s: want %s, got %s (%s)", id, node.DisplayPath, want, got.Outcome, got.Label)
	}
}

func TestBuiltinRulesetsCompile(t *testing.T) {
	defs := builtinDefinitions()
	if len(defs) == 0 {
		t.Fatal("no builtin rulesets defined")
	}
	seen := make(map[string]bool, len(defs))
	for _, def := range defs {
		if seen[def.RulesetID] {
			t.Fatalf("duplicate ruleset id %q", def.RulesetID)
		}
		seen[def.RulesetID] = true
		if def.Name == "" || def.Description == "" {
			t.Fatalf("ruleset %s: name and description are required", def.RulesetID)
		}
		if _, err := filter.Compile(def); err != nil {
			t.Fatalf("ruleset %s: %v", def.RulesetID, err)
		}
	}
	if seen["example-empty-folders"] {
		t.Fatal("example-empty-folders should be retired")
	}
}

func TestExamplePDFReportsMatchesReportPDFs(t *testing.T) {
	matched := []filter.NodeInput{
		{Name: "Q3 report.pdf", DisplayPath: "/Reports/Q3 report.pdf", Depth: 2, NodeType: filter.NodeFile, Size: 1},
		{Name: "summary.pdf", DisplayPath: "/Reports/summary.pdf", Depth: 2, NodeType: filter.NodeFile, Size: 1},
	}
	for _, node := range matched {
		assertOutcome(t, "example-pdf-reports", node, filter.OutcomeExcluded)
	}
	kept := []filter.NodeInput{
		{Name: "brochure.pdf", DisplayPath: "/Marketing/brochure.pdf", Depth: 2, NodeType: filter.NodeFile, Size: 1},
		{Name: "notes.docx", DisplayPath: "/Reports/notes.docx", Depth: 2, NodeType: filter.NodeFile, Size: 1},
	}
	for _, node := range kept {
		assertOutcome(t, "example-pdf-reports", node, filter.OutcomePassed)
	}
	assertOutcome(t, "example-pdf-reports", filter.NodeInput{
		Name: "Reports", DisplayPath: "/Reports", Depth: 1, NodeType: filter.NodeFolder,
	}, filter.OutcomePassed)
}

func TestExampleBeforeDate(t *testing.T) {
	assertOutcome(t, "example-before-date", filter.NodeInput{
		Name: "budget 2018.xlsx", DisplayPath: "/Finance/budget 2018.xlsx", Depth: 2,
		NodeType: filter.NodeFile, Size: 1, MTime: "2018-04-02T00:00:00Z",
	}, filter.OutcomeExcluded)
	assertOutcome(t, "example-before-date", filter.NodeInput{
		Name: "forecast.xlsx", DisplayPath: "/Finance/forecast.xlsx", Depth: 2,
		NodeType: filter.NodeFile, Size: 1, MTime: "2025-11-01T00:00:00Z",
	}, filter.OutcomePassed)
}

func TestExampleArchiveCleanupChainsRules(t *testing.T) {
	old := time.Now().AddDate(-8, 0, 0).Format(time.RFC3339)
	recent := time.Now().AddDate(0, 0, -20).Format(time.RFC3339)

	assertOutcome(t, "example-archive-cleanup", filter.NodeInput{
		Name: "Thumbs.db", DisplayPath: "/Media/Thumbs.db", Depth: 2, NodeType: filter.NodeFile,
		Size: 1024, MTime: recent,
	}, filter.OutcomeExcluded)
	assertOutcome(t, "example-archive-cleanup", filter.NodeInput{
		Name: "conference 2017.mp4", DisplayPath: "/Media/conference 2017.mp4", Depth: 2,
		NodeType: filter.NodeFile, Size: 8 * 1024 * 1024 * 1024, MTime: old,
	}, filter.OutcomeExcluded)
	// Large but recent, so the size+age branch must not fire on its own.
	assertOutcome(t, "example-archive-cleanup", filter.NodeInput{
		Name: "promo.mp4", DisplayPath: "/Media/promo.mp4", Depth: 2,
		NodeType: filter.NodeFile, Size: 2 * 1024 * 1024 * 1024, MTime: recent,
	}, filter.OutcomePassed)
	// Empty folders are no longer part of this starter.
	assertOutcome(t, "example-archive-cleanup", filter.NodeInput{
		Name: "Old Archive", DisplayPath: "/Old Archive", Depth: 1, NodeType: filter.NodeFolder,
	}, filter.OutcomePassed)
}

func TestOldSpreadsheetsUsesExtensions(t *testing.T) {
	old := time.Now().AddDate(-4, 0, 0).Format(time.RFC3339)
	recent := time.Now().AddDate(0, 0, -10).Format(time.RFC3339)
	assertOutcome(t, "prepack-old-spreadsheets", filter.NodeInput{
		Name: "budget 2018.xlsx", DisplayPath: "/Finance/budget 2018.xlsx", Depth: 2,
		NodeType: filter.NodeFile, Size: 1, MTime: old,
	}, filter.OutcomeExcluded)
	assertOutcome(t, "prepack-old-spreadsheets", filter.NodeInput{
		Name: "forecast.xlsx", DisplayPath: "/Finance/forecast.xlsx", Depth: 2,
		NodeType: filter.NodeFile, Size: 1, MTime: recent,
	}, filter.OutcomePassed)
	assertOutcome(t, "prepack-old-spreadsheets", filter.NodeInput{
		Name: "notes.docx", DisplayPath: "/Reports/notes.docx", Depth: 2,
		NodeType: filter.NodeFile, Size: 1, MTime: old,
	}, filter.OutcomePassed)
}
