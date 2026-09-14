package apidb

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
)

const CreatedByPrepackaged = "prepackaged"

// CreatedByDefault marks seeded editable rulesets.
const CreatedByDefault = "default"

// DefaultAutoApplyRulesetID is retained for stored preference compatibility.
// New migrations no longer bind this ruleset automatically.
const DefaultAutoApplyRulesetID = "default-common-junk"

// retiredPrepackagedIDs are removed on seed so old installs drop obsolete examples.
var retiredPrepackagedIDs = []string{
	"example-empty-folders",
}

// DefaultRulesetDefinitions are seeded alongside the prepackaged library.
func DefaultRulesetDefinitions() []filter.Ruleset {
	return []filter.Ruleset{
		{
			RulesetID:     DefaultAutoApplyRulesetID,
			Name:          "Common junk",
			Description:   "Skips Thumbs.db, .DS_Store, desktop.ini, temporary Office files and *.tmp. This starter ruleset is no longer applied to new migrations automatically.",
			CreatedBy:     CreatedByDefault,
			SchemaVersion: filter.SchemaVersion,
			RootGroup:     commonJunkGroup(),
		},
	}
}

// commonJunkGroup backs both editable and library starter rulesets, which target
// the same throwaway files. Glob covers the plain names too, since a pattern
// without wildcards is an exact, case-insensitive name match.
func commonJunkGroup() filter.Group {
	return filter.Group{
		Op: filter.OpOR,
		Children: []filter.Child{
			{Condition: &filter.Condition{
				ID:        "junk-names",
				Field:     filter.FieldName,
				Operator:  filter.OpGlob,
				Value:     []any{"Thumbs.db", "desktop.ini", ".DS_Store", "*.tmp", "~$*"},
				AppliesTo: filter.AppliesFile,
			}},
		},
	}
}

func spreadsheetExtensions() []any {
	return []any{"xls", "xlsx", "xlsm", "xltx", "ods", "csv", "tsv", "numbers"}
}

func PrepackagedRulesetDefinitions() []filter.Ruleset {
	junk := commonJunkGroup()
	return []filter.Ruleset{
		{
			RulesetID:     "prepack-stale-large-files",
			Name:          "Stale Large Files",
			Description:   "Match files larger than 5 GB that have not been modified in 5 years or more.",
			CreatedBy:     CreatedByPrepackaged,
			SchemaVersion: filter.SchemaVersion,
			RootGroup: filter.Group{
				Op: filter.OpAND,
				Children: []filter.Child{
					{Condition: &filter.Condition{ID: "size", Field: filter.FieldSize, Operator: filter.OpGT, Value: float64(5368709120), AppliesTo: filter.AppliesFile}},
					{Condition: &filter.Condition{ID: "old5", Field: filter.FieldMTime, Operator: filter.OpOlderThan, Value: "5y", AppliesTo: filter.AppliesFile}},
				},
			},
		},
		{
			RulesetID:     "prepack-old-spreadsheets",
			Name:          "Old Spreadsheets",
			Description:   "Match common spreadsheet extensions not modified in 3 years or more.",
			CreatedBy:     CreatedByPrepackaged,
			SchemaVersion: filter.SchemaVersion,
			RootGroup: filter.Group{
				Op: filter.OpAND,
				Children: []filter.Child{
					{Condition: &filter.Condition{ID: "sheet", Field: filter.FieldExtension, Operator: filter.OpIn, Value: spreadsheetExtensions(), AppliesTo: filter.AppliesFile}},
					{Condition: &filter.Condition{ID: "old3", Field: filter.FieldMTime, Operator: filter.OpOlderThan, Value: "3y", AppliesTo: filter.AppliesFile}},
				},
			},
		},
		{
			RulesetID:     "prepack-common-junk",
			Name:          "Common Junk",
			Description:   "Match common junk names such as Thumbs.db, .DS_Store, desktop.ini, and temporary Office files.",
			CreatedBy:     CreatedByPrepackaged,
			SchemaVersion: filter.SchemaVersion,
			RootGroup:     commonJunkGroup(),
		},
		{
			RulesetID:     "example-pdf-reports",
			Name:          "PDFs under a report folder",
			Description:   "Match PDF files under a folder whose name includes \"report\". The path part is required in order; it is not an alternative to the file name.",
			CreatedBy:     CreatedByPrepackaged,
			SchemaVersion: filter.SchemaVersion,
			RootGroup: filter.Group{
				Op: filter.OpAND,
				Children: []filter.Child{
					{Condition: &filter.Condition{ID: "pdf", Field: filter.FieldExtension, Operator: filter.OpIn, Value: []any{"pdf"}, AppliesTo: filter.AppliesFile}},
					{Condition: &filter.Condition{ID: "report-folder", Field: filter.FieldPath, Operator: filter.OpContains, Value: []any{"report"}, AppliesTo: filter.AppliesFile}},
				},
			},
		},
		{
			RulesetID:     "example-untouched-5-years",
			Name:          "Skip anything untouched for 5 years",
			Description:   "Match files that have not been modified in the last 5 years, counted from whenever the migration runs.",
			CreatedBy:     CreatedByPrepackaged,
			SchemaVersion: filter.SchemaVersion,
			RootGroup: filter.Group{
				Op: filter.OpAND,
				Children: []filter.Child{
					{Condition: &filter.Condition{ID: "stale", Field: filter.FieldMTime, Operator: filter.OpOlderThan, Value: "5y", AppliesTo: filter.AppliesFile}},
				},
			},
		},
		{
			RulesetID:     "example-before-date",
			Name:          "Skip anything older than a fixed date",
			Description:   "Match files last modified before 1 January 2020. Pick your own cut-off date after loading this set.",
			CreatedBy:     CreatedByPrepackaged,
			SchemaVersion: filter.SchemaVersion,
			RootGroup: filter.Group{
				Op: filter.OpAND,
				Children: []filter.Child{
					{Condition: &filter.Condition{ID: "cutoff", Field: filter.FieldMTime, Operator: filter.OpBefore, Value: "2020-01-01", AppliesTo: filter.AppliesFile}},
				},
			},
		},
		{
			RulesetID:     "example-archive-cleanup",
			Name:          "Archive cleanup (combined rules)",
			Description:   "Any of these: common junk names, or files that are both larger than 1 GB and untouched for 5 years. Nested groups show how all-of / any-of combine.",
			CreatedBy:     CreatedByPrepackaged,
			SchemaVersion: filter.SchemaVersion,
			RootGroup: filter.Group{
				Op: filter.OpOR,
				Children: []filter.Child{
					{Group: &junk},
					{Group: &filter.Group{
						Op: filter.OpAND,
						Children: []filter.Child{
							{Condition: &filter.Condition{ID: "big", Field: filter.FieldSize, Operator: filter.OpGT, Value: float64(1073741824), AppliesTo: filter.AppliesFile}},
							{Condition: &filter.Condition{ID: "stale", Field: filter.FieldMTime, Operator: filter.OpOlderThan, Value: "5y", AppliesTo: filter.AppliesFile}},
						},
					}},
				},
			},
		},
	}
}
