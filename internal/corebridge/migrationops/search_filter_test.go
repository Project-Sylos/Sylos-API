package migrationops

import (
	"encoding/json"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

func TestSearchRequestHasFilter(t *testing.T) {
	t.Parallel()

	falseVal := false
	trueVal := true

	cases := []struct {
		name string
		req  corebridge.SearchRequest
		want bool
	}{
		{name: "empty", req: corebridge.SearchRequest{}, want: false},
		{name: "empty_conditions", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{}}, want: false},
		{name: "blank_path", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "path", Value: "  "}}}, want: false},
		{name: "blank_name", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "name", Value: ""}}}, want: false},
		{name: "statusSearchType_alone", req: corebridge.SearchRequest{StatusSearchType: "traversal"}, want: false},
		{name: "sort_alone", req: corebridge.SearchRequest{Sort: &corebridge.SortOption{Field: "name", Direction: "asc"}}, want: false},
		{name: "include_dst_only_true", req: corebridge.SearchRequest{IncludeDestinationOnly: &trueVal}, want: false},
		{name: "include_dst_only_nil", req: corebridge.SearchRequest{IncludeDestinationOnly: nil}, want: false},
		{name: "exclude_dst_only", req: corebridge.SearchRequest{IncludeDestinationOnly: &falseVal}, want: true},
		{name: "path_query", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "path", Value: "docs"}}}, want: true},
		{name: "name_query", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "name", Value: "readme"}}}, want: true},
		{name: "type_folder", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "type", Value: "folder"}}}, want: true},
		{name: "traversal_status", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "traversalStatus", Value: "failed"}}}, want: true},
		{name: "copy_status", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "copyStatus", Value: "pending"}}}, want: true},
		{name: "delete_status", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "deleteStatus", Value: "pending"}}}, want: true},
		{name: "path_issue_status", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "pathIssueStatus", Value: "issues"}}}, want: true},
		{name: "path_issue_category", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "pathIssueCategory", Value: "InvalidChar"}}}, want: true},
		{name: "depth", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "depth", Operator: "gte", Value: float64(2)}}}, want: true},
		{name: "size", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "size", Operator: "gt", Value: 100}}}, want: true},
		{name: "blank_status", req: corebridge.SearchRequest{Conditions: []corebridge.SearchCondition{{Field: "traversalStatus", Value: "  "}}}, want: false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := SearchRequestHasFilter(tc.req); got != tc.want {
				t.Fatalf("SearchRequestHasFilter() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSearchListResponseOmitsUnknownTotal(t *testing.T) {
	t.Parallel()

	resp := searchListResponse(migration.SearchResult{
		Items: []migration.DiffItem{
			{Path: "/a", Name: "a", Type: "file"},
		},
		Total:   nil,
		HasMore: true,
		Limit:   1,
		Offset:  0,
	})
	if resp.Pagination.Total != nil {
		t.Fatalf("Total = %v, want nil", *resp.Pagination.Total)
	}
	if !resp.Pagination.HasMore {
		t.Fatal("HasMore = false, want true")
	}

	raw, err := json.Marshal(resp.Pagination)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["total"]; ok {
		t.Fatalf("JSON should omit total when unknown, got %s", raw)
	}
	if decoded["hasMore"] != true {
		t.Fatalf("hasMore = %v, want true", decoded["hasMore"])
	}
}

func TestSearchListResponseKeepsKnownTotalZero(t *testing.T) {
	t.Parallel()

	zero := 0
	resp := searchListResponse(migration.SearchResult{
		Items:   nil,
		Total:   &zero,
		HasMore: false,
		Limit:   10,
		Offset:  0,
	})
	if resp.Pagination.Total == nil || *resp.Pagination.Total != 0 {
		t.Fatalf("Total = %v, want 0", resp.Pagination.Total)
	}

	raw, err := json.Marshal(resp.Pagination)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["total"] != float64(0) {
		t.Fatalf("known total=0 must be present in JSON, got %s", raw)
	}
}

func TestDiffListResponseSetsTotalAndHasMore(t *testing.T) {
	t.Parallel()

	resp := diffListResponse(nil, 0, 10, 25)
	if resp.Pagination.Total == nil || *resp.Pagination.Total != 25 {
		t.Fatalf("Total = %v, want 25", resp.Pagination.Total)
	}
	if !resp.Pagination.HasMore {
		t.Fatal("HasMore = false, want true")
	}

	resp = diffListResponse(nil, 20, 10, 25)
	if resp.Pagination.HasMore {
		t.Fatal("HasMore = true, want false at last page")
	}
}
