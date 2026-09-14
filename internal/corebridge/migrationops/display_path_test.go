// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migrationops

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

func TestDiffItemToPathNodesDisplayPath(t *testing.T) {
	item := migration.DiffItem{
		Path:            "/id-a/id-b",
		Name:            "a.txt",
		DisplayPath:     "/Reports/a.txt",
		DstDisplayPath:  "/Reports/a-renamed.txt",
		ResolvedDstName: "a-renamed.txt",
		SrcNodeID:       "src1",
		DstNodeID:       "dst1",
		Type:            "file",
		Depth:           2,
	}
	nodes := diffItemToPathNodes(item)
	if nodes.Src == nil || nodes.Dst == nil {
		t.Fatal("want src and dst")
	}
	if nodes.Src.LocationPath != "/id-a/id-b" {
		t.Fatalf("src locationPath want id_path, got %q", nodes.Src.LocationPath)
	}
	if nodes.Dst.LocationPath != "/id-a/id-b" {
		t.Fatalf("dst locationPath want id_path, got %q", nodes.Dst.LocationPath)
	}
	if nodes.Src.DisplayPath != "/Reports/a.txt" {
		t.Fatalf("src displayPath: %q", nodes.Src.DisplayPath)
	}
	if nodes.Dst.DisplayPath != "/Reports/a-renamed.txt" {
		t.Fatalf("dst displayPath: %q", nodes.Dst.DisplayPath)
	}
	if nodes.Dst.Name != "a-renamed.txt" {
		t.Fatalf("dst name: %q", nodes.Dst.Name)
	}
}
