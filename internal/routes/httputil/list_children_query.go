package httputil

import (
	"net/url"
	"strconv"
)

type ListChildrenQuery struct {
	Identifier   string
	Role         string
	RootType     string
	DriveID      string
	ConnectionID string
	Offset       int
	Limit        int
	FoldersOnly  bool
}

func ParseListChildrenQuery(q url.Values) ListChildrenQuery {
	driveID := q.Get("driveId")
	if driveID == "" {
		driveID = q.Get("drive_id")
	}

	connectionID := q.Get("connectionId")
	if connectionID == "" {
		connectionID = q.Get("connection_id")
	}

	offset := 0
	if offsetStr := q.Get("offset"); offsetStr != "" {
		if parsed, err := strconv.Atoi(offsetStr); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	limit := 100
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	foldersOnly := false
	if foldersOnlyStr := q.Get("foldersOnly"); foldersOnlyStr != "" {
		if parsed, err := strconv.ParseBool(foldersOnlyStr); err == nil {
			foldersOnly = parsed
		}
	} else if foldersOnlyStr := q.Get("folders_only"); foldersOnlyStr != "" {
		if parsed, err := strconv.ParseBool(foldersOnlyStr); err == nil {
			foldersOnly = parsed
		}
	}

	return ListChildrenQuery{
		Identifier:   q.Get("identifier"),
		Role:         q.Get("role"),
		RootType:     q.Get("rootType"),
		DriveID:      driveID,
		ConnectionID: connectionID,
		Offset:       offset,
		Limit:        limit,
		FoldersOnly:  foldersOnly,
	}
}
