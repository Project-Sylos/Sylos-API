package manager

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

func (m *Manager) GetQueueMetrics(_ context.Context, migrationID string) (*corebridge.QueueMetricsResponse, error) {
	mig, err := m.engineMgr.GetMigration(migrationID)
	if err != nil {
		return nil, err
	}
	if mig == nil {
		return nil, corebridge.ErrMigrationNotFound
	}
	metrics, err := mig.GetQueueMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to query queue metrics: %w", err)
	}

	resp := &corebridge.QueueMetricsResponse{Success: true}
	if q, ok := metrics.Queues["src-traversal"]; ok {
		resp.SrcTraversal = &corebridge.ExternalQueueMetrics{
			Name:                     "src-traversal",
			FilesDiscoveredTotal:     asInt64(q["files_discovered_total"]),
			FoldersDiscoveredTotal:   asInt64(q["folders_discovered_total"]),
			DiscoveryRateItemsPerSec: asFloat64(q["discovery_rate_items_per_sec"]),
			TotalDiscovered:          asInt64(q["total_discovered"]),
			Round:                    asInt(q["round"]),
			Pending:                  asInt(q["pending"]),
			InProgress:               asInt(q["in_progress"]),
			Workers:                  asInt(q["workers"]),
			TotalPending:             asInt(q["total_pending"]),
			TotalFailed:              asInt(q["total_failed"]),
		}
	}
	if q, ok := metrics.Queues["dst-traversal"]; ok {
		resp.DstTraversal = &corebridge.ExternalQueueMetrics{
			Name:                     "dst-traversal",
			FilesDiscoveredTotal:     asInt64(q["files_discovered_total"]),
			FoldersDiscoveredTotal:   asInt64(q["folders_discovered_total"]),
			DiscoveryRateItemsPerSec: asFloat64(q["discovery_rate_items_per_sec"]),
			TotalDiscovered:          asInt64(q["total_discovered"]),
			Round:                    asInt(q["round"]),
			Pending:                  asInt(q["pending"]),
			InProgress:               asInt(q["in_progress"]),
			Workers:                  asInt(q["workers"]),
			TotalPending:             asInt(q["total_pending"]),
			TotalFailed:              asInt(q["total_failed"]),
		}
	}
	if q, ok := metrics.Queues["copy"]; ok {
		resp.Copy = &corebridge.ExternalQueueMetrics{
			Name:           "copy",
			Folders:        asInt64(q["folders"]),
			Files:          asInt64(q["files"]),
			Total:          asInt64(q["total"]),
			Bytes:          asInt64(q["bytes"]),
			ItemsPerSecond: asFloat64(q["items_per_second"]),
			BytesPerSecond: asFloat64(q["bytes_per_second"]),
			Round:          asInt(q["round"]),
			Pending:        asInt(q["pending"]),
			InProgress:     asInt(q["in_progress"]),
			Workers:        asInt(q["workers"]),
			TotalPending:   asInt(q["total_pending"]),
			TotalFailed:    asInt(q["total_failed"]),
		}
	}

	return &corebridge.QueueMetricsResponse{
		Success:      resp.Success,
		SrcTraversal: resp.SrcTraversal,
		DstTraversal: resp.DstTraversal,
		Copy:         resp.Copy,
	}, nil
}

func (m *Manager) GetLogs(_ context.Context, migrationID string, _ corebridge.GetLogsRequest) (*corebridge.GetLogsResponse, error) {
	mig, err := m.engineMgr.GetMigration(migrationID)
	if err != nil {
		return nil, err
	}
	if mig == nil {
		return nil, corebridge.ErrMigrationNotFound
	}
	logs, err := mig.GetLogs(1000, true)
	if err != nil {
		return nil, fmt.Errorf("failed to get logs: %w", err)
	}
	out := make(map[string][]corebridge.LogEntry)
	for level, entries := range logs.ByLevel {
		for i, entry := range entries {
			out[level] = append(out[level], corebridge.LogEntry{
				ID:    fmt.Sprintf("%d", i+1),
				Level: level,
				Data: map[string]any{
					"message":   entry.Message,
					"timestamp": entry.Timestamp.Format(time.RFC3339),
				},
			})
		}
	}
	return &corebridge.GetLogsResponse{Success: true, Logs: out}, nil
}

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	case float64:
		return int(t)
	default:
		return 0
	}
}

func asInt64(v any) int64 {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case float64:
		return int64(t)
	default:
		return 0
	}
}

func asFloat64(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	default:
		return 0
	}
}
