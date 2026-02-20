package manager

import (
	"context"
	"fmt"
	"os"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
)

func (m *Manager) GetQueueMetrics(ctx context.Context, migrationID string) (*corebridge.QueueMetricsResponse, error) {
	if controller := m.migrationsMgr.GetController(migrationID); controller != nil {
		liveStats, err := controller.GetLiveStats()
		if err == nil && len(liveStats) > 0 {
			dbMetrics := liveStatsToQueueMetricsResponse(liveStats)
			return metricsToResponse(dbMetrics), nil
		}
	}

	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return nil, corebridge.ErrMigrationNotFound
	}

	dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, corebridge.ErrMigrationNotFound
	}

	engineMetrics, err := migration.GetQueueMetricsFromDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue metrics: %w", err)
	}
	dbMetrics := liveStatsToQueueMetricsResponse(engineMetrics)
	return metricsToResponse(dbMetrics), nil
}

func liveStatsToQueueMetricsResponse(liveStats map[string]queue.ExternalQueueMetrics) *database.QueueMetricsResponse {
	resp := &database.QueueMetricsResponse{}
	if met, ok := liveStats["src-traversal"]; ok {
		resp.SrcTraversal = queueMetricsToDB(&met)
	}
	if met, ok := liveStats["dst-traversal"]; ok {
		resp.DstTraversal = queueMetricsToDB(&met)
	}
	if met, ok := liveStats["copy"]; ok {
		resp.Copy = queueMetricsToDB(&met)
	}
	return resp
}

func queueMetricsToDB(met *queue.ExternalQueueMetrics) *database.ExternalQueueMetrics {
	if met == nil {
		return nil
	}
	return &database.ExternalQueueMetrics{
		FilesDiscoveredTotal:     met.FilesDiscoveredTotal,
		FoldersDiscoveredTotal:   met.FoldersDiscoveredTotal,
		DiscoveryRateItemsPerSec: met.DiscoveryRateItemsPerSec,
		TotalDiscovered:          met.TotalDiscovered,
		Folders:                  met.Folders,
		Files:                    met.Files,
		Total:                    met.Total,
		Bytes:                    met.Bytes,
		ItemsPerSecond:           met.ItemsPerSecond,
		BytesPerSecond:           met.BytesPerSecond,
		Round:                    met.Round,
		Pending:                  met.Pending,
		InProgress:               met.InProgress,
		Workers:                  met.Workers,
		TotalPending:             met.TotalPending,
		TotalFailed:              met.TotalFailed,
		Name:                     met.Name,
	}
}

func metricsToResponse(dbMetrics *database.QueueMetricsResponse) *corebridge.QueueMetricsResponse {
	if dbMetrics == nil {
		return &corebridge.QueueMetricsResponse{Success: true}
	}
	metrics := &corebridge.QueueMetricsResponse{Success: true}
	if dbMetrics.SrcTraversal != nil {
		metrics.SrcTraversal = &corebridge.ExternalQueueMetrics{
			FilesDiscoveredTotal:     dbMetrics.SrcTraversal.FilesDiscoveredTotal,
			FoldersDiscoveredTotal:   dbMetrics.SrcTraversal.FoldersDiscoveredTotal,
			DiscoveryRateItemsPerSec: dbMetrics.SrcTraversal.DiscoveryRateItemsPerSec,
			TotalDiscovered:          dbMetrics.SrcTraversal.TotalDiscovered,
			Round:                    dbMetrics.SrcTraversal.Round,
			Pending:                  dbMetrics.SrcTraversal.Pending,
			InProgress:               dbMetrics.SrcTraversal.InProgress,
			Workers:                  dbMetrics.SrcTraversal.Workers,
			TotalPending:             dbMetrics.SrcTraversal.TotalPending,
			TotalFailed:              dbMetrics.SrcTraversal.TotalFailed,
			Name:                     dbMetrics.SrcTraversal.Name,
		}
	}
	if dbMetrics.DstTraversal != nil {
		metrics.DstTraversal = &corebridge.ExternalQueueMetrics{
			FilesDiscoveredTotal:     dbMetrics.DstTraversal.FilesDiscoveredTotal,
			FoldersDiscoveredTotal:   dbMetrics.DstTraversal.FoldersDiscoveredTotal,
			DiscoveryRateItemsPerSec: dbMetrics.DstTraversal.DiscoveryRateItemsPerSec,
			TotalDiscovered:          dbMetrics.DstTraversal.TotalDiscovered,
			Round:                    dbMetrics.DstTraversal.Round,
			Pending:                  dbMetrics.DstTraversal.Pending,
			InProgress:               dbMetrics.DstTraversal.InProgress,
			Workers:                  dbMetrics.DstTraversal.Workers,
			TotalPending:             dbMetrics.DstTraversal.TotalPending,
			TotalFailed:              dbMetrics.DstTraversal.TotalFailed,
			Name:                     dbMetrics.DstTraversal.Name,
		}
	}
	if dbMetrics.Copy != nil {
		metrics.Copy = &corebridge.ExternalQueueMetrics{
			Folders:                  dbMetrics.Copy.Folders,
			Files:                    dbMetrics.Copy.Files,
			Total:                    dbMetrics.Copy.Total,
			Bytes:                    dbMetrics.Copy.Bytes,
			ItemsPerSecond:           dbMetrics.Copy.ItemsPerSecond,
			BytesPerSecond:           dbMetrics.Copy.BytesPerSecond,
			Round:                    dbMetrics.Copy.Round,
			Pending:                  dbMetrics.Copy.Pending,
			InProgress:               dbMetrics.Copy.InProgress,
			Workers:                  dbMetrics.Copy.Workers,
			TotalPending:             dbMetrics.Copy.TotalPending,
			TotalFailed:              dbMetrics.Copy.TotalFailed,
			Name:                     dbMetrics.Copy.Name,
			FilesDiscoveredTotal:     dbMetrics.Copy.FilesDiscoveredTotal,
			FoldersDiscoveredTotal:   dbMetrics.Copy.FoldersDiscoveredTotal,
			DiscoveryRateItemsPerSec: dbMetrics.Copy.DiscoveryRateItemsPerSec,
			TotalDiscovered:          dbMetrics.Copy.TotalDiscovered,
		}
	}
	return metrics
}

func (m *Manager) GetLogs(ctx context.Context, migrationID string, req corebridge.GetLogsRequest) (*corebridge.GetLogsResponse, error) {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return nil, corebridge.ErrMigrationNotFound
	}

	dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, corebridge.ErrMigrationNotFound
	}

	dbLogs, err := migration.GetLogsFromDB(ctx, dbPath, migration.GetLogsOptions{MaxPerLevel: 1000})
	if err != nil {
		return nil, fmt.Errorf("failed to get logs: %w", err)
	}

	logs := make(map[string][]corebridge.LogEntry)
	for level, entries := range dbLogs {
		logEntries := make([]corebridge.LogEntry, len(entries))
		for i, entry := range entries {
			logEntries[i] = corebridge.LogEntry{
				ID:    entry.ID,
				Level: entry.Level,
				Data:  entry.Data,
			}
		}
		logs[level] = logEntries
	}

	return &corebridge.GetLogsResponse{Success: true, Logs: logs}, nil
}
