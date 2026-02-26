package database

import (
	"context"
	"fmt"
	"strconv"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"github.com/rs/zerolog"
)

// InspectMigrationStatusFromDB opens the migration DB at dbPath, inspects it via the engine,
// and returns the MigrationStatus. Use when there is no live controller.
func InspectMigrationStatusFromDB(ctx context.Context, logger zerolog.Logger, dbPath string) (migration.MigrationStatus, error) {
	database, err := db.Open(db.Options{Path: dbPath})
	if err != nil {
		return migration.MigrationStatus{}, fmt.Errorf("failed to open database: %w", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			logger.Warn().Err(err).Str("db_path", dbPath).Msg("failed to close database after inspection")
		}
	}()

	status, err := migration.InspectMigrationStatus(database)
	if err != nil {
		return migration.MigrationStatus{}, fmt.Errorf("failed to inspect migration status: %w", err)
	}
	return status, nil
}

// GetLogsFromDB opens the migration DB at dbPath, reads up to limitPerLevel log entries per level from the logs table,
// and returns them grouped by level. Keys are level strings (e.g. "info", "error").
func GetLogsFromDB(ctx context.Context, dbPath string, limitPerLevel int) (map[string][]LogEntry, error) {
	database, err := db.Open(db.Options{Path: dbPath})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	conn, err := database.GetDB()
	if err != nil {
		return nil, err
	}

	rows, err := conn.QueryContext(ctx,
		`SELECT id, level, message, component, entity, entity_id, queue FROM logs ORDER BY id DESC LIMIT $1`,
		limitPerLevel*10, // rough cap total; we'll group by level and trim per level if needed
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query logs: %w", err)
	}
	defer rows.Close()

	byLevel := make(map[string][]LogEntry)
	for rows.Next() {
		var id int64
		var level, message, component, entity, entityID, queue string
		if err := rows.Scan(&id, &level, &message, &component, &entity, &entityID, &queue); err != nil {
			return nil, err
		}
		entries := byLevel[level]
		if len(entries) >= limitPerLevel {
			continue
		}
		byLevel[level] = append(entries, LogEntry{
			ID:    strconv.FormatInt(id, 10),
			Level: level,
			Data: map[string]any{
				"message":   message,
				"component": component,
				"entity":    entity,
				"entity_id": entityID,
				"queue":     queue,
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return byLevel, nil
}
