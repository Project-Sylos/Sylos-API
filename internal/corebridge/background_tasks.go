package corebridge

import (
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// BackgroundTaskType represents the type of background task
type BackgroundTaskType string

const (
	BackgroundTaskTypeETL                  BackgroundTaskType = "etl"
	BackgroundTaskTypeExclusionPropagate   BackgroundTaskType = "exclusion_propagate"
	BackgroundTaskTypeUnexclusionPropagate BackgroundTaskType = "unexclusion_propagate"
	BackgroundTaskTypeExclusionSweep       BackgroundTaskType = "exclusion_sweep"
	BackgroundTaskTypeRetrySweep           BackgroundTaskType = "retry_sweep"
)

// BackgroundTaskStatus represents the status of a background task
type BackgroundTaskStatus string

const (
	BackgroundTaskStatusRunning   BackgroundTaskStatus = "running"
	BackgroundTaskStatusCompleted BackgroundTaskStatus = "completed"
	BackgroundTaskStatusFailed    BackgroundTaskStatus = "failed"
)

// BackgroundTask represents a running background task
type BackgroundTask struct {
	ID          string                 `json:"id"`
	Type        BackgroundTaskType     `json:"type"`
	Status      BackgroundTaskStatus   `json:"status"`
	StartedAt   time.Time              `json:"startedAt"`
	CompletedAt *time.Time             `json:"completedAt,omitempty"`
	Error       string                 `json:"error,omitempty"`
	Progress    map[string]interface{} `json:"progress,omitempty"` // Task-specific progress info
	Path        string                 `json:"path,omitempty"`     // Path for exclusion/unexclusion propagation tasks
}

// BackgroundTaskManager manages background tasks for migrations
type BackgroundTaskManager struct {
	tasks  map[string]map[string]*BackgroundTask // migrationID -> taskID -> task
	mu     sync.RWMutex
	logger zerolog.Logger
}

// NewBackgroundTaskManager creates a new background task manager
func NewBackgroundTaskManager(logger zerolog.Logger) *BackgroundTaskManager {
	return &BackgroundTaskManager{
		tasks:  make(map[string]map[string]*BackgroundTask),
		logger: logger,
	}
}

// StartTask starts a new background task
func (m *BackgroundTaskManager) StartTask(migrationID string, taskType BackgroundTaskType) string {
	return m.StartTaskWithPath(migrationID, taskType, "")
}

// StartTaskWithPath starts a new background task with an associated path
func (m *BackgroundTaskManager) StartTaskWithPath(migrationID string, taskType BackgroundTaskType, path string) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Generate task ID (simple timestamp-based for now)
	taskID := fmt.Sprintf("%s-%d", taskType, time.Now().UnixNano())

	if m.tasks[migrationID] == nil {
		m.tasks[migrationID] = make(map[string]*BackgroundTask)
	}

	task := &BackgroundTask{
		ID:        taskID,
		Type:      taskType,
		Status:    BackgroundTaskStatusRunning,
		StartedAt: time.Now().UTC(),
		Progress:  make(map[string]interface{}),
		Path:      path,
	}

	m.tasks[migrationID][taskID] = task

	m.logger.Info().
		Str("migration_id", migrationID).
		Str("task_id", taskID).
		Str("task_type", string(taskType)).
		Str("path", path).
		Msg("started background task")

	return taskID
}

// CompleteTask marks a task as completed
func (m *BackgroundTaskManager) CompleteTask(migrationID, taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tasks, exists := m.tasks[migrationID]
	if !exists {
		return
	}

	task, exists := tasks[taskID]
	if !exists {
		return
	}

	now := time.Now().UTC()
	task.Status = BackgroundTaskStatusCompleted
	task.CompletedAt = &now

	m.logger.Info().
		Str("migration_id", migrationID).
		Str("task_id", taskID).
		Str("task_type", string(task.Type)).
		Dur("duration", now.Sub(task.StartedAt)).
		Msg("completed background task")
}

// FailTask marks a task as failed
func (m *BackgroundTaskManager) FailTask(migrationID, taskID string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tasks, exists := m.tasks[migrationID]
	if !exists {
		return
	}

	task, exists := tasks[taskID]
	if !exists {
		return
	}

	now := time.Now().UTC()
	task.Status = BackgroundTaskStatusFailed
	task.CompletedAt = &now
	if err != nil {
		task.Error = err.Error()
	}

	m.logger.Error().
		Err(err).
		Str("migration_id", migrationID).
		Str("task_id", taskID).
		Str("task_type", string(task.Type)).
		Msg("background task failed")
}

// UpdateTaskProgress updates the progress of a running task
func (m *BackgroundTaskManager) UpdateTaskProgress(migrationID, taskID string, progress map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tasks, exists := m.tasks[migrationID]
	if !exists {
		return
	}

	task, exists := tasks[taskID]
	if !exists {
		return
	}

	for k, v := range progress {
		task.Progress[k] = v
	}
}

// GetTasks returns all tasks for a migration
func (m *BackgroundTaskManager) GetTasks(migrationID string) []BackgroundTask {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tasks, exists := m.tasks[migrationID]
	if !exists {
		return []BackgroundTask{}
	}

	result := make([]BackgroundTask, 0, len(tasks))
	for _, task := range tasks {
		result = append(result, *task)
	}

	return result
}

// GetRunningTasks returns only running tasks for a migration
func (m *BackgroundTaskManager) GetRunningTasks(migrationID string) []BackgroundTask {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tasks, exists := m.tasks[migrationID]
	if !exists {
		return []BackgroundTask{}
	}

	result := make([]BackgroundTask, 0)
	for _, task := range tasks {
		if task.Status == BackgroundTaskStatusRunning {
			result = append(result, *task)
		}
	}

	return result
}

// HasRunningTask checks if there's a running task of a specific type
func (m *BackgroundTaskManager) HasRunningTask(migrationID string, taskType BackgroundTaskType) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tasks, exists := m.tasks[migrationID]
	if !exists {
		return false
	}

	for _, task := range tasks {
		if task.Type == taskType && task.Status == BackgroundTaskStatusRunning {
			return true
		}
	}

	return false
}

// CleanupCompletedTasks removes completed/failed tasks older than the specified duration
func (m *BackgroundTaskManager) CleanupCompletedTasks(olderThan time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().UTC().Add(-olderThan)

	for migrationID, tasks := range m.tasks {
		for taskID, task := range tasks {
			if task.Status != BackgroundTaskStatusRunning {
				if task.CompletedAt != nil && task.CompletedAt.Before(cutoff) {
					delete(tasks, taskID)
					m.logger.Debug().
						Str("migration_id", migrationID).
						Str("task_id", taskID).
						Msg("cleaned up old background task")
				}
			}
		}

		// Remove migration entry if no tasks left
		if len(tasks) == 0 {
			delete(m.tasks, migrationID)
		}
	}
}
