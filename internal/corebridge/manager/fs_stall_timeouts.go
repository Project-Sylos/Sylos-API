// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package manager

import (
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

// FS stall ProgressWatchdog budgets applied to new phase runs from user prefs.
type fsStallTimeoutPrefs struct {
	mu                      sync.RWMutex
	traversal, delete, copy time.Duration // 0 = unlimited
}

func (m *Manager) initFSStallTimeoutPrefs() {
	if m == nil {
		return
	}
	def := 60 * time.Second
	m.fsStall.traversal = def
	m.fsStall.delete = def
	m.fsStall.copy = def
}

// SetFSStallTimeoutSecs stores per-kind stall budgets for the next phase start.
// Seconds: 0 = unlimited; negative values keep the previous/default 60s.
func (m *Manager) SetFSStallTimeoutSecs(traversalSec, deleteSec, copySec int) {
	if m == nil {
		return
	}
	m.fsStall.mu.Lock()
	defer m.fsStall.mu.Unlock()
	m.fsStall.traversal = queue.StallTimeoutFromSeconds(traversalSec, 60)
	m.fsStall.delete = queue.StallTimeoutFromSeconds(deleteSec, 60)
	m.fsStall.copy = queue.StallTimeoutFromSeconds(copySec, 60)
}

func (m *Manager) fsStallTimeoutsLocked() (trav, del, copy time.Duration) {
	m.fsStall.mu.RLock()
	defer m.fsStall.mu.RUnlock()
	return m.fsStall.traversal, m.fsStall.delete, m.fsStall.copy
}
