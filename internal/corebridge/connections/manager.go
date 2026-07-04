package connections

import (
	"sync"
	"time"
)

// Record tracks in-memory OAuth state for a cloud connection.
type Record struct {
	ProviderID  string
	ServiceID   string
	MigrationID string
	AccessToken string
	ExpiresAt   time.Time
}

// Manager holds access tokens in process memory only.
type Manager struct {
	mu      sync.RWMutex
	records map[string]Record
}

func NewManager() *Manager {
	return &Manager{records: make(map[string]Record)}
}

func (m *Manager) Set(connectionID string, rec Record) {
	if m == nil || connectionID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[connectionID] = rec
}

func (m *Manager) Get(connectionID string) (Record, bool) {
	if m == nil || connectionID == "" {
		return Record{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.records[connectionID]
	return rec, ok
}

func (m *Manager) Delete(connectionID string) {
	if m == nil || connectionID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.records, connectionID)
}
