package manager

import (
	"context"
	"strconv"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

func (m *Manager) SubscribeProgress(ctx context.Context, id string) (<-chan corebridge.ProgressEvent, func(), error) {
	ch := make(chan corebridge.ProgressEvent, 16)

	m.mu.Lock()
	if _, ok := m.progressByID[id]; !ok {
		m.progressByID[id] = make(map[string]chan corebridge.ProgressEvent)
	}
	m.progressCounter++
	subID := "sub-" + strconv.FormatUint(m.progressCounter, 10)
	m.progressByID[id][subID] = ch
	m.mu.Unlock()

	status, _ := m.GetMigrationStatus(ctx, id)
	ch <- corebridge.ProgressEvent{
		Event:     "subscribed",
		Timestamp: status.StartedAt,
		Migration: status,
	}

	cancel := func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if subs, ok := m.progressByID[id]; ok {
			if existing, ok := subs[subID]; ok {
				close(existing)
				delete(subs, subID)
			}
			if len(subs) == 0 {
				delete(m.progressByID, id)
			}
		}
	}

	go func() {
		<-ctx.Done()
		cancel()
	}()
	return ch, cancel, nil
}
