package manager

import (
	"context"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

// Shutdown stops all live migrations, waiting up to grace for soft suspend then force-canceling runs.
func (m *Manager) Shutdown(ctx context.Context) {
	m.mu.RLock()
	live := make([]*migration.Migration, 0, len(m.runtimeByID))
	for _, rec := range m.runtimeByID {
		if rec != nil && rec.Migration != nil && rec.Migration.IsLive() {
			live = append(live, rec.Migration)
		}
	}
	m.mu.RUnlock()

	if len(live) == 0 {
		return
	}

	m.logger.Info().Int("count", len(live)).Msg("stopping live migrations for API shutdown")

	grace := migration.DefaultStopGracePeriod
	for _, mig := range live {
		if _, err := mig.Stop(); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", mig.ID).Msg("stop migration during shutdown")
		}
	}

	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		anyLive := false
		for _, mig := range live {
			if mig.IsLive() {
				anyLive = true
				break
			}
		}
		if !anyLive {
			m.logger.Info().Msg("all migrations stopped")
			return
		}
		select {
		case <-ctx.Done():
			goto force
		case <-time.After(200 * time.Millisecond):
		}
	}

force:
	for _, mig := range live {
		if !mig.IsLive() {
			continue
		}
		m.logger.Warn().Str("migration_id", mig.ID).Msg("force-stopping migration after grace period")
		if _, err := mig.ForceStop(); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", mig.ID).Msg("force stop migration during shutdown")
		}
	}

	forceDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(forceDeadline) {
		anyLive := false
		for _, mig := range live {
			if mig.IsLive() {
				anyLive = true
				break
			}
		}
		if !anyLive {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}
