package manager

import (
	"context"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

// ReconcileOrphanedMigrations heals registry rows stuck in active phases with no live
// run (typical after process kill). Sets unclean_interrupt and *-suspended /
// *-finalize-failed so Resume lists show recoverable state. Never starts work.
func (m *Manager) ReconcileOrphanedMigrations(ctx context.Context) {
	if m == nil || m.apiDB == nil {
		return
	}
	recs, err := m.apiDB.ListMigrations()
	if err != nil {
		m.logger.Warn().Err(err).Msg("crash recovery: list migrations")
		return
	}
	for _, rec := range recs {
		if !registryPhaseNeedsHeal(rec.Phase) {
			continue
		}
		if ctx != nil && ctx.Err() != nil {
			return
		}
		mig, err := m.GetMigration(context.Background(), rec.ID)
		if err != nil || mig == nil {
			m.logger.Warn().Err(err).Str("migration_id", rec.ID).Msg("crash recovery: open migration")
			continue
		}
		if mig.IsLive() {
			continue
		}
		changed, err := mig.NormalizeDeadInProgressToSuspended()
		if err != nil {
			m.logger.Warn().Err(err).Str("migration_id", rec.ID).Msg("crash recovery: normalize")
			continue
		}
		if !changed && mig.Phase() == migration.PhaseAborted {
			// Ensure unclean + suspended even if Normalize no-op edge cases.
			changed, err = mig.NormalizeDeadInProgressToSuspended()
			if err != nil {
				m.logger.Warn().Err(err).Str("migration_id", rec.ID).Msg("crash recovery: normalize aborted")
				continue
			}
		}
		phase := mig.Phase()
		unclean := mig.HasUncleanInterrupt()
		m.persistMigrationListHints(rec.ID, phase, nil, nil, &unclean)
		if changed {
			m.logger.Info().
				Str("migration_id", rec.ID).
				Str("phase", phase).
				Bool("unclean", mig.HasUncleanInterrupt()).
				Msg("crash recovery: healed orphaned migration phase")
		}
	}
}

func registryPhaseNeedsHeal(phase string) bool {
	p := strings.TrimSpace(strings.ToLower(phase))
	if p == "" {
		return false
	}
	if p == migration.PhaseAborted {
		return true
	}
	return strings.HasSuffix(p, "-in-progress") || strings.HasSuffix(p, "-finalizing")
}
