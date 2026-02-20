package manager

import (
	"context"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

func (m *Manager) SubscribeProgress(ctx context.Context, id string) (<-chan corebridge.ProgressEvent, func(), error) {
	migCh, cancel, err := m.migrationsMgr.SubscribeProgress(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	ch := make(chan corebridge.ProgressEvent, cap(migCh))
	go func() {
		for migEvent := range migCh {
			ch <- corebridge.ProgressEvent{
				Event:     migEvent.Event,
				Timestamp: migEvent.Timestamp,
				Migration: convertStatus(migEvent.Migration),
				Source: corebridge.QueueStatsSnapshot{
					Round:        migEvent.Source.Round,
					Pending:      migEvent.Source.Pending,
					InProgress:   migEvent.Source.InProgress,
					TotalTracked: migEvent.Source.TotalTracked,
					Workers:      migEvent.Source.Workers,
				},
				Destination: corebridge.QueueStatsSnapshot{
					Round:        migEvent.Destination.Round,
					Pending:      migEvent.Destination.Pending,
					InProgress:   migEvent.Destination.InProgress,
					TotalTracked: migEvent.Destination.TotalTracked,
					Workers:      migEvent.Destination.Workers,
				},
			}
		}
		close(ch)
	}()
	return ch, cancel, nil
}

func (m *Manager) ToggleLogTerminal(ctx context.Context, enable bool, logAddress string) error {
	return m.terminalMgr.ToggleLogTerminal(ctx, enable, logAddress)
}
