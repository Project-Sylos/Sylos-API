package manager

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrations"
)

func convertResultView(r *migrations.ResultView) *corebridge.ResultView {
	if r == nil {
		return nil
	}
	return &corebridge.ResultView{
		RootSummary: corebridge.RootSummaryView{
			SrcRoots: r.RootSummary.SrcRoots,
			DstRoots: r.RootSummary.DstRoots,
		},
		Runtime: corebridge.RuntimeStatsView{
			Duration: r.Runtime.Duration,
			Src: corebridge.QueueStatsView{
				Name:         r.Runtime.Src.Name,
				Round:        r.Runtime.Src.Round,
				Pending:      r.Runtime.Src.Pending,
				InProgress:   r.Runtime.Src.InProgress,
				TotalTracked: r.Runtime.Src.TotalTracked,
				Workers:      r.Runtime.Src.Workers,
			},
			Dst: corebridge.QueueStatsView{
				Name:         r.Runtime.Dst.Name,
				Round:        r.Runtime.Dst.Round,
				Pending:      r.Runtime.Dst.Pending,
				InProgress:   r.Runtime.Dst.InProgress,
				TotalTracked: r.Runtime.Dst.TotalTracked,
				Workers:      r.Runtime.Dst.Workers,
			},
		},
		Verification: corebridge.VerificationView{
			SrcTotal:    r.Verification.SrcTotal,
			DstTotal:    r.Verification.DstTotal,
			SrcPending:  r.Verification.SrcPending,
			DstPending:  r.Verification.DstPending,
			SrcFailed:   r.Verification.SrcFailed,
			DstFailed:   r.Verification.DstFailed,
			DstNotOnSrc: r.Verification.DstNotOnSrc,
		},
	}
}

func convertStatus(s migrations.Status) corebridge.Status {
	return corebridge.Status{
		Migration: corebridge.Migration{
			ID:            s.ID,
			SourceID:      s.SourceID,
			DestinationID: s.DestinationID,
			StartedAt:     s.StartedAt,
			Status:        s.Status,
		},
		CompletedAt: s.CompletedAt,
		Error:       s.Error,
		Result:      convertResultView(s.Result),
	}
}

func convertResultToView(res *migration.Result) *corebridge.ResultView {
	if res == nil {
		return nil
	}
	return &corebridge.ResultView{
		RootSummary: corebridge.RootSummaryView{
			SrcRoots: res.RootSummary.SrcRoots,
			DstRoots: res.RootSummary.DstRoots,
		},
		Runtime: corebridge.RuntimeStatsView{
			Duration: res.Runtime.Duration.String(),
			Src: corebridge.QueueStatsView{
				Name:         res.Runtime.Src.Name,
				Round:        res.Runtime.Src.Round,
				Pending:      res.Runtime.Src.Pending,
				InProgress:   res.Runtime.Src.InProgress,
				TotalTracked: res.Runtime.Src.TotalTracked,
				Workers:      res.Runtime.Src.Workers,
			},
			Dst: corebridge.QueueStatsView{
				Name:         res.Runtime.Dst.Name,
				Round:        res.Runtime.Dst.Round,
				Pending:      res.Runtime.Dst.Pending,
				InProgress:   res.Runtime.Dst.InProgress,
				TotalTracked: res.Runtime.Dst.TotalTracked,
				Workers:      res.Runtime.Dst.Workers,
			},
		},
		Verification: corebridge.VerificationView{
			SrcTotal:    res.Verification.SrcTotal,
			DstTotal:    res.Verification.DstTotal,
			SrcPending:  res.Verification.SrcPending,
			DstPending:  res.Verification.DstPending,
			SrcFailed:   res.Verification.SrcFailed,
			DstFailed:   res.Verification.DstFailed,
			DstNotOnSrc: res.Verification.DstNotOnSrc,
		},
	}
}

func convertMetadata(meta metadata.MigrationMetadata) corebridge.MigrationMetadata {
	return corebridge.MigrationMetadata{
		ID:         meta.ID,
		Name:       meta.Name,
		ConfigPath: meta.ConfigPath,
		CreatedAt:  meta.CreatedAt,
	}
}
