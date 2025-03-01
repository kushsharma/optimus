package job

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/salt/log"
)

var (
	ReplayStatusToSynced = []string{models.ReplayStatusReplayed, models.ReplayStatusInProgress, models.ReplayStatusAccepted}
	ReplayMessageSuccess = "all instances for this replay are successfully run"
	ReplayMessageFailed  = "instance run failure found"
	replaySyncerCounter  = promauto.NewCounter(prometheus.CounterOpts{
		Name: "replay_synced",
		Help: "Number of times replay syncer finished syncing",
	})
)

type Syncer struct {
	replaySpecFactory  ReplaySpecRepoFactory
	projectRepoFactory ProjectRepoFactory
	scheduler          models.SchedulerUnit
	Now                func() time.Time
	l                  log.Logger
}

func NewReplaySyncer(log log.Logger, replaySpecFactory ReplaySpecRepoFactory, projectRepoFactory ProjectRepoFactory, scheduler models.SchedulerUnit,
	timeFn func() time.Time) *Syncer {
	return &Syncer{
		l:                  log,
		replaySpecFactory:  replaySpecFactory,
		projectRepoFactory: projectRepoFactory,
		scheduler:          scheduler,
		Now:                timeFn,
	}
}

func (s Syncer) Sync(ctx context.Context, runTimeout time.Duration) error {
	replaySpecRepo := s.replaySpecFactory.New()

	projectSpecs, err := s.projectRepoFactory.New().GetAll(ctx)
	if err != nil {
		return err
	}
	for _, projectSpec := range projectSpecs {
		replaySpecs, err := replaySpecRepo.GetByProjectIDAndStatus(ctx, projectSpec.ID, ReplayStatusToSynced)
		if err != nil {
			if err == store.ErrResourceNotFound {
				return nil
			}
			return err
		}

		for _, replaySpec := range replaySpecs {
			// sync end state of replayed replays
			if replaySpec.Status == models.ReplayStatusReplayed {
				if err := s.syncRunningReplay(ctx, projectSpec, replaySpec, replaySpecRepo); err != nil {
					return err
				}
				continue
			}

			// sync timed out replays for accepted and in progress replays
			if err := s.syncTimedOutReplay(ctx, replaySpecRepo, replaySpec, runTimeout); err != nil {
				return err
			}
		}
	}

	replaySyncerCounter.Inc()
	return nil
}

func (s Syncer) syncTimedOutReplay(ctx context.Context, replaySpecRepo store.ReplaySpecRepository, replaySpec models.ReplaySpec, runTimeout time.Duration) error {
	runningTime := s.Now().Sub(replaySpec.CreatedAt)
	if runningTime > runTimeout {
		if updateStatusErr := replaySpecRepo.UpdateStatus(ctx, replaySpec.ID, models.ReplayStatusFailed, models.ReplayMessage{
			Type:    ReplayRunTimeout,
			Message: fmt.Sprintf("replay has been running since %s", replaySpec.CreatedAt.UTC().Format(TimestampLogFormat)),
		}); updateStatusErr != nil {
			s.l.Error("marking long running replay jobs as failed", "status error", updateStatusErr)
			return updateStatusErr
		}
	}
	return nil
}

func (s Syncer) syncRunningReplay(ctx context.Context, projectSpec models.ProjectSpec, replaySpec models.ReplaySpec, replaySpecRepo store.ReplaySpecRepository) error {
	stateSummary, err := s.checkInstanceState(ctx, projectSpec, replaySpec)
	if err != nil {
		return err
	}

	return updateCompletedReplays(ctx, s.l, stateSummary, replaySpecRepo, replaySpec.ID)
}

func (s Syncer) checkInstanceState(ctx context.Context, projectSpec models.ProjectSpec, replaySpec models.ReplaySpec) (map[models.JobRunState]int, error) {
	stateSummary := make(map[models.JobRunState]int)
	stateSummary[models.RunStateRunning] = 0
	stateSummary[models.RunStateFailed] = 0
	stateSummary[models.RunStateSuccess] = 0

	for _, node := range replaySpec.ExecutionTree.GetAllNodes() {
		batchEndDate := replaySpec.EndDate.AddDate(0, 0, 1).Add(time.Second * -1)
		jobStatusAllRuns, err := s.scheduler.GetJobRunStatus(ctx, projectSpec, node.Data.(models.JobSpec).Name, replaySpec.StartDate, batchEndDate, schedulerBatchSize)
		if err != nil {
			return nil, err
		}
		for _, jobStatus := range jobStatusAllRuns {
			stateSummary[jobStatus.State]++
		}
	}
	return stateSummary, nil
}

func updateCompletedReplays(ctx context.Context, l log.Logger, stateSummary map[models.JobRunState]int, replaySpecRepo store.ReplaySpecRepository, replayID uuid.UUID) error {
	if stateSummary[models.RunStateRunning] == 0 && stateSummary[models.RunStateFailed] > 0 {
		if updateStatusErr := replaySpecRepo.UpdateStatus(ctx, replayID, models.ReplayStatusFailed, models.ReplayMessage{
			Type:    models.ReplayStatusFailed,
			Message: ReplayMessageFailed,
		}); updateStatusErr != nil {
			l.Error("marking replay as failed error", "status error", updateStatusErr)
			return updateStatusErr
		}
	} else if stateSummary[models.RunStateRunning] == 0 && stateSummary[models.RunStateFailed] == 0 && stateSummary[models.RunStateSuccess] > 0 {
		if updateStatusErr := replaySpecRepo.UpdateStatus(ctx, replayID, models.ReplayStatusSuccess, models.ReplayMessage{
			Type:    models.ReplayStatusSuccess,
			Message: ReplayMessageSuccess,
		}); updateStatusErr != nil {
			l.Error("marking replay as success error", "status error", updateStatusErr)
			return updateStatusErr
		}
		l.Info("successfully marked replay", "replay id", replayID)
	}
	return nil
}
