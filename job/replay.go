package job

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/core/cron"
	"github.com/odpf/optimus/core/set"
	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
)

const (
	// ReplayDateFormat YYYY-mm-dd for replay dates and dag start date
	ReplayDateFormat = "2006-01-02"
)

func (srv *Service) prepareJobSpecMap(ctx context.Context, replayRequest models.ReplayRequest) (map[string]models.JobSpec, error) {
	projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(replayRequest.Project)
	jobSpecs, err := srv.GetDependencyResolvedSpecs(ctx, replayRequest.Project, projectJobSpecRepo, nil)
	if err != nil {
		return nil, err
	}
	jobSpecMap := make(map[string]models.JobSpec)
	for _, currSpec := range jobSpecs {
		jobSpecMap[currSpec.Name] = currSpec
	}
	return jobSpecMap, nil
}

func (srv *Service) ReplayDryRun(ctx context.Context, replayRequest models.ReplayRequest) (*tree.TreeNode, error) {
	jobSpecMap, err := srv.prepareJobSpecMap(ctx, replayRequest)
	if err != nil {
		return nil, err
	}
	replayRequest.JobSpecMap = jobSpecMap

	return prepareReplayExecutionTree(replayRequest)
}

func (srv *Service) Replay(ctx context.Context, replayRequest models.ReplayRequest) (string, error) {
	jobSpecMap, err := srv.prepareJobSpecMap(ctx, replayRequest)
	if err != nil {
		return "", err
	}
	replayRequest.JobSpecMap = jobSpecMap

	replayUUID, err := srv.replayManager.Replay(ctx, replayRequest)
	if err != nil {
		return "", err
	}
	return replayUUID, nil
}

// prepareReplayExecutionTree creates a execution tree for replay operation
func prepareReplayExecutionTree(replayRequest models.ReplayRequest) (*tree.TreeNode, error) {
	replayJobSpec, found := replayRequest.JobSpecMap[replayRequest.Job.Name]
	if !found {
		return nil, fmt.Errorf("couldn't find any job with name %s", replayRequest.Job.Name)
	}

	// compute runs that require replay
	dagTree := tree.NewMultiRootTree()
	parentNode := tree.NewTreeNode(replayJobSpec)
	if runs, err := getRunsBetweenDates(replayRequest.Start, replayRequest.End, replayJobSpec.Schedule.Interval); err == nil {
		for _, run := range runs {
			parentNode.Runs.Add(run)
		}
	} else {
		return nil, err
	}
	dagTree.AddNode(parentNode)

	rootInstance, err := populateDownstreamDAGs(dagTree, replayJobSpec, replayRequest.JobSpecMap)
	if err != nil {
		return nil, err
	}

	rootInstance, err = populateDownstreamRuns(rootInstance)
	if err != nil {
		return nil, err
	}

	return rootInstance, nil
}

func findOrCreateDAGNode(dagTree *tree.MultiRootTree, dagSpec models.JobSpec) *tree.TreeNode {
	node, ok := dagTree.GetNodeByName(dagSpec.Name)
	if !ok {
		node = tree.NewTreeNode(dagSpec)
		dagTree.AddNode(node)
	}
	return node
}

func populateDownstreamRuns(parentNode *tree.TreeNode) (*tree.TreeNode, error) {
	for idx, childNode := range parentNode.Dependents {
		childDag := childNode.Data.(models.JobSpec)
		taskSchedule, err := cron.ParseCronSchedule(childDag.Schedule.Interval)
		if err != nil {
			return nil, err
		}

		for _, parentRunDateRaw := range parentNode.Runs.Values() { //
			parentRunDate := parentRunDateRaw.(time.Time)

			// subtract 1 day to make end inclusive
			parentEndDate := parentRunDate.Add(time.Hour * -24).Add(childDag.Task.Window.Size)

			// subtracting 1 sec to accommodate next call of cron
			// where parent task and current task has same scheduled interval
			taskFirstEffectedRun := taskSchedule.Next(parentRunDate.Add(-1 * time.Second))

			//make sure it is after current dag start date
			if taskFirstEffectedRun.Before(childDag.Schedule.StartDate) {
				continue
			}

			runs, err := getRunsBetweenDates(parentRunDate, parentEndDate, childDag.Schedule.Interval)
			if err != nil {
				return nil, errors.Wrap(err, "failed to find runs with parent dag")
			}
			for _, run := range runs {
				childNode.Runs.Add(run)
			}
		}
		updatedChildNode, err := populateDownstreamRuns(childNode)
		if err != nil {
			return nil, err
		}
		parentNode.Dependents[idx] = updatedChildNode
	}
	return parentNode, nil
}

// getRunsBetweenDates provides execution runs from start to end following a schedule interval
// start and end both are inclusive
func getRunsBetweenDates(start time.Time, end time.Time, schedule string) ([]time.Time, error) {
	var runs []time.Time

	// standard cron parser without descriptors
	schd, err := cron.ParseCronSchedule(schedule)
	if err != nil {
		return nil, err
	}

	replayRunEnd := schd.Next(end)
	for replayRunEnd.Before(end.AddDate(0, 0, 1)) {
		replayRunEnd = schd.Next(replayRunEnd)
	}

	// loop until start date reaches end date
	for run := schd.Next(start.Add(time.Second * -1)); run.Before(replayRunEnd); run = schd.Next(run) {
		runs = append(runs, run)
	}

	return runs, nil
}

func (srv *Service) GetReplayStatus(ctx context.Context, replayRequest models.ReplayRequest) (models.ReplayState, error) {
	// Get replay
	replaySpec, err := srv.replayManager.GetReplay(ctx, replayRequest.ID)
	if err != nil {
		return models.ReplayState{}, err
	}

	// updating tree with status per run
	rootInstance, err := srv.prepareReplayStatusTree(ctx, replayRequest, replaySpec)
	if err != nil {
		return models.ReplayState{}, err
	}

	return models.ReplayState{
		Status: replaySpec.Status,
		Node:   rootInstance,
	}, nil
}

func TimeOfJobStatusComparator(a, b interface{}) int {
	aAsserted := a.(models.JobStatus).ScheduledAt
	bAsserted := b.(models.JobStatus).ScheduledAt
	switch {
	case aAsserted.After(bAsserted):
		return 1
	case aAsserted.Before(bAsserted):
		return -1
	default:
		return 0
	}
}

// prepareReplayStatusTree update execution tree with the status per run
func (srv *Service) prepareReplayStatusTree(ctx context.Context, replayRequest models.ReplayRequest, replaySpec models.ReplaySpec) (*tree.TreeNode, error) {
	runsWithStatus := set.NewTreeSetWith(TimeOfJobStatusComparator)
	jobStatusList, err := srv.replayManager.GetRunStatus(ctx, replayRequest.Project, replaySpec.StartDate, replaySpec.EndDate, replaySpec.Job.Name)
	if err != nil {
		return nil, err
	}
	for _, jobStatus := range jobStatusList {
		runsWithStatus.Add(jobStatus)
	}
	replaySpec.ExecutionTree.Runs = runsWithStatus
	return srv.populateDownstreamRunsWithStatus(ctx, replayRequest.Project, replaySpec.StartDate, replaySpec.EndDate, replaySpec.ExecutionTree)
}

func (srv *Service) populateDownstreamRunsWithStatus(ctx context.Context, projectSpec models.ProjectSpec, startDate time.Time, endDate time.Time, parentNode *tree.TreeNode) (*tree.TreeNode, error) {
	for _, dependent := range parentNode.Dependents {
		runsWithStatus := set.NewTreeSetWith(TimeOfJobStatusComparator)
		jobStatusList, err := srv.replayManager.GetRunStatus(ctx, projectSpec, startDate, endDate, dependent.Data.(models.JobSpec).Name)
		if err != nil {
			return nil, err
		}
		for _, jobStatus := range jobStatusList {
			runsWithStatus.Add(jobStatus)
		}
		dependent.Runs = runsWithStatus
		_, err = srv.populateDownstreamRunsWithStatus(ctx, projectSpec, startDate, endDate, dependent)
		if err != nil {
			return nil, err
		}
	}
	return parentNode, nil
}

func (srv *Service) GetReplayList(ctx context.Context, projectUUID uuid.UUID) ([]models.ReplaySpec, error) {
	return srv.replayManager.GetReplayList(ctx, projectUUID)
}
