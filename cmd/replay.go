package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/set"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"github.com/xlab/treeprint"
	"google.golang.org/grpc"
)

var (
	replayTimeout = time.Minute * 1
)

type taskRunBlock struct {
	name   string
	height int
	runs   set.Set
}

func taskRunBlockComperator(a, b interface{}) int {
	aAsserted := a.(taskRunBlock)
	bAsserted := b.(taskRunBlock)
	switch {
	case aAsserted.height < bAsserted.height:
		return -1
	case aAsserted.height > bAsserted.height:
		return 1
	}
	return strings.Compare(aAsserted.name, bAsserted.name)
}

//formatRunsPerJobInstance returns a hashmap with Job -> Runs[] mapping
func formatRunsPerJobInstance(instance *pb.ReplayExecutionTreeNode, taskReruns map[string]taskRunBlock, height int) {
	if _, ok := taskReruns[instance.JobName]; !ok {
		taskReruns[instance.JobName] = taskRunBlock{
			name:   instance.JobName,
			height: height,
			runs:   set.NewTreeSetWithTimeComparator(),
		}
	}

	for _, taskRun := range instance.Runs {
		taskReruns[instance.JobName].runs.Add(taskRun.AsTime())
	}
	for _, child := range instance.Dependents {
		formatRunsPerJobInstance(child, taskReruns, height+1)
	}
}

func replayCommand(l log.Logger, conf config.Provider) *cli.Command {
	cmd := &cli.Command{
		Use:   "replay",
		Short: "re-running jobs in order to update data for older dates/partitions",
		Long:  `Backfill etl job and all of its downstream dependencies`,
	}
	cmd.AddCommand(replayRunSubCommand(l, conf))
	cmd.AddCommand(replayStatusSubCommand(l, conf))
	cmd.AddCommand(replayListSubCommand(l, conf))
	return cmd
}

func replayRunSubCommand(l log.Logger, conf config.Provider) *cli.Command {
	dryRun := false
	forceRun := false
	var (
		replayProject string
		namespace     string
	)

	reCmd := &cli.Command{
		Use:     "run",
		Short:   "run replay operation on a dag based on provided date range",
		Example: "optimus replay run optimus.dag.name 2020-02-03 2020-02-05",
		Long: `
This operation takes three arguments, first is DAG name[required]
used in optimus specification, second is start date[required] of
replay, third is end date[optional] of replay. 
ReplayDryRun date ranges are inclusive.
		`,
		Args: func(cmd *cli.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("dag name is required")
			}
			if len(args) < 2 {
				return errors.New("replay start date is required")
			}
			return nil
		},
	}
	reCmd.Flags().BoolVarP(&dryRun, "dry-run", "", dryRun, "do a trial run with no permanent changes")
	reCmd.Flags().StringVarP(&replayProject, "project", "p", "", "project name of optimus managed repository")
	reCmd.MarkFlagRequired("project")
	reCmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace of deployee")
	reCmd.MarkFlagRequired("namespace")
	reCmd.Flags().BoolVarP(&forceRun, "force", "f", forceRun, "run replay even if a previous run is in progress")

	reCmd.RunE = func(cmd *cli.Command, args []string) error {
		endDate := args[1]
		if len(args) >= 3 {
			endDate = args[2]
		}
		if err := printReplayExecutionTree(l, replayProject, namespace, args[0], args[1], endDate, conf); err != nil {
			return err
		}
		if dryRun {
			//if only dry run, exit now
			return nil
		}

		proceedWithReplay := "Yes"
		if err := survey.AskOne(&survey.Select{
			Message: "Proceed with replay?",
			Options: []string{"Yes", "No"},
			Default: "Yes",
		}, &proceedWithReplay); err != nil {
			return err
		}

		if proceedWithReplay == "No" {
			l.Info("aborting...")
			return nil
		}

		replayId, err := runReplayRequest(l, replayProject, namespace, args[0], args[1], endDate, conf, forceRun)
		if err != nil {
			return err
		}
		l.Info(fmt.Sprintf("🚀 replay request created with id %s", replayId))
		return nil
	}
	return reCmd
}

func printReplayExecutionTree(l log.Logger, projectName, namespace, jobName, startDate, endDate string, conf config.Provider) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, conf.GetHost()); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("can't reach optimus service")
		}
		return err
	}
	defer conn.Close()

	replayRequestTimeout, replayRequestCancel := context.WithTimeout(context.Background(), replayTimeout)
	defer replayRequestCancel()

	l.Info("please wait...")
	runtime := pb.NewRuntimeServiceClient(conn)
	replayRequest := &pb.ReplayDryRunRequest{
		ProjectName: projectName,
		JobName:     jobName,
		Namespace:   namespace,
		StartDate:   startDate,
		EndDate:     endDate,
	}
	replayDryRunResponse, err := runtime.ReplayDryRun(replayRequestTimeout, replayRequest)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("replay dry run took too long, timing out")
		}
		return errors.Wrapf(err, "request failed for job %s", jobName)
	}

	printReplayDryRunResponse(l, replayRequest, replayDryRunResponse)
	return nil
}

func printReplayDryRunResponse(l log.Logger, replayRequest *pb.ReplayDryRunRequest, replayDryRunResponse *pb.ReplayDryRunResponse) {
	l.Info(fmt.Sprintf("For %s project and %s namespace\n", coloredNotice(replayRequest.ProjectName), coloredNotice(replayRequest.Namespace)))
	l.Info(coloredNotice("REPLAY RUNS"))
	table := tablewriter.NewWriter(l.Writer())
	table.SetBorder(false)
	table.SetHeader([]string{
		"Index",
		"Job",
		"Run",
	})
	taskRerunsMap := make(map[string]taskRunBlock)
	formatRunsPerJobInstance(replayDryRunResponse.Response, taskRerunsMap, 0)

	//sort run block
	taskRerunsSorted := set.NewTreeSetWith(taskRunBlockComperator)
	for _, block := range taskRerunsMap {
		taskRerunsSorted.Add(block)
	}
	for idx, rawBlock := range taskRerunsSorted.Values() {
		runBlock := rawBlock.(taskRunBlock)
		runTimes := []string{}
		for _, runRaw := range runBlock.runs.Values() {
			runTimes = append(runTimes, runRaw.(time.Time).String())
		}

		table.Append([]string{fmt.Sprintf("%d", idx+1), runBlock.name, strings.Join(runTimes, "\n")})
	}
	table.Render()

	//print tree
	l.Info(coloredNotice("\nDEPENDENCY TREE"))
	l.Info(fmt.Sprintf("%s", printExecutionTree(replayDryRunResponse.Response, treeprint.New())))
}

// printExecutionTree creates a ascii tree to visually inspect
// instance dependencies that will be recomputed after replay operation
func printExecutionTree(instance *pb.ReplayExecutionTreeNode, tree treeprint.Tree) treeprint.Tree {
	subtree := tree.AddBranch(instance.JobName)
	runBranch := subtree.AddMetaBranch(len(instance.Runs), "runs")
	for _, run := range instance.Runs {
		if run.AsTime().Before(time.Now()) {
			runBranch.AddNode(run.AsTime().Format(time.RFC3339))
		}
	}

	for _, childInstance := range instance.Dependents {
		printExecutionTree(childInstance, subtree)
	}
	return tree
}

func runReplayRequest(l log.Logger, projectName, namespace, jobName, startDate, endDate string, conf config.Provider, forceRun bool) (string, error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	conn, err := createConnection(dialTimeoutCtx, conf.GetHost())
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("can't reach optimus service")
		}
		return "", err
	}
	defer conn.Close()

	replayRequestTimeout, replayRequestCancel := context.WithTimeout(context.Background(), replayTimeout)
	defer replayRequestCancel()

	l.Info("firing the replay request...")
	if forceRun {
		l.Info("force running replay even if its already in progress")
	}
	runtime := pb.NewRuntimeServiceClient(conn)
	replayRequest := &pb.ReplayRequest{
		ProjectName: projectName,
		JobName:     jobName,
		Namespace:   namespace,
		StartDate:   startDate,
		EndDate:     endDate,
		Force:       forceRun,
	}
	replayResponse, err := runtime.Replay(replayRequestTimeout, replayRequest)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("replay request took too long, timing out")
		}
		return "", errors.Wrapf(err, "request failed for job %s", jobName)
	}
	return replayResponse.Id, nil
}

func replayStatusSubCommand(l log.Logger, conf config.Provider) *cli.Command {
	var (
		replayProject string
	)

	reCmd := &cli.Command{
		Use:     "status",
		Short:   "get status of a replay using its ID",
		Example: "optimus replay status replay-id",
		Long: `
The status command is used to fetch the current replay progress.
It takes one argument, replay ID[required] that gets generated when starting a replay. 
		`,
		Args: func(cmd *cli.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("replay ID is required")
			}
			return nil
		},
	}
	reCmd.Flags().StringVarP(&replayProject, "project", "p", "", "project name of optimus managed repository")
	reCmd.MarkFlagRequired("project")
	reCmd.RunE = func(cmd *cli.Command, args []string) error {
		dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
		defer dialCancel()

		conn, err := createConnection(dialTimeoutCtx, conf.GetHost())
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Info("can't reach optimus service")
			}
			return err
		}
		defer conn.Close()

		replayRequestTimeout, replayRequestCancel := context.WithTimeout(context.Background(), replayTimeout)
		defer replayRequestCancel()

		runtime := pb.NewRuntimeServiceClient(conn)
		replayStatusRequest := &pb.GetReplayStatusRequest{
			Id:          args[0],
			ProjectName: replayProject,
		}
		replayResponse, err := runtime.GetReplayStatus(replayRequestTimeout, replayStatusRequest)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Info("replay request took too long, timing out")
			}
			return errors.Wrapf(err, "request getting status for replay %s is failed", args[0])
		}
		printReplayStatusResponse(l, replayResponse)
		return nil
	}
	return reCmd
}

func printReplayStatusResponse(l log.Logger, replayStatusResponse *pb.GetReplayStatusResponse) {
	if replayStatusResponse.State == models.ReplayStatusFailed {
		l.Info(fmt.Sprintf("\nThis replay has been marked as %s", coloredNotice(models.ReplayStatusFailed)))
	} else if replayStatusResponse.State == models.ReplayStatusReplayed {
		l.Info(fmt.Sprintf("\nThis replay is still %s", coloredNotice("running")))
	} else if replayStatusResponse.State == models.ReplayStatusSuccess {
		l.Info(fmt.Sprintf("\nThis replay has been marked as %s", coloredSuccess(models.ReplayStatusSuccess)))
	}
	l.Info(coloredNotice("Latest Instances Status"))
	l.Info(fmt.Sprintf("%s", printStatusTree(replayStatusResponse.Response, treeprint.New())))
}

func printStatusTree(instance *pb.ReplayStatusTreeNode, tree treeprint.Tree) treeprint.Tree {
	subtree := tree.AddBranch(instance.JobName)
	runBranch := subtree.AddMetaBranch(len(instance.Runs), "runs")
	for _, run := range instance.Runs {
		runBranch.AddNode(fmt.Sprintf("%s (%s)", run.Run.AsTime().Format(time.RFC3339), run.State))
	}

	for _, childInstance := range instance.Dependents {
		printStatusTree(childInstance, subtree)
	}
	return tree
}

func replayListSubCommand(l log.Logger, conf config.Provider) *cli.Command {
	var (
		replayProject string
	)

	reCmd := &cli.Command{
		Use:     "list",
		Short:   "get list of a replay using project ID",
		Example: "optimus replay status replay-id",
		Long: `
The list command is used to fetch the recent replay in one project. 
		`,
	}
	reCmd.Flags().StringVarP(&replayProject, "project", "p", "", "project name of optimus managed repository")
	reCmd.MarkFlagRequired("project")
	reCmd.RunE = func(cmd *cli.Command, args []string) error {
		dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
		defer dialCancel()

		conn, err := createConnection(dialTimeoutCtx, conf.GetHost())
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Info("can't reach optimus service")
			}
			return err
		}
		defer conn.Close()

		replayRequestTimeout, replayRequestCancel := context.WithTimeout(context.Background(), replayTimeout)
		defer replayRequestCancel()

		runtime := pb.NewRuntimeServiceClient(conn)
		replayStatusRequest := &pb.ListReplaysRequest{
			ProjectName: replayProject,
		}
		replayResponse, err := runtime.ListReplays(replayRequestTimeout, replayStatusRequest)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Info("replay request took too long, timing out")
			}
			return errors.Wrapf(err, "failed to get replay requests")
		}
		if len(replayResponse.ReplayList) == 0 {
			l.Info(fmt.Sprintf("no replays were found in %s project.", replayProject))
		} else {
			printReplayListResponse(l, replayResponse)
		}
		return nil
	}
	return reCmd
}

func printReplayListResponse(l log.Logger, replayListResponse *pb.ListReplaysResponse) {
	l.Info(coloredNotice("LATEST REPLAY"))
	table := tablewriter.NewWriter(l.Writer())
	table.SetBorder(false)
	table.SetHeader([]string{
		"ID",
		"Job",
		"Start Date",
		"End Date",
		"Requested At",
		"Status",
	})

	for _, replaySpec := range replayListResponse.ReplayList {
		table.Append([]string{replaySpec.Id, replaySpec.JobName, replaySpec.StartDate.AsTime().Format(models.JobDatetimeLayout),
			replaySpec.EndDate.AsTime().Format(models.JobDatetimeLayout), replaySpec.CreatedAt.AsTime().Format(time.RFC3339),
			replaySpec.State})
	}

	table.Render()
}
