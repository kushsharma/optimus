package postgres

import (
	"context"
	"encoding/json"
	"time"

	"gorm.io/gorm"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
	"gorm.io/datatypes"
)

// Job are inputs from user to create a job
// postgres representation of the job
type Job struct {
	ID           uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`
	Version      int
	Name         string `gorm:"not null" json:"name"`
	Owner        string
	Description  string
	Labels       datatypes.JSON
	StartDate    time.Time
	EndDate      *time.Time
	Interval     string
	Destination  string
	Dependencies datatypes.JSON
	Behavior     datatypes.JSON

	ProjectID uuid.UUID
	Project   Project `gorm:"foreignKey:ProjectID"`

	NamespaceID uuid.UUID
	Namespace   Namespace `gorm:"foreignKey:NamespaceID"`

	TaskName         string
	TaskConfig       datatypes.JSON
	WindowSize       *int64 //duration in nanos
	WindowOffset     *int64
	WindowTruncateTo *string

	Assets datatypes.JSON
	Hooks  datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt gorm.DeletedAt
}

type JobBehavior struct {
	DependsOnPast bool
	CatchUp       bool
	Retry         JobBehaviorRetry
	Notify        []JobBehaviorNotifier
}

type JobBehaviorRetry struct {
	Count              int
	Delay              int64
	ExponentialBackoff bool
}

type JobBehaviorNotifier struct {
	On       string
	Config   map[string]string
	Channels []string
}

type JobAsset struct {
	Name  string
	Value string
}

func (a JobAsset) ToSpec() models.JobSpecAsset {
	return models.JobSpecAsset{
		Name:  a.Name,
		Value: a.Value,
	}
}

func (a JobAsset) FromSpec(spec models.JobSpecAsset) JobAsset {
	return JobAsset{
		Name:  spec.Name,
		Value: spec.Value,
	}
}

type JobHook struct {
	Name   string
	Config datatypes.JSON
}

// ToSpec converts the postgres' JobHook representation to the optimus' models.JobSpecHook
func (a JobHook) ToSpec(pluginRepo models.PluginRepository) (models.JobSpecHook, error) {
	hookUnit, err := pluginRepo.GetByName(a.Name)
	if err != nil {
		return models.JobSpecHook{}, errors.Wrap(err, "spec reading error")
	}

	conf := models.JobSpecConfigs{}
	if err := json.Unmarshal(a.Config, &conf); err != nil {
		return models.JobSpecHook{}, err
	}

	return models.JobSpecHook{
		Config: conf,
		Unit:   hookUnit,
	}, nil
}

func (a JobHook) FromSpec(spec models.JobSpecHook) (JobHook, error) {
	configJSON, err := json.Marshal(spec.Config)
	if err != nil {
		return JobHook{}, err
	}
	return JobHook{
		Name:   spec.Unit.Info().Name,
		Config: configJSON,
	}, nil
}

type JobSpecAdapter struct {
	pluginRepo models.PluginRepository
}

func NewAdapter(pluginRepo models.PluginRepository) *JobSpecAdapter {
	return &JobSpecAdapter{
		pluginRepo: pluginRepo,
	}
}

// ToSpec converts the postgres' Job representation to the optimus' JobSpec
func (adapt JobSpecAdapter) ToSpec(conf Job) (models.JobSpec, error) {
	labels := map[string]string{}
	if conf.Labels != nil {
		if err := json.Unmarshal(conf.Labels, &labels); err != nil {
			return models.JobSpec{}, err
		}
	}

	behavior := JobBehavior{}
	if conf.Behavior != nil {
		if err := json.Unmarshal(conf.Behavior, &behavior); err != nil {
			return models.JobSpec{}, err
		}
	}

	// prep dirty dependencies
	dependencies := map[string]models.JobSpecDependency{}
	if err := json.Unmarshal(conf.Dependencies, &dependencies); err != nil {
		return models.JobSpec{}, err
	}

	// prep task conf
	taskConf := models.JobSpecConfigs{}
	if err := json.Unmarshal(conf.TaskConfig, &taskConf); err != nil {
		return models.JobSpec{}, err
	}

	//prep assets
	jobAssets := []models.JobSpecAsset{}
	assetsRaw := []JobAsset{}
	if err := json.Unmarshal(conf.Assets, &assetsRaw); err != nil {
		return models.JobSpec{}, err
	}
	for _, asset := range assetsRaw {
		jobAssets = append(jobAssets, asset.ToSpec())
	}

	//prep hooks
	jobHooks := []models.JobSpecHook{}
	hooksRaw := []JobHook{}
	if err := json.Unmarshal(conf.Hooks, &hooksRaw); err != nil {
		return models.JobSpec{}, err
	}
	for _, hook := range hooksRaw {
		hookSpec, err := hook.ToSpec(adapt.pluginRepo)
		if err != nil {
			return models.JobSpec{}, err
		}
		jobHooks = append(jobHooks, hookSpec)
	}

	execUnit, err := adapt.pluginRepo.GetByName(conf.TaskName)
	if err != nil {
		return models.JobSpec{}, errors.Wrap(err, "spec reading error")
	}

	var notifiers []models.JobSpecNotifier
	for _, notify := range behavior.Notify {
		notifiers = append(notifiers, models.JobSpecNotifier{
			On:       models.JobEventType(notify.On),
			Config:   notify.Config,
			Channels: notify.Channels,
		})
	}

	job := models.JobSpec{
		ID:          conf.ID,
		Version:     conf.Version,
		Name:        conf.Name,
		Owner:       conf.Owner,
		Description: conf.Description,
		Labels:      labels,
		Schedule: models.JobSpecSchedule{
			StartDate: conf.StartDate,
			EndDate:   conf.EndDate,
			Interval:  conf.Interval,
		},
		Behavior: models.JobSpecBehavior{
			DependsOnPast: behavior.DependsOnPast,
			CatchUp:       behavior.CatchUp,
			Retry: models.JobSpecBehaviorRetry{
				Count:              behavior.Retry.Count,
				Delay:              time.Duration(behavior.Retry.Delay),
				ExponentialBackoff: behavior.Retry.ExponentialBackoff,
			},
			Notify: notifiers,
		},
		Task: models.JobSpecTask{
			Unit:   execUnit,
			Config: taskConf,
			Window: models.JobSpecTaskWindow{
				Size:       time.Duration(*conf.WindowSize),
				Offset:     time.Duration(*conf.WindowOffset),
				TruncateTo: *conf.WindowTruncateTo,
			},
		},
		Assets:       *(models.JobAssets{}).New(jobAssets),
		Dependencies: dependencies,
		Hooks:        jobHooks,
	}
	return job, nil
}

// FromJobSpec converts the optimus representation of JobSpec to postgres' Job
func (adapt JobSpecAdapter) FromJobSpec(spec models.JobSpec) (Job, error) {
	if spec.Task.Unit == nil {
		return Job{}, errors.New("task unit cannot be empty")
	}

	labelsJSON, err := json.Marshal(spec.Labels)
	if err != nil {
		return Job{}, err
	}

	var notifiers []JobBehaviorNotifier
	for _, notify := range spec.Behavior.Notify {
		notifiers = append(notifiers, JobBehaviorNotifier{
			On:       string(notify.On),
			Config:   notify.Config,
			Channels: notify.Channels,
		})
	}

	behaviorJSON, err := json.Marshal(JobBehavior{
		DependsOnPast: spec.Behavior.DependsOnPast,
		CatchUp:       spec.Behavior.CatchUp,
		Retry: JobBehaviorRetry{
			Count:              spec.Behavior.Retry.Count,
			Delay:              spec.Behavior.Retry.Delay.Nanoseconds(),
			ExponentialBackoff: spec.Behavior.Retry.ExponentialBackoff,
		},
		Notify: notifiers,
	})
	if err != nil {
		return Job{}, err
	}

	// prep dependencies, make them dirty first(remove job and project)
	for idx, dep := range spec.Dependencies {
		dep.Project = nil
		dep.Job = nil
		spec.Dependencies[idx] = dep
	}
	dependenciesJSON, err := json.Marshal(spec.Dependencies)
	if err != nil {
		return Job{}, err
	}

	// prep task config
	taskConfigJSON, err := json.Marshal(spec.Task.Config)
	if err != nil {
		return Job{}, err
	}

	// prep assets
	assets := []JobAsset{}
	for _, jobAsset := range spec.Assets.GetAll() {
		assets = append(assets, JobAsset{}.FromSpec(jobAsset))
	}
	assetsJSON, err := json.Marshal(assets)
	if err != nil {
		return Job{}, err
	}

	hooks := []JobHook{}
	for _, hook := range spec.Hooks {
		h, err := JobHook{}.FromSpec(hook)
		if err != nil {
			return Job{}, err
		}
		hooks = append(hooks, h)
	}
	hooksJSON, err := json.Marshal(hooks)
	if err != nil {
		return Job{}, err
	}

	wsize := spec.Task.Window.Size.Nanoseconds()
	woffset := spec.Task.Window.Offset.Nanoseconds()

	var jobDestination string
	if spec.Task.Unit.DependencyMod != nil {
		jobDestinationResponse, err := spec.Task.Unit.DependencyMod.GenerateDestination(context.TODO(), models.GenerateDestinationRequest{
			Config: models.PluginConfigs{}.FromJobSpec(spec.Task.Config),
			Assets: models.PluginAssets{}.FromJobSpec(spec.Assets),
		})
		if err != nil {
			return Job{}, err
		}
		jobDestination = jobDestinationResponse.URN()
	}

	return Job{
		ID:               spec.ID,
		Version:          spec.Version,
		Name:             spec.Name,
		Owner:            spec.Owner,
		Description:      spec.Description,
		Labels:           labelsJSON,
		StartDate:        spec.Schedule.StartDate,
		EndDate:          spec.Schedule.EndDate,
		Interval:         spec.Schedule.Interval,
		Behavior:         behaviorJSON,
		Destination:      jobDestination,
		Dependencies:     dependenciesJSON,
		TaskName:         spec.Task.Unit.Info().Name,
		TaskConfig:       taskConfigJSON,
		WindowSize:       &wsize,
		WindowOffset:     &woffset,
		WindowTruncateTo: &spec.Task.Window.TruncateTo,
		Assets:           assetsJSON,
		Hooks:            hooksJSON,
	}, nil
}

func (adapt JobSpecAdapter) FromSpecWithNamespace(spec models.JobSpec, namespace models.NamespaceSpec) (Job, error) {
	adaptJob, err := adapt.FromJobSpec(spec)
	if err != nil {
		return adaptJob, err
	}

	// namespace
	adaptNamespace, err := Namespace{}.FromSpecWithProject(namespace, namespace.ProjectSpec)
	if err != nil {
		return adaptJob, err
	}
	adaptJob.NamespaceID = adaptNamespace.ID
	adaptJob.Namespace = adaptNamespace

	// project
	adaptProject, err := Project{}.FromSpec(namespace.ProjectSpec)
	if err != nil {
		return adaptJob, err
	}
	adaptJob.ProjectID = adaptProject.ID
	adaptJob.Project = adaptProject

	return adaptJob, nil
}

type JobRun struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

	JobID uuid.UUID

	// job spec for which this run was created, spec should contain a valid
	// uuid if it belongs to a saved job and not an adhoc job
	Spec datatypes.JSON `gorm:"column:specification;"`

	NamespaceID uuid.UUID
	Namespace   Namespace `gorm:"foreignKey:NamespaceID"`

	Trigger     string
	Status      string
	ScheduledAt time.Time

	Instances []Instance

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

func (adapt JobSpecAdapter) FromJobRun(jr models.JobRun, nsSpec models.NamespaceSpec) (JobRun, error) {
	adaptedJobSpec, err := adapt.FromJobSpec(jr.Spec)
	if err != nil {
		return JobRun{}, err
	}
	specBytes, err := json.Marshal(adaptedJobSpec)
	if err != nil {
		return JobRun{}, err
	}

	// namespace
	adaptNamespace, err := Namespace{}.FromSpecWithProject(nsSpec, nsSpec.ProjectSpec)
	if err != nil {
		return JobRun{}, err
	}

	var instances []Instance
	for _, instanceSpec := range jr.Instances {
		instance, err := Instance{}.FromSpec(instanceSpec, jr.ID)
		if err != nil {
			return JobRun{}, err
		}
		instances = append(instances, instance)
	}

	return JobRun{
		ID:    jr.ID,
		JobID: jr.Spec.ID,
		Spec:  specBytes,

		NamespaceID: adaptNamespace.ID,
		Namespace:   adaptNamespace,

		Trigger:     jr.Trigger.String(),
		Status:      jr.Status.String(),
		ScheduledAt: jr.ScheduledAt,

		Instances: instances,
	}, nil
}

func (adapt JobSpecAdapter) ToJobRun(jr JobRun) (models.JobRun, models.NamespaceSpec, error) {
	adaptedSpec := Job{}
	if err := json.Unmarshal(jr.Spec, &adaptedSpec); err != nil {
		return models.JobRun{}, models.NamespaceSpec{}, err
	}

	jobSpec, err := adapt.ToSpec(adaptedSpec)
	if err != nil {
		return models.JobRun{}, models.NamespaceSpec{}, err
	}

	var instanceSpecs []models.InstanceSpec
	for _, instance := range jr.Instances {
		is, err := instance.ToSpec()
		if err != nil {
			return models.JobRun{}, models.NamespaceSpec{}, err
		}
		instanceSpecs = append(instanceSpecs, is)
	}

	var adaptProject models.ProjectSpec
	var adaptNamespace models.NamespaceSpec

	if jr.Namespace.Name != "" {
		adaptProject, err = jr.Namespace.Project.ToSpec()
		if err != nil {
			return models.JobRun{}, models.NamespaceSpec{}, err
		}

		// namespace
		adaptNamespace, err = jr.Namespace.ToSpec(adaptProject)
		if err != nil {
			return models.JobRun{}, models.NamespaceSpec{}, err
		}
	}

	return models.JobRun{
		ID:          jr.ID,
		Spec:        jobSpec,
		Trigger:     models.JobRunTrigger(jr.Trigger),
		Status:      models.JobRunState(jr.Status),
		ScheduledAt: jr.ScheduledAt,
		Instances:   instanceSpecs,
	}, adaptNamespace, nil
}
