package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/odpf/optimus/meta"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestJobAdapter(t *testing.T) {
	projectSpec := models.ProjectSpec{
		Name: "humara-projectSpec",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}

	namespaceSpec := models.NamespaceSpec{
		Name: "humara-namespaceSpec",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
		ProjectSpec: projectSpec,
	}

	execUnit := new(mock.TaskPlugin)
	hookUnit := new(mock.HookPlugin)

	jobSpecs := []models.JobSpec{
		{
			Name:    "job-1",
			Owner:   "mee@mee",
			Version: 100,
			Labels:  map[string]string{"l1": "lv1"},
			Behavior: models.JobSpecBehavior{
				CatchUp:       true,
				DependsOnPast: false,
			},
			Schedule: models.JobSpecSchedule{
				StartDate: time.Date(2000, 11, 11, 0, 0, 0, 0, time.UTC),
				Interval:  "* * * * *",
			},
			Task: models.JobSpecTask{
				Unit: execUnit,
				Config: models.JobSpecConfigs{
					{
						Name:  "do",
						Value: "this",
					},
				},
				Priority: 2000,
				Window: models.JobSpecTaskWindow{
					Size:       time.Hour,
					Offset:     0,
					TruncateTo: "d",
				},
			},
			Assets: *models.JobAssets{}.New(
				[]models.JobSpecAsset{
					{
						Name:  "query.sql",
						Value: "select * from 1",
					},
				}),
			Dependencies: map[string]models.JobSpecDependency{"job-2": {
				Project: &models.ProjectSpec{
					Name: "some_other_project",
				},
				Job: &models.JobSpec{
					Name: "job-2",
				},
				Type: models.JobSpecDependencyTypeInter,
			}},
			Hooks: []models.JobSpecHook{
				{
					Config: models.JobSpecConfigs{
						{
							Name:  "SAMPLE_CONFIG",
							Value: "200",
						},
						{
							Name:  "PRODUCER_CONFIG_BOOTSTRAP_SERVERS",
							Value: `{{.GLOBAL__transporterKafkaBroker}}`,
						},
					},
					Unit: hookUnit,
				},
			},
		},
	}

	execUnit.On("GetTaskSchema", context.Background(), models.GetTaskSchemaRequest{}).Return(models.GetTaskSchemaResponse{
		Name:        "bq2bq",
		Image:       "image",
		Description: "description",
	}, nil)
	execUnit.On("GenerateDestination", context.TODO(), models.GenerateDestinationRequest{
		Config: models.TaskPluginConfigs{}.FromJobSpec(jobSpecs[0].Task.Config),
		Assets: models.TaskPluginAssets{}.FromJobSpec(jobSpecs[0].Assets),
	}).Return(models.GenerateDestinationResponse{Destination: "destination_table"}, nil)

	hookUnit.On("GetHookSchema", context.Background(), models.GetHookSchemaRequest{}).Return(models.GetHookSchemaResponse{
		Name:        "transporter",
		Description: "h_description",
		Image:       "h_image",
		DependsOn:   []string{"some_value"},
		Type:        models.HookTypePost,
	}, nil)

	t.Run("should build JobMetadata from JobSpec without any error", func(t *testing.T) {
		jobSpec1 := jobSpecs[0]
		expectedResourceMetadata := &models.JobMetadata{
			Urn:         "humara-projectSpec::job/job-1",
			Name:        "job-1",
			Namespace:   namespaceSpec.Name,
			Tenant:      "humara-projectSpec",
			Version:     100,
			Description: "",
			Labels:      meta.CompileSpecLabels(jobSpec1),
			Owner:       "mee@mee",
			Task: models.JobTaskMetadata{
				Name:        "bq2bq",
				Image:       "image",
				Description: "description",
				Destination: "destination_table",
				Config: []models.JobSpecConfigItem{{
					Name:  "do",
					Value: "this",
				}},
				Window:   jobSpec1.Task.Window,
				Priority: 2000,
			},
			Schedule: jobSpec1.Schedule,
			Behavior: jobSpec1.Behavior,
			Dependencies: []models.JobDependencyMetadata{{
				Tenant: "some_other_project",
				Job:    "job-2",
				Type:   models.JobSpecDependencyTypeInter.String(),
			}},
			Hooks: []models.JobHookMetadata{{
				Name:        "transporter",
				Image:       "h_image",
				Description: "h_description",
				Config: []models.JobSpecConfigItem{{
					Name:  "SAMPLE_CONFIG",
					Value: "200",
				}, {
					Name:  "PRODUCER_CONFIG_BOOTSTRAP_SERVERS",
					Value: `{{.GLOBAL__transporterKafkaBroker}}`,
				},
				},
				Type:      models.HookTypePost,
				DependsOn: []string{"some_value"},
			}},
		}
		resourceMetadata, err := meta.JobAdapter{}.FromJobSpec(namespaceSpec, jobSpec1)
		assert.Nil(t, err)
		assert.Equal(t, expectedResourceMetadata, resourceMetadata)

		_, err = meta.JobAdapter{}.CompileKey(jobSpec1.Name)
		assert.Nil(t, err)

		_, err = meta.JobAdapter{}.CompileMessage(resourceMetadata)
		assert.Nil(t, err)
	})
}
