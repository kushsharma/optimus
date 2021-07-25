package models

import (
	"context"
	"strings"
)

const (
	HookTypePre  HookType = "pre"
	HookTypePost HookType = "post"
	HookTypeFail HookType = "fail"
)

type HookType string

func (ht HookType) String() string {
	return string(ht)
}

// HookPlugin needs to be implemented to register a hook
type HookPlugin interface {
	GetHookSchema(context.Context, GetHookSchemaRequest) (GetHookSchemaResponse, error)

	// GetHookQuestions list down all the cli inputs required to generate spec files
	// name used for question will be directly mapped to DefaultHookConfig() parameters
	GetHookQuestions(context.Context, GetHookQuestionsRequest) (GetHookQuestionsResponse, error)
	ValidateHookQuestion(context.Context, ValidateHookQuestionRequest) (ValidateHookQuestionResponse, error)

	// DefaultHookConfig will be passed down to execution unit as env vars
	// they will be generated based on results of AskQuestions
	// if DryRun is true in PluginOptions, should not throw error for missing inputs
	// includes parent task configs
	DefaultHookConfig(context.Context, DefaultHookConfigRequest) (DefaultHookConfigResponse, error)

	// DefaultHookAssets will be passed down to execution unit as files
	// if DryRun is true in PluginOptions, should not throw error for missing inputs
	DefaultHookAssets(context.Context, DefaultHookAssetsRequest) (DefaultHookAssetsResponse, error)
}

type GetHookSchemaRequest struct{}

type GetHookSchemaResponse struct {
	Name        string
	Description string
	Image       string

	// DependsOn returns list of hooks this should be executed after
	DependsOn []string

	// Type provides the place of execution, could be before the transformation
	// after the transformation, etc
	Type HookType

	// SecretPath will be mounted inside the container as volume
	// e.g. /opt/secret/auth.json
	// here auth.json should be a key in kube secret which gets
	// translated to a file mounted in provided path
	SecretPath string
}

type GetHookQuestionsRequest struct {
	JobName string
	PluginOptions
}

type GetHookQuestionsResponse struct {
	Questions PluginQuestions
}

type ValidateHookQuestionRequest struct {
	PluginOptions

	Answer PluginAnswer
}

type ValidateHookQuestionResponse struct {
	Success bool
	Error   string
}

type HookPluginConfig struct {
	Name  string
	Value string
}

type HookPluginConfigs []HookPluginConfig

func (c HookPluginConfigs) Get(name string) (HookPluginConfig, bool) {
	for _, con := range c {
		if strings.ToLower(con.Name) == strings.ToLower(name) {
			return con, true
		}
	}
	return HookPluginConfig{}, false
}

func (c HookPluginConfigs) FromJobSpec(jobSpecConfig JobSpecConfigs) HookPluginConfigs {
	taskPluginConfigs := HookPluginConfigs{}
	for _, c := range jobSpecConfig {
		taskPluginConfigs = append(taskPluginConfigs, HookPluginConfig{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return taskPluginConfigs
}

func (c HookPluginConfigs) ToJobSpec() JobSpecConfigs {
	jsConfigs := JobSpecConfigs{}
	for _, c := range c {
		jsConfigs = append(jsConfigs, JobSpecConfigItem{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return jsConfigs
}

type DefaultHookConfigRequest struct {
	PluginOptions

	Answers PluginAnswers

	// TaskConfig of the parent on which this task belongs to
	TaskConfig PluginConfigs
}

type DefaultHookConfigResponse struct {
	Config HookPluginConfigs
}

type HookPluginAsset struct {
	Name  string
	Value string
}

type HookPluginAssets []HookPluginAsset

func (c HookPluginAssets) Get(name string) (HookPluginAsset, bool) {
	for _, con := range c {
		if strings.ToLower(con.Name) == strings.ToLower(name) {
			return con, true
		}
	}
	return HookPluginAsset{}, false
}

func (c HookPluginAssets) FromJobSpec(jobSpecAssets JobAssets) HookPluginAssets {
	taskPluginAssets := HookPluginAssets{}
	for _, c := range jobSpecAssets.GetAll() {
		taskPluginAssets = append(taskPluginAssets, HookPluginAsset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return taskPluginAssets
}

func (c HookPluginAssets) ToJobSpec() *JobAssets {
	jsAssets := []JobSpecAsset{}
	for _, c := range c {
		jsAssets = append(jsAssets, JobSpecAsset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return JobAssets{}.New(jsAssets)
}

type DefaultHookAssetsRequest struct {
	PluginOptions

	Answers PluginAnswers
	// TaskConfig of the parent on which this task belongs to
	TaskConfig PluginConfigs
}

type DefaultHookAssetsResponse struct {
	Assets HookPluginAssets
}
