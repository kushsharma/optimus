package dependencyresolver

import (
	"context"

	"github.com/odpf/optimus/plugin/base"

	"github.com/odpf/optimus/plugin/cli"

	pbp "github.com/odpf/optimus/api/proto/odpf/optimus/plugins"
	"github.com/odpf/optimus/models"
)

// GRPCClient will be used by core to talk over grpc with plugins
type GRPCClient struct {
	client             pbp.TaskClient
	projectSpecAdapter ProjectSpecAdapter

	baseClient *base.GRPCClient

	// plugin name
	name string
}

func (m *GRPCClient) PluginInfo() (*models.PluginInfoResponse, error) {
	return m.baseClient.PluginInfo()
}

func (m *GRPCClient) SetName(n string) {
	m.name = n
}

func (m *GRPCClient) GenerateDestination(ctx context.Context, request models.GenerateDestinationRequest) (*models.GenerateDestinationResponse, error) {
	resp, err := m.client.GenerateDestination(ctx, &pbp.GenerateDestination_Request{
		Config:  cli.AdaptConfigsToProto(request.Config),
		Assets:  cli.AdaptAssetsToProto(request.Assets),
		Project: m.projectSpecAdapter.ToProjectProtoWithSecret(request.Project, models.InstanceTypeTask, m.name),
		Options: &pbp.PluginOptions{DryRun: request.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &models.GenerateDestinationResponse{
		Destination: resp.Destination,
	}, nil
}

func (m *GRPCClient) GenerateDependencies(ctx context.Context, request models.GenerateDependenciesRequest) (*models.GenerateDependenciesResponse, error) {
	resp, err := m.client.GenerateDependencies(ctx, &pbp.GenerateDependencies_Request{
		Config:  cli.AdaptConfigsToProto(request.Config),
		Assets:  cli.AdaptAssetsToProto(request.Assets),
		Project: m.projectSpecAdapter.ToProjectProtoWithSecret(request.Project, models.InstanceTypeTask, m.name),
		Options: &pbp.PluginOptions{DryRun: request.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &models.GenerateDependenciesResponse{
		Dependencies: resp.Dependencies,
	}, nil
}
