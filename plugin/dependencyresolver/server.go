package dependencyresolver

import (
	"context"

	"github.com/odpf/optimus/plugin/cli"

	"github.com/odpf/optimus/models"

	pbp "github.com/odpf/optimus/api/proto/odpf/optimus/plugins"
)

// GRPCServer will be used by plugins this is working as proto adapter
type GRPCServer struct {
	// This is the real implementation coming from plugin
	Impl models.DependencyResolverMod

	projectSpecAdapter ProjectSpecAdapter
	pbp.UnimplementedTaskServer
}

func (s *GRPCServer) GenerateDestination(ctx context.Context, req *pbp.GenerateDestination_Request) (*pbp.GenerateDestination_Response, error) {
	resp, err := s.Impl.GenerateDestination(ctx, models.GenerateDestinationRequest{
		Config:        cli.AdaptConfigsFromProto(req.Config),
		Assets:        cli.AdaptAssetsFromProto(req.Assets),
		Project:       s.projectSpecAdapter.FromProjectProtoWithSecrets(req.Project),
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &pbp.GenerateDestination_Response{Destination: resp.Destination}, nil
}

func (s *GRPCServer) GenerateDependencies(ctx context.Context, req *pbp.GenerateDependencies_Request) (*pbp.GenerateDependencies_Response, error) {
	resp, err := s.Impl.GenerateDependencies(ctx, models.GenerateDependenciesRequest{
		Config:        cli.AdaptConfigsFromProto(req.Config),
		Assets:        cli.AdaptAssetsFromProto(req.Assets),
		Project:       s.projectSpecAdapter.FromProjectProtoWithSecrets(req.Project),
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &pbp.GenerateDependencies_Response{Dependencies: resp.Dependencies}, nil
}
