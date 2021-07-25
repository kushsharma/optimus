package base

import (
	"context"

	"github.com/odpf/optimus/models"

	pbp "github.com/odpf/optimus/api/proto/odpf/optimus/plugins"
)

// GRPCServer will be used by plugins, this is working as proto adapter
type GRPCServer struct {
	// This is the real implementation coming from plugin
	Impl models.BasePlugin

	pbp.UnimplementedBaseServer
}

func (s *GRPCServer) PluginInfo(ctx context.Context, req *pbp.PluginInfo_Request) (*pbp.PluginInfo_Response, error) {
	n, err := s.Impl.PluginInfo()
	if err != nil {
		return nil, err
	}

	ptype := pbp.PluginType_PluginType_HOOK
	switch n.PluginType {
	case models.PluginTypeTask:
		ptype = pbp.PluginType_PluginType_TASK
	}

	htype := pbp.HookType_HookType_UNKNOWN
	switch n.HookType {
	case models.HookTypePre:
		htype = pbp.HookType_HookType_PRE
	case models.HookTypePost:
		htype = pbp.HookType_HookType_POST
	case models.HookTypeFail:
		htype = pbp.HookType_HookType_FAIL
	}
	return &pbp.PluginInfo_Response{
		Name:          n.Name,
		PluginType:    ptype,
		PluginVersion: n.PluginVersion,
		ApiVersion:    n.APIVersion,
		Description:   n.Description,
		Image:         n.Image,
		DependsOn:     n.DependsOn,
		HookType:      htype,
		SecretPath:    n.SecretPath,
	}, nil
}
