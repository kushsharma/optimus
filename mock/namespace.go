package mock

import (
	"context"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/mock"
)

type NamespaceRepository struct {
	mock.Mock
}

func (pr *NamespaceRepository) Save(ctx context.Context, spec models.NamespaceSpec) error {
	return pr.Called(ctx, spec).Error(0)
}

func (pr *NamespaceRepository) GetByName(ctx context.Context, name string) (models.NamespaceSpec, error) {
	args := pr.Called(ctx, name)
	return args.Get(0).(models.NamespaceSpec), args.Error(1)
}

func (pr *NamespaceRepository) GetAll(ctx context.Context) ([]models.NamespaceSpec, error) {
	args := pr.Called(ctx)
	return args.Get(0).([]models.NamespaceSpec), args.Error(1)
}

type NamespaceRepoFactory struct {
	mock.Mock
}

func (fac *NamespaceRepoFactory) New(proj models.ProjectSpec) store.NamespaceRepository {
	args := fac.Called(proj)
	return args.Get(0).(store.NamespaceRepository)
}
