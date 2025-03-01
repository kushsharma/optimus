// +build !unit_test

package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/odpf/optimus/mock"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
	testMock "github.com/stretchr/testify/mock"
	"gorm.io/gorm"
)

func TestResourceSpecRepository(t *testing.T) {
	ctx := context.Background()
	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: "t-optimus-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	// prepare mocked datastore
	dsTypeTableAdapter := new(mock.DatastoreTypeAdapter)

	dsTypeTableController := new(mock.DatastoreTypeController)
	dsTypeTableController.On("Adapter").Return(dsTypeTableAdapter)

	dsController := map[models.ResourceType]models.DatastoreTypeController{
		models.ResourceTypeTable: dsTypeTableController,
	}
	datastorer := new(mock.Datastorer)
	datastorer.On("Types").Return(dsController)
	datastorer.On("Name").Return("DS")

	DBSetup := func() *gorm.DB {
		dbURL, ok := os.LookupEnv("TEST_OPTIMUS_DB_URL")
		if !ok {
			panic("unable to find TEST_OPTIMUS_DB_URL env var")
		}
		dbConn, err := Connect(dbURL, 1, 1, os.Stdout)
		if err != nil {
			panic(err)
		}
		m, err := NewHTTPFSMigrator(dbURL)
		if err != nil {
			panic(err)
		}
		if err := m.Drop(); err != nil {
			panic(err)
		}
		if err := Migrate(dbURL); err != nil {
			panic(err)
		}

		projRepo := NewProjectRepository(dbConn, hash)
		assert.Nil(t, projRepo.Save(ctx, projectSpec))
		return dbConn
	}
	testConfigs := []models.ResourceSpec{
		{
			ID:        uuid.Must(uuid.NewRandom()),
			Version:   1,
			Name:      "proj.datas.test",
			Type:      models.ResourceTypeTable,
			Datastore: datastorer,
			Spec:      nil,
			Assets: map[string]string{
				"query.sql": "select * from 1",
			},
			URN: "datastore://proj:datas.test",
		},
		{
			Name: "",
		},
		{
			ID:        uuid.Must(uuid.NewRandom()),
			Version:   1,
			Name:      "proj.ttt.test2",
			Type:      models.ResourceTypeTable,
			Datastore: datastorer,
			Spec:      nil,
			URN:       "datastore://proj:ttt.test2",
		},
	}
	testConfigWithoutAssets := []models.ResourceSpec{
		{
			ID:        testConfigs[0].ID,
			Version:   1,
			Name:      "proj.datas.test",
			Type:      models.ResourceTypeTable,
			Datastore: datastorer,
			Spec:      nil,
			URN:       "datastore://proj:datas.test",
		},
	}

	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}

	namespaceSpec2 := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "dev-team-2",
		ProjectSpec: projectSpec,
	}

	dsTypeTableAdapter.On("ToYaml", testConfigWithoutAssets[0]).Return([]byte("some binary data"), nil)
	dsTypeTableAdapter.On("FromYaml", []byte("some binary data")).Return(testConfigWithoutAssets[0], nil)
	dsTypeTableAdapter.On("ToYaml", testConfigs[2]).Return([]byte("some binary data X"), nil)
	dsTypeTableAdapter.On("FromYaml", []byte("some binary data X")).Return(testConfigs[2], nil)

	t.Run("Insert", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.ResourceSpec{}
		testModels = append(testModels, testConfigs...)

		dsTypeTableController.On("GenerateURN", testMock.Anything).Return(testModels[0].URN, nil).Once()
		dsTypeTableController.On("GenerateURN", testMock.Anything).Return(testModels[1].URN, nil).Once()

		projectResourceSpecRepo := NewProjectResourceSpecRepository(db, projectSpec, datastorer)
		repo := NewResourceSpecRepository(db, namespaceSpec, datastorer, projectResourceSpecRepo)

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		err = repo.Insert(ctx, testModels[1])
		assert.NotNil(t, err)

		checkModel, err := repo.GetByID(ctx, testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, "proj.datas.test", checkModel.Name)
	})

	t.Run("Upsert", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[0]
			testModelB := testConfigs[2]

			projectResourceSpecRepo := NewProjectResourceSpecRepository(db, projectSpec, datastorer)
			repo := NewResourceSpecRepository(db, namespaceSpec, datastorer, projectResourceSpecRepo)

			dsTypeTableController.On("GenerateURN", testMock.Anything).Return(testModelA.URN, nil).Once()

			//try for create
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "proj.datas.test", checkModel.Name)

			//try for create
			err = repo.Save(ctx, testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(ctx, testModelB.ID)
			assert.Nil(t, err)
			assert.Equal(t, "proj.ttt.test2", checkModel.Name)
			assert.Equal(t, "table", checkModel.Type.String())
		})
		t.Run("insert same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[2]

			projectResourceSpecRepo := NewProjectResourceSpecRepository(db, projectSpec, datastorer)
			repo := NewResourceSpecRepository(db, namespaceSpec, datastorer, projectResourceSpecRepo)

			dsTypeTableController.On("GenerateURN", testMock.Anything).Return(testModelA.URN, nil).Twice()

			//try for create
			err := repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "proj.ttt.test2", checkModel.Name)

			//try for update
			testModelA.Version = 6
			dsTypeTableAdapter.On("ToYaml", testModelA).Return([]byte("some binary data testModelA"), nil)
			dsTypeTableAdapter.On("FromYaml", []byte("some binary data testModelA")).Return(testModelA, nil)

			err = repo.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(ctx, testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, 6, checkModel.Version)
		})
		t.Run("upsert without ID should auto generate it", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			resourceSpecWithEmptyUUID := testConfigWithoutAssets[0]
			resourceSpecWithEmptyUUID.ID = uuid.Nil

			dsTypeTableAdapterLocal := new(mock.DatastoreTypeAdapter)

			dsTypeTableControllerLocal := new(mock.DatastoreTypeController)
			dsTypeTableControllerLocal.On("Adapter").Return(dsTypeTableAdapterLocal)

			dsControllerLocal := map[models.ResourceType]models.DatastoreTypeController{
				models.ResourceTypeTable: dsTypeTableControllerLocal,
			}
			datastorerLocal := new(mock.Datastorer)
			datastorerLocal.On("Types").Return(dsControllerLocal)
			datastorerLocal.On("Name").Return("DS")
			resourceSpecWithEmptyUUID.Datastore = datastorerLocal

			dsTypeTableAdapterLocal.On("ToYaml", resourceSpecWithEmptyUUID).Return([]byte("some binary data emptyUUID nil"), nil)
			dsTypeTableAdapterLocal.On("FromYaml", []byte("some binary data emptyUUID nil")).Return(resourceSpecWithEmptyUUID, nil)

			projectResourceSpecRepo := NewProjectResourceSpecRepository(db, projectSpec, datastorerLocal)
			repo := NewResourceSpecRepository(db, namespaceSpec, datastorerLocal, projectResourceSpecRepo)

			dsTypeTableControllerLocal.On("GenerateURN", testMock.Anything).Return(resourceSpecWithEmptyUUID.URN, nil).Once()

			// try for create
			err := repo.Save(ctx, resourceSpecWithEmptyUUID)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(ctx, resourceSpecWithEmptyUUID.Name)
			assert.Nil(t, err)
			assert.Equal(t, "proj.datas.test", checkModel.Name)
		})
		t.Run("should fail if resource is already registered for a project with different namespace", func(t *testing.T) {
			db := DBSetup()
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			testModelA := testConfigs[2]

			projectResourceSpecRepo := NewProjectResourceSpecRepository(db, projectSpec, datastorer)
			resourceSpecNamespace1 := NewResourceSpecRepository(db, namespaceSpec, datastorer, projectResourceSpecRepo)
			resourceSpecNamespace2 := NewResourceSpecRepository(db, namespaceSpec2, datastorer, projectResourceSpecRepo)

			dsTypeTableController.On("GenerateURN", testMock.Anything).Return(testModelA.URN, nil).Twice()

			//try for create
			err := resourceSpecNamespace1.Save(ctx, testModelA)
			assert.Nil(t, err)

			checkModel, checkNamespace, err := projectResourceSpecRepo.GetByName(ctx, testModelA.Name)
			assert.Nil(t, err)
			assert.Equal(t, "proj.ttt.test2", checkModel.Name)
			assert.Equal(t, namespaceSpec.ID, checkNamespace.ID)
			assert.Equal(t, namespaceSpec.ProjectSpec.ID, checkNamespace.ProjectSpec.ID)

			// try to create same resource with second client and it should fail.
			err = resourceSpecNamespace2.Save(ctx, testModelA)
			assert.NotNil(t, err)
			assert.Equal(t, "resource proj.ttt.test2 already exists for the project t-optimus-project", err.Error())
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.ResourceSpec{}
		testModels = append(testModels, testConfigs...)

		projectResourceSpecRepo := NewProjectResourceSpecRepository(db, projectSpec, datastorer)
		repo := NewResourceSpecRepository(db, namespaceSpec, datastorer, projectResourceSpecRepo)

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "proj.datas.test", checkModel.Name)
	})
}

func TestProjectResourceSpecRepository(t *testing.T) {
	ctx := context.Background()
	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: "t-optimus-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	// prepare mocked datastore
	dsTypeTableAdapter := new(mock.DatastoreTypeAdapter)

	dsTypeTableController := new(mock.DatastoreTypeController)
	dsTypeTableController.On("Adapter").Return(dsTypeTableAdapter)

	dsController := map[models.ResourceType]models.DatastoreTypeController{
		models.ResourceTypeTable: dsTypeTableController,
	}
	datastorer := new(mock.Datastorer)
	datastorer.On("Types").Return(dsController)
	datastorer.On("Name").Return("DS")

	DBSetup := func() *gorm.DB {
		dbURL, ok := os.LookupEnv("TEST_OPTIMUS_DB_URL")
		if !ok {
			panic("unable to find TEST_OPTIMUS_DB_URL env var")
		}
		dbConn, err := Connect(dbURL, 1, 1, os.Stdout)
		if err != nil {
			panic(err)
		}
		m, err := NewHTTPFSMigrator(dbURL)
		if err != nil {
			panic(err)
		}
		if err := m.Drop(); err != nil {
			panic(err)
		}
		if err := Migrate(dbURL); err != nil {
			panic(err)
		}

		projRepo := NewProjectRepository(dbConn, hash)
		assert.Nil(t, projRepo.Save(ctx, projectSpec))
		return dbConn
	}
	testConfigs := []models.ResourceSpec{
		{
			ID:        uuid.Must(uuid.NewRandom()),
			Version:   1,
			Name:      "proj.datas.test",
			Type:      models.ResourceTypeTable,
			Datastore: datastorer,
			URN:       "datastore:proj.datas.test",
			Spec:      nil,
			Assets: map[string]string{
				"query.sql": "select * from 1",
			},
		},
		{
			Name: "",
		},
		{
			ID:        uuid.Must(uuid.NewRandom()),
			Version:   1,
			Name:      "proj.ttt.test2",
			Type:      models.ResourceTypeTable,
			URN:       "datastore:proj.ttt.test2",
			Datastore: datastorer,
			Spec:      nil,
		},
	}
	testConfigWithoutAssets := []models.ResourceSpec{
		{
			ID:        testConfigs[0].ID,
			Version:   1,
			Name:      "proj.datas.test",
			Type:      models.ResourceTypeTable,
			Datastore: datastorer,
			URN:       "datastore:proj.datas.test",
			Spec:      nil,
		},
	}

	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}

	dsTypeTableAdapter.On("ToYaml", testConfigWithoutAssets[0]).Return([]byte("some binary data"), nil)
	dsTypeTableAdapter.On("FromYaml", []byte("some binary data")).Return(testConfigWithoutAssets[0], nil)
	dsTypeTableAdapter.On("ToYaml", testConfigs[2]).Return([]byte("some binary data X"), nil)
	dsTypeTableAdapter.On("FromYaml", []byte("some binary data X")).Return(testConfigs[2], nil)

	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.ResourceSpec{}
		testModels = append(testModels, testConfigs...)

		projectResourceSpecRepo := NewProjectResourceSpecRepository(db, projectSpec, datastorer)
		repo := NewResourceSpecRepository(db, namespaceSpec, datastorer, projectResourceSpecRepo)

		dsTypeTableController.On("GenerateURN", testMock.Anything).Return(testModels[0].URN, nil).Once()

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		// validate at project level
		checkModel, checkClient, err := projectResourceSpecRepo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "proj.datas.test", checkModel.Name)
		assert.Equal(t, namespaceSpec.Name, checkClient.Name)

		// validate at client level
		checkModel, err = repo.GetByName(ctx, testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "proj.datas.test", checkModel.Name)
	})

	t.Run("GetByURN", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		var testModels []models.ResourceSpec
		testModels = append(testModels, testConfigs...)

		projectResourceSpecRepo := NewProjectResourceSpecRepository(db, projectSpec, datastorer)
		repo := NewResourceSpecRepository(db, namespaceSpec, datastorer, projectResourceSpecRepo)

		dsTypeTableController.On("GenerateURN", testMock.Anything).Return(testModels[0].URN, nil).Once()

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByURN(ctx, testModels[0].URN)
		assert.Nil(t, err)
		assert.Equal(t, "proj.datas.test", checkModel.Name)
	})

	t.Run("GetAll", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()
		testModels := []models.ResourceSpec{}
		testModels = append(testModels, testConfigs...)

		projectResourceSpecRepo := NewProjectResourceSpecRepository(db, projectSpec, datastorer)
		repo := NewResourceSpecRepository(db, namespaceSpec, datastorer, projectResourceSpecRepo)

		dsTypeTableController.On("GenerateURN", testMock.Anything).Return(testModels[0].URN, nil).Once()

		err := repo.Insert(ctx, testModels[0])
		assert.Nil(t, err)

		// validate at project level
		checkModels, err := projectResourceSpecRepo.GetAll(ctx)
		assert.Nil(t, err)
		assert.Equal(t, "proj.datas.test", checkModels[0].Name)
		assert.Equal(t, 1, len(checkModels))

		// validate at client level
		checkModels, err = repo.GetAll(ctx)
		assert.Nil(t, err)
		assert.Equal(t, "proj.datas.test", checkModels[0].Name)
		assert.Equal(t, 1, len(checkModels))
	})
}
