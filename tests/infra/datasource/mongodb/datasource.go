package mongodb

import (
	"fmt"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/infra/docker_compose"
)

const (
	serviceName  = "mongodb"
	internalPort = 27017
	database     = "connector"
	username     = "admin"
	password     = "password"
)

var defaultMongoDbOptions *api_common.TMongoDbDataSourceOptions = &api_common.TMongoDbDataSourceOptions{
	CountDocsToRead:      3,
	DoParse:              true,
	SkipUnsupportedTypes: true,
}

func deriveDataSourceFromDockerCompose(ed *docker_compose.EndpointDeterminer) (*datasource.DataSource, error) {
	dsi := &api_common.TGenericDataSourceInstance{
		Kind:     api_common.EGenericDataSourceKind_MONGODB,
		Database: database,
		Credentials: &api_common.TGenericCredentials{
			Payload: &api_common.TGenericCredentials_Basic{
				Basic: &api_common.TGenericCredentials_TBasic{
					Username: username,
					Password: password,
				},
			},
		},
		Protocol: api_common.EGenericProtocol_NATIVE,
		UseTls:   false,
		Options: &api_common.TGenericDataSourceInstance_MongodbOptions{
			MongodbOptions: defaultMongoDbOptions,
		},
	}

	var err error
	dsi.Endpoint, err = ed.GetEndpoint(serviceName, internalPort)

	if err != nil {
		return nil, fmt.Errorf("derive endpoint: %w", err)
	}

	return &datasource.DataSource{
		Instances: []*api_common.TGenericDataSourceInstance{dsi},
	}, nil
}
