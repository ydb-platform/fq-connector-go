package oracle

import (
	"fmt"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/infra/docker_compose"
)

const (
	endpointServiceName = "oracle"
	internalPort        = 1521
	database            = "C##ADMIN"
	username            = "C##ADMIN"
	password            = "password"
	dbServiceName       = "FREE"
)

func deriveDataSourceFromDockerCompose(ed *docker_compose.EndpointDeterminer) (*datasource.DataSource, error) {
	dsi := &api_common.TGenericDataSourceInstance{
		Kind:     api_common.EGenericDataSourceKind_ORACLE,
		Database: database,
		Credentials: &api_common.TGenericCredentials{
			Payload: &api_common.TGenericCredentials_Basic{
				Basic: &api_common.TGenericCredentials_TBasic{
					Username: username,
					Password: password,
				},
			},
		},
		Options: &api_common.TGenericDataSourceInstance_OracleOptions{
			OracleOptions: &api_common.TOracleDataSourceOptions{
				ServiceName: dbServiceName,
			},
		},
		Protocol: api_common.EGenericProtocol_NATIVE,
		UseTls:   false,
	}

	var err error

	dsi.Endpoint, err = ed.GetEndpoint(endpointServiceName, internalPort)
	if err != nil {
		return nil, fmt.Errorf("derive endpoint: %w", err)
	}

	return &datasource.DataSource{
		Instances: []*api_common.TGenericDataSourceInstance{dsi},
	}, nil
}
