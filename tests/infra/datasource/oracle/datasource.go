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
	dsi := &api_common.TDataSourceInstance{
		Kind:     api_common.EDataSourceKind_ORACLE,
		Database: database,
		Credentials: &api_common.TCredentials{
			Payload: &api_common.TCredentials_Basic{
				Basic: &api_common.TCredentials_TBasic{
					Username: username,
					Password: password,
				},
			},
		},
		Options: &api_common.TDataSourceInstance_OraOptions{
			OraOptions: &api_common.TOracleDataSourceOptions{
				ServiceName: dbServiceName,
			},
		},
		Protocol: api_common.EProtocol_NATIVE,
		UseTls:   false,
	}

	var err error
	dsi.Endpoint, err = ed.GetEndpoint(endpointServiceName, internalPort)

	if err != nil {
		return nil, fmt.Errorf("derive endpoint: %w", err)
	}

	return &datasource.DataSource{
		Instances: []*api_common.TDataSourceInstance{dsi},
	}, nil
}
