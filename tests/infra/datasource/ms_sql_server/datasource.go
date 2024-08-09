package ms_sql_server

import (
	"fmt"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/infra/docker_compose"
)

const (
	serviceName  = "ms_sql_server"
	internalPort = 1433
	database     = "master"
	username     = "sa"
	password     = "Qwerty12345!"
)

func deriveDataSourceFromDockerCompose(ed *docker_compose.EndpointDeterminer) (*datasource.DataSource, error) {
	dsi := &api_common.TDataSourceInstance{
		Kind:     api_common.EDataSourceKind_MS_SQL_SERVER,
		Database: database,
		Credentials: &api_common.TCredentials{
			Payload: &api_common.TCredentials_Basic{
				Basic: &api_common.TCredentials_TBasic{
					Username: username,
					Password: password,
				},
			},
		},
		Protocol: api_common.EProtocol_NATIVE,
		UseTls:   false,
	}

	var err error
	dsi.Endpoint, err = ed.GetEndpoint(serviceName, internalPort)

	if err != nil {
		return nil, fmt.Errorf("derive endpoint: %w", err)
	}

	return &datasource.DataSource{
		Instances: []*api_common.TDataSourceInstance{dsi},
	}, nil
}
