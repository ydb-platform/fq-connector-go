package ydb

import (
	"fmt"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/infra/docker_compose"
)

const (
	serviceName  = "ydb"
	internalPort = 2136
	database     = "local"
)

func deriveDataSourceFromDockerCompose(ed *docker_compose.EndpointDeterminer) (*datasource.DataSource, error) {
	var (
		dsi = &api_common.TGenericDataSourceInstance{
			Kind:     api_common.EGenericDataSourceKind_YDB,
			Database: database,
			UseTls:   false,
			Protocol: api_common.EGenericProtocol_NATIVE,
			Credentials: &api_common.TGenericCredentials{
				Payload: &api_common.TGenericCredentials_Basic{
					Basic: &api_common.TGenericCredentials_TBasic{
						Username: "admin",
						Password: "password",
					},
				},
			},
		}

		err error
	)

	dsi.Endpoint, err = ed.GetEndpoint(serviceName, internalPort)
	if err != nil {
		return nil, fmt.Errorf("derive endpoint: %w", err)
	}

	return &datasource.DataSource{
		Instances: []*api_common.TGenericDataSourceInstance{dsi},
	}, nil
}
