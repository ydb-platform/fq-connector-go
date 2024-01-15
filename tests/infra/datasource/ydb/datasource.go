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
		dsi = &api_common.TDataSourceInstance{
			Kind:     api_common.EDataSourceKind_YDB,
			Database: database,
			UseTls:   false,
			Protocol: api_common.EProtocol_NATIVE,
		}

		err error
	)

	dsi.Endpoint, err = ed.GetEndpoint(serviceName, internalPort)
	if err != nil {
		return nil, fmt.Errorf("derive endpoint: %w", err)
	}

	return &datasource.DataSource{
		Instances: []*api_common.TDataSourceInstance{dsi},
	}, nil
}
