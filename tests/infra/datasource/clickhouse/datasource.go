package clickhouse

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/infra/docker_compose"
)

const (
	serviceName        = "clickhouse"
	internalPortHTTP   = 8123
	internalPortNative = 9000
	database           = "connector"
	username           = "admin"
	password           = "password"
)

func deriveDataSourceFromDockerCompose(ed *docker_compose.EndpointDeterminer) (*datasource.DataSource, error) {
	var (
		dsi = &api_common.TGenericDataSourceInstance{
			Kind:     api_common.EGenericDataSourceKind_CLICKHOUSE,
			Database: database,
			Credentials: &api_common.TGenericCredentials{
				Payload: &api_common.TGenericCredentials_Basic{
					Basic: &api_common.TGenericCredentials_TBasic{
						Username: username,
						Password: password,
					},
				},
			},
			UseTls: false,
		}
		err error
	)

	dsiNative := proto.Clone(dsi).(*api_common.TGenericDataSourceInstance)
	dsiNative.Protocol = api_common.EGenericProtocol_NATIVE

	dsiNative.Endpoint, err = ed.GetEndpoint(serviceName, internalPortNative)
	if err != nil {
		return nil, fmt.Errorf("derive native endpoint: %w", err)
	}

	dsiHTTP := proto.Clone(dsi).(*api_common.TGenericDataSourceInstance)
	dsiHTTP.Protocol = api_common.EGenericProtocol_HTTP

	dsiHTTP.Endpoint, err = ed.GetEndpoint(serviceName, internalPortHTTP)
	if err != nil {
		return nil, fmt.Errorf("derive HTTP endpoint: %w", err)
	}

	return &datasource.DataSource{Instances: []*api_common.TGenericDataSourceInstance{dsiHTTP, dsiNative}}, nil
}
