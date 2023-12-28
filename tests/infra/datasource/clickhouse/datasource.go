package clickhouse

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/docker_compose"
)

type DataSource struct {
	dsi map[api_common.EProtocol]*api_common.TDataSourceInstance
}

func (ds *DataSource) GetDataSourceInstance(protocol api_common.EProtocol) (*api_common.TDataSourceInstance, error) {
	result, exists := ds.dsi[protocol]
	if !exists {
		return nil, fmt.Errorf("unexpected protocol %v", protocol)
	}

	return result, nil
}

const (
	serviceName        = "clickhouse"
	internalPortHTTP   = 8123
	internalPortNative = 9000
	database           = "connector"
	username           = "admin"
	password           = "password"
)

func DeriveDataSourceFromDockerCompose(ed *docker_compose.EndpointDeterminer) (*DataSource, error) {
	var (
		dsi = &api_common.TDataSourceInstance{
			Kind:     api_common.EDataSourceKind_CLICKHOUSE,
			Database: database,
			Credentials: &api_common.TCredentials{
				Payload: &api_common.TCredentials_Basic{
					Basic: &api_common.TCredentials_TBasic{
						Username: username,
						Password: password,
					},
				},
			},
			UseTls: false,
		}
		err error
	)

	dsiNative := proto.Clone(dsi).(*api_common.TDataSourceInstance)
	dsiNative.Protocol = api_common.EProtocol_NATIVE

	dsiNative.Endpoint, err = ed.GetEndpoint(serviceName, internalPortNative)
	if err != nil {
		return nil, fmt.Errorf("derive native endpoint: %w", err)
	}

	dsiHTTP := proto.Clone(dsi).(*api_common.TDataSourceInstance)
	dsiHTTP.Protocol = api_common.EProtocol_HTTP

	dsiHTTP.Endpoint, err = ed.GetEndpoint(serviceName, internalPortHTTP)
	if err != nil {
		return nil, fmt.Errorf("derive HTTP endpoint: %w", err)
	}

	out := &DataSource{
		dsi: map[api_common.EProtocol]*api_common.TDataSourceInstance{
			api_common.EProtocol_HTTP:   dsiHTTP,
			api_common.EProtocol_NATIVE: dsiNative,
		},
	}

	return out, nil
}
