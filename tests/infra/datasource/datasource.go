package datasource

import (
	"fmt"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
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

func NewDataSource(
	dsi map[api_common.EProtocol]*api_common.TDataSourceInstance,
) *DataSource {
	return &DataSource{dsi: dsi}
}
