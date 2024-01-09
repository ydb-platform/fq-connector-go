package datasource

import (
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

type DataSource struct {
	Instances []*api_common.TDataSourceInstance
}
