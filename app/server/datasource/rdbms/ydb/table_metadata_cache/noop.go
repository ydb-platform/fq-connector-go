package table_metadata_cache

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

var _ Cache = (*noopCache)(nil)

type noopCache struct {
}

func (noopCache) Put(_ *api_common.TGenericDataSourceInstance, _ string, _ options.StoreType) bool {
	return true
}

func (noopCache) Get(_ *api_common.TGenericDataSourceInstance, _ string) (options.StoreType, bool) {
	return 0, false
}

func (noopCache) Metrics() *Metrics {
	return nil
}
