package table_store_type_cache

import (
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

var _ Cache = (*noopCache)(nil)

type noopCache struct {
}

func (noopCache) Put(dsi *api_common.TGenericDataSourceInstance, tableName string, storeType options.StoreType) bool {
	return true
}

func (noopCache) Get(dsi *api_common.TGenericDataSourceInstance, tableName string) (options.StoreType, bool) {
	return 0, false
}
