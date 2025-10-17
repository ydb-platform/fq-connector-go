package table_store_type_cache

import (
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type Cache interface {
	Put(dsi *api_common.TGenericDataSourceInstance, tableName string, storeType options.StoreType) bool
	Get(dsi *api_common.TGenericDataSourceInstance, tableName string) (storeType options.StoreType, found bool)
}
