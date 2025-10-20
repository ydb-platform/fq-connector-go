package table_metadata_cache

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

type Cache interface {
	Put(dsi *api_common.TGenericDataSourceInstance, tableName string, storeType options.StoreType) bool
	Get(dsi *api_common.TGenericDataSourceInstance, tableName string) (storeType options.StoreType, found bool)
}
