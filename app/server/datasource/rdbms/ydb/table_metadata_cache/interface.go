package table_metadata_cache

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

// Metrics represents cache statistics
type Metrics struct {
	Hits        uint64
	Misses      uint64
	Ratio       float64
	KeysAdded   uint64
	KeysEvicted uint64
	CostAdded   uint64
	CostEvicted uint64
}

type Cache interface {
	Put(dsi *api_common.TGenericDataSourceInstance, tableName string, storeType options.StoreType) bool
	Get(dsi *api_common.TGenericDataSourceInstance, tableName string) (storeType options.StoreType, found bool)
	Metrics() *Metrics
}
