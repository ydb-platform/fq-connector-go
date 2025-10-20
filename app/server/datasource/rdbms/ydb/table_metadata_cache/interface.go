package table_metadata_cache

import (
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

// Metrics represents cache statistics
type Metrics struct {
	Hits        uint64
	Misses      uint64
	Ratio       float64
	KeysAdded   uint64
	KeysEvicted uint64
	KeysDropped uint64
	Size        uint64
}

type Cache interface {
	Put(dsi *api_common.TGenericDataSourceInstance, tableName string, value *TValue) bool
	Get(dsi *api_common.TGenericDataSourceInstance, tableName string) (*TValue, bool)
	Metrics() *Metrics
}
