package table_metadata_cache

import (
	"go.uber.org/zap"

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
	Put(logger *zap.Logger, dsi *api_common.TGenericDataSourceInstance, tableName string, value *TValue) bool
	Get(logger *zap.Logger, dsi *api_common.TGenericDataSourceInstance, tableName string) (*TValue, bool)
	Metrics() *Metrics
}
