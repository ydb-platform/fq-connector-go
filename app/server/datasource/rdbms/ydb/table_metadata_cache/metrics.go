package table_metadata_cache

import (
	"github.com/ydb-platform/fq-connector-go/library/go/core/metrics"
	"github.com/ydb-platform/fq-connector-go/library/go/core/metrics/solomon"
)

// RegisterMetrics registers cache metrics with the provided registry.
// It uses the ydb_table_metadata_cache_ prefix for all metrics.
func RegisterMetrics(registry metrics.Registry, cache Cache) {
	metrics := cache.Metrics()
	if metrics == nil {
		// noop cache returns nil metrics
		return
	}

	// Register gauges for cache statistics with ydb_table_metadata_cache_ prefix
	_ = registry.FuncGauge("ydb_table_metadata_cache_hit_ratio", func() float64 {
		m := cache.Metrics()
		if m == nil {
			return 0
		}
		return m.Ratio
	})

	cacheHits := registry.FuncCounter("ydb_table_metadata_cache_hits_total", func() int64 {
		m := cache.Metrics()
		if m == nil {
			return 0
		}
		return int64(m.Hits)
	})

	cacheMisses := registry.FuncCounter("ydb_table_metadata_cache_misses_total", func() int64 {
		m := cache.Metrics()
		if m == nil {
			return 0
		}
		return int64(m.Misses)
	})

	cacheKeysAdded := registry.FuncCounter("ydb_table_metadata_cache_keys_added_total", func() int64 {
		m := cache.Metrics()
		if m == nil {
			return 0
		}
		return int64(m.KeysAdded)
	})

	cacheKeysEvicted := registry.FuncCounter("ydb_table_metadata_cache_keys_evicted_total", func() int64 {
		m := cache.Metrics()
		if m == nil {
			return 0
		}
		return int64(m.KeysEvicted)
	})

	cacheKeysDropped := registry.FuncCounter("ydb_table_metadata_cache_keys_dropped_total", func() int64 {
		m := cache.Metrics()
		if m == nil {
			return 0
		}
		return int64(m.KeysDropped)
	})

	_ = registry.FuncGauge("ydb_table_metadata_cache_size", func() float64 {
		m := cache.Metrics()
		if m == nil {
			return 0
		}
		return float64(m.Size)
	})

	// Mark counters as rated for proper visualization
	solomon.Rated(cacheHits)
	solomon.Rated(cacheMisses)
	solomon.Rated(cacheKeysAdded)
	solomon.Rated(cacheKeysEvicted)
	solomon.Rated(cacheKeysDropped)
}
