package table_metadata_cache

import (
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ Cache = (*ristrettoCache)(nil)

func serializeKey(dsi *api_common.TGenericDataSourceInstance, tableName string) string {
	return fmt.Sprintf("%s:%d_%s_%s", dsi.Endpoint.Host, dsi.Endpoint.Port, dsi.Database, tableName)
}

type ristrettoCache struct {
	cache *ristretto.Cache[string, []byte]
	ttl   time.Duration
}

func (r *ristrettoCache) Put(logger *zap.Logger, dsi *api_common.TGenericDataSourceInstance, tableName string, value *TValue) bool {
	key := serializeKey(dsi, tableName)

	logger.Debug("putting value into ristretto cache",
		zap.String("key", key),
		zap.Stringer("value", value),
	)

	// Serialize TValue to bytes
	data, err := proto.Marshal(value)
	if err != nil {
		panic(err)
	}

	return r.cache.SetWithTTL(key, data, int64(len(data)), r.ttl)
}

func (r *ristrettoCache) Get(logger *zap.Logger, dsi *api_common.TGenericDataSourceInstance, tableName string) (*TValue, bool) {
	key := serializeKey(dsi, tableName)

	logger.Debug("getting value from ristretto cache",
		zap.String("key", key),
		zap.String("table", tableName),
	)

	data, found := r.cache.Get(key)
	if !found {
		logger.Debug("ristretto cache miss", zap.String("key", key))
		return nil, false
	}

	// Deserialize bytes to TValue
	value := &TValue{}
	if err := proto.Unmarshal(data, value); err != nil {
		panic(err)
	}

	logger.Debug("ristretto cache hit", zap.String("key", key), zap.Stringer("value", value))

	return value, true
}

func (r *ristrettoCache) Metrics() *Metrics {
	m := r.cache.Metrics
	costAdded := m.CostAdded()
	costEvicted := m.CostEvicted()
	keysAdded := m.KeysAdded()
	keysEvicted := m.KeysEvicted()

	// KeysDropped represents keys that were rejected/dropped (not added successfully)
	// We can compute this as the difference between keys that should have been added
	// and keys that were actually added, but Ristretto doesn't expose this directly.
	// For now, we'll set it to 0 as Ristretto doesn't track dropped keys separately.

	return &Metrics{
		Hits:        m.Hits(),
		Misses:      m.Misses(),
		Ratio:       m.Ratio(),
		KeysAdded:   keysAdded,
		KeysEvicted: keysEvicted,
		KeysDropped: 0, // Ristretto doesn't expose this metric
		Size:        costAdded - costEvicted,
	}
}

func newRistrettoCache(cfg *config.TYdbConfig_TTableMetadataCache) (*ristrettoCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: cfg.GetRistretto().NumCounters,
		MaxCost:     cfg.GetRistretto().MaxCost,
		BufferItems: cfg.GetRistretto().BufferItems,
		Metrics:     true,
	})

	if err != nil {
		return nil, fmt.Errorf("ristretto new cache: %w", err)
	}

	return &ristrettoCache{
		cache: cache,
		ttl:   common.MustDurationFromString(cfg.GetTtl()),
	}, nil
}
