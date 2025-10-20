package table_metadata_cache

import (
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

var _ Cache = (*ristrettoCache)(nil)

func serializeKey(dsi *api_common.TGenericDataSourceInstance, tableName string) string {
	return fmt.Sprintf("%s:%d_%s_%s", dsi.Endpoint.Host, dsi.Endpoint.Port, dsi.Database, tableName)
}

type ristrettoCache struct {
	cache *ristretto.Cache[string, int]
	ttl   time.Duration
}

func (r *ristrettoCache) Put(dsi *api_common.TGenericDataSourceInstance, tableName string, storeType options.StoreType) bool {
	key := serializeKey(dsi, tableName)

	return r.cache.SetWithTTL(key, int(storeType), 1, r.ttl)
}

func (r *ristrettoCache) Get(dsi *api_common.TGenericDataSourceInstance, tableName string) (options.StoreType, bool) {
	key := serializeKey(dsi, tableName)

	value, found := r.cache.Get(key)
	if !found {
		return 0, false
	}

	return options.StoreType(value), true
}

func newRistrettoCache(cfg *config.TYdbConfig_TTableMetadataCache) (*ristrettoCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config[string, int]{
		NumCounters: cfg.GetRistretto().NumCounters,
		MaxCost:     cfg.GetRistretto().MaxCost,
		BufferItems: cfg.GetRistretto().BufferItems,
	})

	if err != nil {
		return nil, fmt.Errorf("ristretto new cache: %w", err)
	}

	return &ristrettoCache{
		cache: cache,
		ttl:   common.MustDurationFromString(cfg.GetTtl()),
	}, nil
}
