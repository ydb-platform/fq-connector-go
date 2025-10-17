package table_store_type_cache

import (
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

var _ Cache = (*ristrettoCache)(nil)

type ristrettoCache struct {
	cache *ristretto.Cache[string, int]
	ttl   time.Duration
}

func (r *ristrettoCache) Put(key *Key, storeType options.StoreType) {
	r.cache.SetWithTTL(key.serialize(), int(storeType), 1, r.ttl)
}

func (r *ristrettoCache) Get(key *Key) (options.StoreType, bool) {
	value, found := r.cache.Get(key.serialize())
	if !found {
		return 0, false
	}

	return options.StoreType(value), true
}

func newRistrettoCache(cfg *config.TYdbConfig_TTableStoreTypeCache) (*ristrettoCache, error) {
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
