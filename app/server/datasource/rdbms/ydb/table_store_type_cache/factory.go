package table_store_type_cache

import (
	"fmt"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

func NewCache(cfg *config.TYdbConfig_TTableStoreTypeCache) (Cache, error) {
	if cfg == nil {
		return &noopCache{}, nil
	}

	switch cfg.GetStorage().(type) {
	case *config.TYdbConfig_TTableStoreTypeCache_Ristretto:
		return newRistrettoCache(cfg)
	default:
		return nil, fmt.Errorf("unknown storage: %v", cfg.GetStorage())
	}
}
