package table_store_type_cache

import "github.com/ydb-platform/ydb-go-sdk/v3/table/options"

var _ Cache = (*noopCache)(nil)

type noopCache struct {
}

func (noopCache) Put(key *Key, storeType options.StoreType) {
}

func (noopCache) Get(key *Key) (options.StoreType, bool) {
	return 0, false
}
