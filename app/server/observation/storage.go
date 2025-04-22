package observation

import (
	"fmt"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

func NewStorage(cfg *config.TObservationConfig) (Storage, error) {
	if cfg == nil {
		return storageDummyImpl{}, nil
	}

	storage, err := newStorageSQLite(cfg.Storage.GetSqlite())
	if err != nil {
		return nil, fmt.Errorf("new storage SQLite: %w", err)
	}

	return storage, nil
}
