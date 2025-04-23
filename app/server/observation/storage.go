package observation

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

func NewStorage(logger *zap.Logger, cfg *config.TObservationConfig) (Storage, error) {
	if cfg == nil {
		return storageDummyImpl{}, nil
	}

	storage, err := newStorageSQLite(logger, cfg.Storage.GetSqlite())
	if err != nil {
		return nil, fmt.Errorf("new storage SQLite: %w", err)
	}

	return storage, nil
}
