package table_metadata_cache

import (
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

var _ Cache = (*noopCache)(nil)

type noopCache struct {
}

func (noopCache) Put(_ *zap.Logger, _ *api_common.TGenericDataSourceInstance, _ string, _ *TValue) bool {
	return true
}

func (noopCache) Get(_ *zap.Logger, _ *api_common.TGenericDataSourceInstance, _ string) (*TValue, bool) {
	return nil, false
}

func (noopCache) Metrics() *Metrics {
	return nil
}
