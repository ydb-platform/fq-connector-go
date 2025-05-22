package observation

import (
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	observation "github.com/ydb-platform/fq-connector-go/api/observation"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

var _ Storage = (*storageDummyImpl)(nil)

type storageDummyImpl struct {
}

// Incoming query operations
func (storageDummyImpl) CreateIncomingQuery(_ api_common.EGenericDataSourceKind) (uint64, error) {
	return 0, nil
}

func (storageDummyImpl) FinishIncomingQuery(_ uint64, _ *api_service_protos.TReadSplitsResponse_TStats) error {
	return nil
}

func (storageDummyImpl) CancelIncomingQuery(_ uint64, _ string, _ *api_service_protos.TReadSplitsResponse_TStats) error {
	return nil
}

func (storageDummyImpl) ListIncomingQueries(_ *observation.QueryState, _ int, _ int) ([]*observation.IncomingQuery, error) {
	return nil, nil
}

// Outgoing query operations
func (storageDummyImpl) CreateOutgoingQuery(
	_ *zap.Logger,
	_ uint64,
	_ *api_common.TGenericDataSourceInstance,
	_ string,
	_ []any,
) (uint64, error) {
	return 0, nil
}

func (storageDummyImpl) FinishOutgoingQuery(_ uint64, _ int64) error {
	return nil
}

func (storageDummyImpl) CancelOutgoingQuery(_ uint64, _ string) error {
	return nil
}

func (storageDummyImpl) ListOutgoingQueries(_ *uint64, _ *observation.QueryState, _ int, _ int) ([]*observation.OutgoingQuery, error) {
	return nil, nil
}

// Lifecycle
func (storageDummyImpl) Close() error {
	return nil
}
