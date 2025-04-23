package observation

import (
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

var _ Storage = (*storageDummyImpl)(nil)

type storageDummyImpl struct {
}

// Incoming query operations
func (storageDummyImpl) CreateIncomingQuery(_ api_common.EGenericDataSourceKind) (IncomingQueryID, error) {
	return 0, nil
}

func (storageDummyImpl) FinishIncomingQuery(_ IncomingQueryID, _ *api_service_protos.TReadSplitsResponse_TStats) error {
	return nil
}

func (storageDummyImpl) CancelIncomingQuery(_ IncomingQueryID, _ string, _ *api_service_protos.TReadSplitsResponse_TStats) error {
	return nil
}

func (storageDummyImpl) ListIncomingQueries(_ *QueryState, _ int, _ int) ([]*IncomingQuery, error) {
	return nil, nil
}

// Outgoing query operations
func (storageDummyImpl) CreateOutgoingQuery(
	_ *zap.Logger,
	_ IncomingQueryID,
	_ *api_common.TGenericDataSourceInstance,
	_ string,
	_ []any,
) (OutgoingQueryID, error) {
	return 0, nil
}

func (storageDummyImpl) FinishOutgoingQuery(_ OutgoingQueryID, _ int64) error {
	return nil
}

func (storageDummyImpl) CancelOutgoingQuery(_ OutgoingQueryID, _ string) error {
	return nil
}

func (storageDummyImpl) ListOutgoingQueries(_ *IncomingQueryID, _ *QueryState, _ int, _ int) ([]*OutgoingQuery, error) {
	return nil, nil
}

// Analysis operations
func (storageDummyImpl) ListSimilarOutgoingQueriesWithDifferentStats(_ *zap.Logger) ([][]*OutgoingQuery, error) {
	return nil, nil
}

// Lifecycle
func (storageDummyImpl) Close() error {
	return nil
}
