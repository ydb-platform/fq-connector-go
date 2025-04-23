package observation

import (
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"go.uber.org/zap"
)

var _ Storage = (*storageDummyImpl)(nil)

type storageDummyImpl struct {
}

// Incoming query operations
func (s storageDummyImpl) CreateIncomingQuery(dataSourceKind api_common.EGenericDataSourceKind) (IncomingQueryID, error) {
	return 0, nil
}

func (s storageDummyImpl) FinishIncomingQuery(id IncomingQueryID, stats *api_service_protos.TReadSplitsResponse_TStats) error {
	return nil
}

func (s storageDummyImpl) CancelIncomingQuery(id IncomingQueryID, errorMsg string, stats *api_service_protos.TReadSplitsResponse_TStats) error {
	return nil
}

func (s storageDummyImpl) ListIncomingQueries(state *QueryState, limit int, offset int) ([]*IncomingQuery, error) {
	return nil, nil
}

// Outgoing query operations
func (s storageDummyImpl) CreateOutgoingQuery(logger *zap.Logger, incomingQueryID IncomingQueryID, dsi *api_common.TGenericDataSourceInstance, queryText string, queryArgs []any) (OutgoingQueryID, error) {
	return 0, nil
}

func (s storageDummyImpl) FinishOutgoingQuery(id OutgoingQueryID, rowsRead int64) error {
	return nil
}

func (s storageDummyImpl) CancelOutgoingQuery(id OutgoingQueryID, errorMsg string) error {
	return nil
}

func (s storageDummyImpl) ListOutgoingQueries(incomingQueryID *IncomingQueryID, state *QueryState, limit int, offset int) ([]*OutgoingQuery, error) {
	return nil, nil
}

// Analysis operations
func (s storageDummyImpl) ListSimilarOutgoingQueriesWithDifferentStats() ([][]*OutgoingQuery, error) {
	return nil, nil
}

// Lifecycle
func (s storageDummyImpl) Close() error {
	return nil
}
