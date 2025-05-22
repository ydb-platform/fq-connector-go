package observation

import (
	"context"

	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/api/observation"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

var _ Storage = (*storageDummyImpl)(nil)

type storageDummyImpl struct {
}

// Incoming query operations
func (storageDummyImpl) CreateIncomingQuery(_ context.Context, _ *zap.Logger, _ api_common.EGenericDataSourceKind) (uint64, error) {
	return 0, nil
}

func (storageDummyImpl) FinishIncomingQuery(
	_ context.Context, _ *zap.Logger, _ uint64, _ *api_service_protos.TReadSplitsResponse_TStats) error {
	return nil
}

func (storageDummyImpl) CancelIncomingQuery(
	_ context.Context, _ *zap.Logger, _ uint64, _ string, _ *api_service_protos.TReadSplitsResponse_TStats) error {
	return nil
}

func (storageDummyImpl) ListIncomingQueries(
	_ context.Context, _ *zap.Logger, _ *observation.QueryState, _ int, _ int) ([]*observation.IncomingQuery, error) {
	return nil, nil
}

// Outgoing query operations
func (storageDummyImpl) CreateOutgoingQuery(
	_ context.Context,
	_ *zap.Logger,
	_ uint64,
	_ *api_common.TGenericDataSourceInstance,
	_ string,
	_ []any,
) (uint64, error) {
	return 0, nil
}

func (storageDummyImpl) FinishOutgoingQuery(_ context.Context, _ *zap.Logger, _ uint64, _ int64) error {
	return nil
}

func (storageDummyImpl) CancelOutgoingQuery(_ context.Context, _ *zap.Logger, _ uint64, _ string) error {
	return nil
}

func (storageDummyImpl) ListOutgoingQueries(
	_ context.Context, _ *zap.Logger, _ *uint64, _ *observation.QueryState, _ int, _ int) ([]*observation.OutgoingQuery, error) {
	return nil, nil
}

// Lifecycle
func (storageDummyImpl) Close(_ context.Context) error {
	return nil
}
