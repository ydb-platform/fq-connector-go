package observation

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/api/observation"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

// Storage interface defines methods for query storage
type Storage interface {
	// Incoming query operations
	CreateIncomingQuery(
		ctx context.Context, logger *zap.Logger, dataSourceKind api_common.EGenericDataSourceKind) (*zap.Logger, string, error)
	FinishIncomingQuery(
		ctx context.Context, logger *zap.Logger, id string, stats *api_service_protos.TReadSplitsResponse_TStats) error
	CancelIncomingQuery(
		ctx context.Context, logger *zap.Logger, id string, errorMsg string, stats *api_service_protos.TReadSplitsResponse_TStats) error
	ListIncomingQueries(
		ctx context.Context, logger *zap.Logger, state *observation.QueryState, limit, offset int,
	) ([]*observation.IncomingQuery, error)

	// Outgoing query operations
	CreateOutgoingQuery(
		ctx context.Context,
		logger *zap.Logger,
		incomingQueryID string,
		dsi *api_common.TGenericDataSourceInstance,
		queryText string,
		queryArgs []any,
	) (*zap.Logger, string, error)
	FinishOutgoingQuery(
		ctx context.Context, logger *zap.Logger, id string, rowsRead int64) error
	CancelOutgoingQuery(
		ctx context.Context, logger *zap.Logger, id string, errorMsg string) error
	ListOutgoingQueries(
		ctx context.Context, logger *zap.Logger, incomingQueryID *string, state *observation.QueryState, limit, offset int,
	) ([]*observation.OutgoingQuery, error)

	Close(ctx context.Context) error
}

// Helper functions for timestamp conversion
func TimeToProtoTimestamp(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

func ProtoTimestampToTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}

	return ts.AsTime()
}

func TimePointerToProtoTimestamp(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}

	return timestamppb.New(*t)
}
