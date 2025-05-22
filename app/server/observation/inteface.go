package observation

import (
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
	CreateIncomingQuery(dataSourceKind api_common.EGenericDataSourceKind) (uint64, error)
	FinishIncomingQuery(id uint64, stats *api_service_protos.TReadSplitsResponse_TStats) error
	CancelIncomingQuery(id uint64, errorMsg string, stats *api_service_protos.TReadSplitsResponse_TStats) error
	ListIncomingQueries(state *observation.QueryState, limit, offset int) ([]*observation.IncomingQuery, error)

	// Outgoing query operations
	CreateOutgoingQuery(
		logger *zap.Logger,
		incomingQueryID uint64,
		dsi *api_common.TGenericDataSourceInstance,
		queryText string,
		queryArgs []any,
	) (uint64, error)
	FinishOutgoingQuery(id uint64, rowsRead int64) error
	CancelOutgoingQuery(id uint64, errorMsg string) error
	ListOutgoingQueries(incomingQueryID *uint64, state *observation.QueryState, limit, offset int) ([]*observation.OutgoingQuery, error)

	// Lifecycle
	Close() error
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
