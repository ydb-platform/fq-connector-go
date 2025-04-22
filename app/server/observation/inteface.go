package observation

import (
	"time"

	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

type IncomingQueryID uint64
type OutgoingQueryID uint64

// QueryState represents the state of a query
type QueryState string

const (
	QueryStateRunning   QueryState = "running"
	QueryStateFinished  QueryState = "finished"
	QueryStateCancelled QueryState = "canceled"
)

// IncomingQuery represents an incoming query
type IncomingQuery struct {
	ID             IncomingQueryID `json:"id"`
	DataSourceKind string          `json:"data_source_kind"`
	RowsRead       int64           `json:"rows_read"`
	BytesRead      int64           `json:"bytes_read"`
	State          QueryState      `json:"state"`
	CreatedAt      time.Time       `json:"created_at"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	Error          string          `json:"error,omitempty"`
}

// OutgoingQuery represents an outgoing query to a data source
type OutgoingQuery struct {
	ID               OutgoingQueryID `json:"id"`
	IncomingQueryID  IncomingQueryID `json:"incoming_query_id"`
	DatabaseName     string          `json:"database_name"`
	DatabaseEndpoint string          `json:"database_endpoint"`
	QueryText        string          `json:"query_text"`
	QueryArgs        string          `json:"query_args"`
	State            QueryState      `json:"state"`
	CreatedAt        time.Time       `json:"created_at"`
	FinishedAt       *time.Time      `json:"finished_at,omitempty"`
	RowsRead         int64           `json:"rows_read"`
	Error            string          `json:"error,omitempty"`
}

// Storage interface defines methods for query storage
type Storage interface {
	// Incoming query operations
	CreateIncomingQuery(dataSourceKind api_common.EGenericDataSourceKind) (IncomingQueryID, error)
	FinishIncomingQuery(id IncomingQueryID, stats *api_service_protos.TReadSplitsResponse_TStats) error
	CancelIncomingQuery(id IncomingQueryID, errorMsg string, stats *api_service_protos.TReadSplitsResponse_TStats) error
	ListIncomingQueries(state *QueryState, limit, offset int) ([]*IncomingQuery, error)

	// Outgoing query operations
	CreateOutgoingQuery(logger *zap.Logger, incomingQueryID IncomingQueryID, dsi *api_common.TGenericDataSourceInstance, queryText string, queryArgs []any) (OutgoingQueryID, error)
	FinishOutgoingQuery(id OutgoingQueryID, rowsRead int64) error
	CancelOutgoingQuery(id OutgoingQueryID, errorMsg string) error
	ListOutgoingQueries(incomingQueryID *IncomingQueryID, state *QueryState, limit, offset int) ([]*OutgoingQuery, error)

	// Analysis operations
	ListSimilarOutgoingQueriesWithDifferentStats() ([][]*OutgoingQuery, error)

	// Lifecycle
	Close() error
}
