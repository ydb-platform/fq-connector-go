package observation

import (
	"time"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

type QueryID uint64

// Query represents a database query and its metadata
type Query struct {
	ID               QueryID    `json:"id"`
	DatabaseName     string     `json:"database_name"`
	DatabaseEndpoint string     `json:"database_endpoint"`
	DataSourceKind   string     `json:"data_source_kind"`
	QueryText        string     `json:"query_text"`
	QueryArgs        string     `json:"query_args"`
	CreatedAt        time.Time  `json:"created_at"`
	FinishedAt       *time.Time `json:"finished_at,omitempty"`
	RowsRead         int64      `json:"rows_read"`
	BytesRead        int64      `json:"bytes_read"`
	State            QueryState `json:"state"`
	Error            string     `json:"error,omitempty"`
}

type Storage interface {
	CreateQuery(dsi *api_common.TGenericDataSourceInstance) (QueryID, error)
	SetQueryDetails(id QueryID, queryText, queryArgs string) error
	GetQuery(id QueryID) (*Query, error)
	ListQueries(state *QueryState, limit, offset int) ([]*Query, error)
	UpdateQueryProgress(id QueryID, rowsRead, bytesRead int64) error
	FinishQuery(id QueryID, rowsRead, bytesRead int64) error
	CancelQuery(id QueryID, errorMsg string, rowsRead, bytesRead int64) error
	DeleteQuery(id QueryID) error
	GetRunningQueries() ([]*Query, error)
	FindSimilarQueriesWithDifferentUsage() ([][]*Query, error)
	Close() error
}
