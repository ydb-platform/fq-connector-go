package utils

import (
	"context"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

type QueryParams struct {
	Ctx       context.Context
	Logger    *zap.Logger
	QueryText string
	QueryArgs *QueryArgs
}

type Connection interface {
	// Query runs a query on a specific connection.
	Query(params *QueryParams) (Rows, error)
	// For the most of the data sources the database name / table name pair
	// is strictly defined by the user input.
	// However, in certain kinds of data sources it's necessary
	// to override database / table names specified by the user request.
	From() (database, table string)
	// Annotated logger that should be used to log all the events related
	// to the particular data source instance.
	Logger() *zap.Logger
	// Close terminates network connections.
	Close() error
}

type Rows interface {
	Close() error
	Err() error
	Next() bool
	NextResultSet() bool
	Scan(dest ...any) error
	MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error)
}

//go:generate stringer -type=QueryPhase
type QueryPhase int8

const (
	QueryPhaseUnspecified QueryPhase = iota
	QueryPhaseDescribeTable
	QueryPhaseListSplits
	QueryPhaseReadSplits
)

type ConnectionParams struct {
	Ctx                context.Context                        // mandatory
	Logger             *zap.Logger                            // mandatory
	DataSourceInstance *api_common.TGenericDataSourceInstance // mandatory
	TableName          string                                 // mandatory
	QueryPhase         QueryPhase                             // mandatory

	// Split field may be filled when making a connection for the ReadSplits request,
	// because different table splits can require to connect different database instances.
	Split *api_service_protos.TSplit // optional
}

type ConnectionManager interface {
	Make(params *ConnectionParams) ([]Connection, error)
	Release(ctx context.Context, logger *zap.Logger, cs []Connection)
}

type ConnectionManagerBase struct {
	QueryLoggerFactory common.QueryLoggerFactory
}

type SelectQueryParts struct {
	SelectClause string
	FromClause   string
	WhereClause  string
}

type SQLFormatter interface {
	// Get placeholder for n'th argument (starting from 0) for prepared statement
	GetPlaceholder(n int) string

	// Sanitize names of databases, tables, columns, views, schemas
	SanitiseIdentifier(ident string) string

	// Support for high level expression (without subexpressions, they are checked separately)
	SupportsPushdownExpression(expression *api_service_protos.TExpression) bool

	// FormatFrom builds a substring containing the literals
	// that must be placed after FROM (`SELECT ... FROM <this>`).
	FormatFrom(tableName string) string

	// RenderSelectQueryText composes final query text from the given clauses.
	// Particular implementation may mix-in some additional parts into the query.
	RenderSelectQueryText(parts *SelectQueryParts, split *api_service_protos.TSplit) (string, error)
}

type SchemaProvider interface {
	GetSchema(
		ctx context.Context,
		logger *zap.Logger,
		conn Connection,
		request *api_service_protos.TDescribeTableRequest,
	) (*api_service_protos.TSchema, error)
}

type ListSplitsParams struct {
	Ctx                   context.Context
	Logger                *zap.Logger
	MakeConnectionRetrier retry.Retrier
	ConnectionManager     ConnectionManager
	Request               *api_service_protos.TListSplitsRequest
	Select                *api_service_protos.TSelect
	// Interface implementations should not close this channel, just return from the function
	// when the data is over.
	ResultChan chan<- *datasource.ListSplitResult
}

// SplitProvider generates stream of splits - the description of the parts of a large external table
type SplitProvider interface {
	ListSplits(*ListSplitsParams) error
}
