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
	// DataSourceInstance comprehensively describing the target of the connection
	DataSourceInstance() *api_common.TGenericDataSourceInstance
	// The name of a table that will be read via this connection.
	TableName() string
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
	MakeTransformer(columns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error)
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
	// because different table splits can require the connection different database instances.
	Split *api_service_protos.TSplit // optional

	// For certain data sources this query prefix may be placed into each request.
	// This could be useful for making the request
	QueryPrefix string // optional
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
	// Checks support for expression rendering
	SupportsExpression(expression *api_service_protos.TExpression) bool
	// Checks support for `left LIKE "right%"` predicate pushdown
	FormatStartsWith(left, right string) (string, error)
	// Checks support for `left LIKE "%right"` predicate pushdown
	FormatEndsWith(left, right string) (string, error)
	// Checks support for `left LIKE "%right%"` predicate pushdown
	FormatContains(left, right string) (string, error)
	// FormatWhat builds a substring containing the SELECT clause.
	FormatWhat(src *api_service_protos.TSelect_TWhat) (string, error)
	// FormatFrom builds a substring containing the literals
	// that must be placed after FROM (`SELECT ... FROM <this>`).
	FormatFrom(tableName string) string
	// RenderSelectQueryText composes final query text from the given clauses.
	// Particular implementation may mix-in some additional parts into the query.
	RenderSelectQueryText(parts *SelectQueryParts, split *api_service_protos.TSplit) (string, error)
	// TransformPredicateComparison transforms the comparison predicate
	// (may be useful for some special data sources)
	TransformPredicateComparison(src *api_service_protos.TPredicate_TComparison) (
		*api_service_protos.TPredicate_TComparison, error)
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
