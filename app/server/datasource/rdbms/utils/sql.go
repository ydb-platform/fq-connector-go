package utils

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

type QueryParams struct {
	Ctx       context.Context
	Logger    *zap.Logger
	QueryText string
	QueryArgs *QueryArgs
}

type Connection interface {
	Query(params *QueryParams) (Rows, error)
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

type ConnectionParamsMakeParams struct {
	Ctx                context.Context                 // mandatory
	Logger             *zap.Logger                     // mandatory
	DataSourceInstance *api_common.TDataSourceInstance // mandatory
	TableName          string                          // optional
}

type ConnectionManager interface {
	Make(params *ConnectionParamsMakeParams) (Connection, error)
	Release(ctx context.Context, logger *zap.Logger, connection Connection)
}

type ConnectionManagerBase struct {
	QueryLoggerFactory common.QueryLoggerFactory
}

type SQLFormatterFormatFromParams struct {
	Ctx                context.Context
	Logger             *zap.Logger
	TableName          string
	DataSourceInstance *api_common.TDataSourceInstance
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
	// For some datasources this call may involve queries to external APIs.
	FormatFrom(params *SQLFormatterFormatFromParams) (string, error)
}

type SchemaProvider interface {
	GetSchema(
		ctx context.Context,
		logger *zap.Logger,
		conn Connection,
		dsi *api_common.TDataSourceInstance,
		tableName string,
		typeMappingSettings *api_service_protos.TTypeMappingSettings,
	) (*api_service_protos.TSchema, error)
}
