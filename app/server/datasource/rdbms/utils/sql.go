package utils

import (
	"context"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	api_service_protos "github.com/ydb-platform/fq-connector-go/libgo/service/protos"
	"github.com/ydb-platform/fq-connector-go/library/go/core/log"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type Connection interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	Close() error
}

type Rows interface {
	Close() error
	Err() error
	Next() bool
	Scan(dest ...any) error
	MakeTransformer(ydbTypes []*Ydb.Type) (utils.RowTransformer[any], error)
}

type ConnectionManager interface {
	Make(ctx context.Context, logger log.Logger, dataSourceInstance *api_common.TDataSourceInstance) (Connection, error)
	Release(logger log.Logger, connection Connection)
}

type ConnectionManagerBase struct {
	QueryLoggerFactory utils.QueryLoggerFactory
}

type SQLFormatter interface {
	// Get placeholder for n'th argument (starting from 0) for prepared statement
	GetPlaceholder(n int) string

	// Sanitize names of databases, tables, columns, views, schemas
	SanitiseIdentifier(ident string) string

	// Support for high level expression (without subexpressions, they are checked separately)
	SupportsPushdownExpression(expression *api_service_protos.TExpression) bool
}

type SchemaProvider interface {
	GetSchema(
		ctx context.Context,
		logger log.Logger,
		conn Connection,
		request *api_service_protos.TDescribeTableRequest,
	) (*api_service_protos.TSchema, error)
}
