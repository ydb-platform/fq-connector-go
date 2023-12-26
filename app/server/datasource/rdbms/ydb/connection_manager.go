package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
)

var _ rdbms_utils.Connection = (*Connection)(nil)

type Connection struct {
	*sql.DB
	logger utils.QueryLogger
}

type rows struct {
	*sql.Rows
}

func (r rows) MakeTransformer(ydbTypes []*Ydb.Type) (utils.RowTransformer[any], error) {
	columns, err := r.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("column types: %w", err)
	}

	typeNames := make([]string, 0, len(columns))
	for _, column := range columns {
		typeNames = append(typeNames, column.DatabaseTypeName())
	}

	transformer, err := transformerFromSQLTypes(typeNames, ydbTypes)
	if err != nil {
		return nil, fmt.Errorf("transformer from sql types: %w", err)
	}

	return transformer, nil
}

func (c Connection) Query(ctx context.Context, query string, args ...any) (rdbms_utils.Rows, error) {
	c.logger.Dump(query, args...)

	out, err := c.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query context: %w", err)
	}

	if err := out.Err(); err != nil {
		defer func() {
			if err = out.Close(); err != nil {
				c.logger.Error("close rows", zap.Error(err))
			}
		}()

		return nil, fmt.Errorf("rows err: %w", err)
	}

	return rows{Rows: out}, nil
}

var _ rdbms_utils.ConnectionManager = (*connectionManager)(nil)

type connectionManager struct {
	rdbms_utils.ConnectionManagerBase
	// TODO: cache of connections, remove unused connections with TTL
}

func (c *connectionManager) Make(
	ctx context.Context,
	logger *zap.Logger,
	dsi *api_common.TDataSourceInstance,
) (rdbms_utils.Connection, error) {
	// TODO: add credentials (iam and basic) support
	endpoint := utils.EndpointToString(dsi.Endpoint)
	dsn := sugar.DSN(endpoint, dsi.Database, dsi.UseTls)

	ydbDriver, err := ydb_sdk.Open(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open driver error: %w", err)
	}

	ydbConn, err := ydb_sdk.Connector(ydbDriver)
	if err != nil {
		return nil, fmt.Errorf("connector error: %w", err)
	}

	conn := sql.OpenDB(ydbConn)

	const (
		maxIdleConns    = 5
		maxOpenConns    = 10
		connMaxLifetime = time.Hour
	)

	conn.SetMaxIdleConns(maxIdleConns)
	conn.SetMaxOpenConns(maxOpenConns)
	conn.SetConnMaxLifetime(connMaxLifetime)

	queryLogger := c.QueryLoggerFactory.Make(logger)

	return &Connection{DB: conn, logger: queryLogger}, nil
}

func (c *connectionManager) Release(logger *zap.Logger, conn rdbms_utils.Connection) {
	utils.LogCloserError(logger, conn, "close clickhouse connection")
}

func NewConnectionManager(cfg rdbms_utils.ConnectionManagerBase) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: cfg}
}
