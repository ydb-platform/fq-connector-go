package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"
)

type rowsDatabaseSql struct {
	*sql.Rows
}

func (r rowsDatabaseSql) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	columns, err := r.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("column types: %w", err)
	}

	typeNames := make([]string, 0, len(columns))
	for _, column := range columns {
		typeNames = append(typeNames, column.DatabaseTypeName())
	}

	transformer, err := transformerFromSQLTypes(typeNames, ydbTypes, cc)
	if err != nil {
		return nil, fmt.Errorf("transformer from sql types: %w", err)
	}

	return transformer, nil
}

var _ rdbms_utils.Connection = (*connectionDatabaseSql)(nil)

type connectionDatabaseSql struct {
	*sql.DB
	driver *ydb_sdk.Driver
	logger common.QueryLogger
}

func (c *connectionDatabaseSql) Query(ctx context.Context, _ *zap.Logger, query string, args ...any) (rdbms_utils.Rows, error) {
	c.logger.Dump(query, args...)

	out, err := c.DB.QueryContext(ydb_sdk.WithQueryMode(ctx, ydb_sdk.ScanQueryMode), query, args...)
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

	return rowsDatabaseSql{Rows: out}, nil
}

func (c *connectionDatabaseSql) Close() error {
	err1 := c.DB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err2 := c.driver.Close(ctx)

	if err1 != nil || err2 != nil {
		return fmt.Errorf("connection close err: %w; driver close err: %w", err1, err2)
	}

	return nil
}

func newConnectionDatabaseSQL(
	ctx context.Context,
	logger *zap.Logger,
	queryLogger common.QueryLogger,
	cfg *config.TYdbConfig,
	dsi *api_common.TDataSourceInstance,
	ydbDriver *ydb_sdk.Driver,
) (rdbms_utils.Connection, error) {
	ydbConn, err := ydb_sdk.Connector(
		ydbDriver,
		ydb_sdk.WithAutoDeclare(),
		ydb_sdk.WithPositionalArgs(),
		ydb_sdk.WithTablePathPrefix(dsi.Database),
	)

	if err != nil {
		return nil, fmt.Errorf("connector error: %w", err)
	}
	conn := sql.OpenDB(ydbConn)

	logger.Debug("Pinging database")

	pingCtx, pingCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(cfg.PingConnectionTimeout))
	defer pingCtxCancel()

	if err := conn.PingContext(pingCtx); err != nil {
		common.LogCloserError(logger, conn, "close YDB connection")
		return nil, fmt.Errorf("conn ping: %w", err)
	}

	logger.Debug("Connection is ready")

	return &connectionDatabaseSql{DB: conn, driver: ydbDriver, logger: queryLogger}, nil
}
