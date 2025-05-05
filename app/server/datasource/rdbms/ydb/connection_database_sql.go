package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

type rowsDatabaseSQL struct {
	*sql.Rows
}

func (r rowsDatabaseSQL) MakeTransformer(ydbColumns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error) {
	columns, err := r.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("column types: %w", err)
	}

	typeNames := make([]string, 0, len(columns))
	for _, column := range columns {
		typeNames = append(typeNames, column.DatabaseTypeName())
	}

	transformer, err := transformerFromSQLTypes(typeNames, common.YDBColumnsToYDBTypes(ydbColumns), cc)
	if err != nil {
		return nil, fmt.Errorf("transformer from sql types: %w", err)
	}

	return transformer, nil
}

var _ rdbms_utils.Connection = (*connectionDatabaseSQL)(nil)

type connectionDatabaseSQL struct {
	*sql.DB
	driver             *ydb_sdk.Driver
	queryLogger        common.QueryLogger
	dataSourceInstance *api_common.TGenericDataSourceInstance
	tableName          string
}

func (c *connectionDatabaseSQL) Query(params *rdbms_utils.QueryParams) (rdbms_utils.Rows, error) {
	c.queryLogger.Dump(params.QueryText, params.QueryArgs.Values()...)

	out, err := c.DB.QueryContext(
		ydb_sdk.WithQueryMode(params.Ctx, ydb_sdk.ScanQueryMode),
		params.QueryText,
		params.QueryArgs.Values()...)
	if err != nil {
		return nil, fmt.Errorf("query context: %w", err)
	}

	if err := out.Err(); err != nil {
		defer func() {
			if err = out.Close(); err != nil {
				c.queryLogger.Error("close rows", zap.Error(err))
			}
		}()

		return nil, fmt.Errorf("rows err: %w", err)
	}

	return rowsDatabaseSQL{Rows: out}, nil
}

func (c *connectionDatabaseSQL) Driver() *ydb_sdk.Driver {
	return c.driver
}

func (c *connectionDatabaseSQL) DataSourceInstance() *api_common.TGenericDataSourceInstance {
	return c.dataSourceInstance
}

func (c *connectionDatabaseSQL) TableName() string {
	return c.tableName
}

func (c *connectionDatabaseSQL) Close() error {
	err1 := c.DB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err2 := c.driver.Close(ctx)

	if err1 != nil || err2 != nil {
		return fmt.Errorf("connection close err: %w; driver close err: %w", err1, err2)
	}

	return nil
}

func (c *connectionDatabaseSQL) Logger() *zap.Logger {
	return c.queryLogger.Logger
}

func newConnectionDatabaseSQL(
	ctx context.Context,
	logger *zap.Logger,
	queryLogger common.QueryLogger,
	cfg *config.TYdbConfig,
	dsi *api_common.TGenericDataSourceInstance,
	tableName string,
	ydbDriver *ydb_sdk.Driver,
) (Connection, error) {
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

	logger.Debug("pinging database")

	pingCtx, pingCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(cfg.PingConnectionTimeout))
	defer pingCtxCancel()

	if err := conn.PingContext(pingCtx); err != nil {
		common.LogCloserError(logger, conn, "close YDB connection")
		return nil, fmt.Errorf("conn ping: %w", err)
	}

	return &connectionDatabaseSQL{
		DB:                 conn,
		driver:             ydbDriver,
		queryLogger:        queryLogger,
		dataSourceInstance: dsi,
		tableName:          tableName}, nil
}
