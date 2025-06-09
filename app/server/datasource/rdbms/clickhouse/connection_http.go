package clickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Rows = (*rows)(nil)

type rows struct {
	*sql.Rows
}

func (r *rows) MakeTransformer(ydbColumns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error) {
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

var _ rdbms_utils.Connection = (*connectionHTTP)(nil)

type connectionHTTP struct {
	*sql.DB
	queryLogger        common.QueryLogger
	dataSourceInstance *api_common.TGenericDataSourceInstance
	tableName          string
}

func (c *connectionHTTP) Query(params *rdbms_utils.QueryParams) (rdbms_utils.Rows, error) {
	c.queryLogger.Dump(params.QueryText, params.QueryArgs.Values()...)

	out, err := c.DB.QueryContext(params.Ctx, params.QueryText, rewriteQueryArgs(params.QueryArgs.Values())...)
	if err != nil {
		return nil, fmt.Errorf("query context: %w", err)
	}

	if err := out.Err(); err != nil {
		defer func() {
			if closeErr := out.Close(); closeErr != nil {
				c.queryLogger.Error("close rows", zap.Error(closeErr))
			}
		}()

		return nil, fmt.Errorf("rows err: %w", err)
	}

	return &rows{Rows: out}, nil
}

func (c *connectionHTTP) DataSourceInstance() *api_common.TGenericDataSourceInstance {
	return c.dataSourceInstance
}

func (c *connectionHTTP) TableName() string {
	return c.tableName
}

func (c *connectionHTTP) Logger() *zap.Logger {
	return c.queryLogger.Logger
}

func makeConnectionHTTP(
	ctx context.Context,
	logger *zap.Logger,
	cfg *config.TClickHouseConfig,
	dsi *api_common.TGenericDataSourceInstance,
	tableName string,
	queryLogger common.QueryLogger,
) (rdbms_utils.Connection, error) {
	opts := &clickhouse.Options{
		Addr: []string{common.EndpointToString(dsi.GetEndpoint())},
		Auth: clickhouse.Auth{
			Database: dsi.Database,
			Username: dsi.Credentials.GetBasic().Username,
			Password: dsi.Credentials.GetBasic().Password,
		},
		// TODO: make it configurable via Connector API
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		// Set this field to true if you want to see ClickHouse driver's debug output
		Debug: false,
		Debugf: func(format string, v ...any) {
			logger.Debug(format, zap.Any("args", v))
		},
		DialTimeout: common.MustDurationFromString(cfg.OpenConnectionTimeout),
		Protocol:    clickhouse.HTTP,
	}

	if dsi.UseTls {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: false,
		}
	}

	conn := clickhouse.OpenDB(opts)

	pingCtx, pingCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(cfg.PingConnectionTimeout))
	defer pingCtxCancel()

	if err := conn.PingContext(pingCtx); err != nil {
		return nil, fmt.Errorf("conn ping: %w", err)
	}

	const (
		maxIdleConns    = 5
		maxOpenConns    = 10
		connMaxLifetime = time.Hour
	)

	conn.SetMaxIdleConns(maxIdleConns)
	conn.SetMaxOpenConns(maxOpenConns)
	conn.SetConnMaxLifetime(connMaxLifetime)

	return &connectionHTTP{
		DB:                 conn,
		queryLogger:        queryLogger,
		dataSourceInstance: dsi,
		tableName:          tableName,
	}, nil
}
