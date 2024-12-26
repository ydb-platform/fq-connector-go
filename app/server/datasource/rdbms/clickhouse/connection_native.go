package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*connectionNative)(nil)

type connectionNative struct {
	driver.Conn
	logger       common.QueryLogger
	databaseName string
	tableName    string
}

var _ rdbms_utils.Rows = (*rowsNative)(nil)

type rowsNative struct {
	driver.Rows
}

func (rowsNative) NextResultSet() bool {
	return false
}

func (r *rowsNative) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	columns := r.ColumnTypes()

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

func (c *connectionNative) Query(params *rdbms_utils.QueryParams) (rdbms_utils.Rows, error) {
	c.logger.Dump(params.QueryText, params.QueryArgs.Values()...)

	out, err := c.Conn.Query(params.Ctx, params.QueryText, params.QueryArgs.Values()...)
	if err != nil {
		return nil, fmt.Errorf("query context: %w", err)
	}

	if err := out.Err(); err != nil {
		defer func() {
			if closeErr := out.Close(); closeErr != nil {
				c.logger.Error("close rows", zap.Error(closeErr))
			}
		}()

		return nil, fmt.Errorf("rows err: %w", err)
	}

	return &rowsNative{Rows: out}, nil
}

func (c *connectionNative) From() (databaseName, tableName string) {
	return c.databaseName, c.tableName
}

func makeConnectionNative(
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
		Protocol:    clickhouse.Native,
	}

	if dsi.UseTls {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: false,
		}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}

	pingCtx, pingCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(cfg.PingConnectionTimeout))
	defer pingCtxCancel()

	if err := conn.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("conn ping: %w", err)
	}

	return &connectionNative{Conn: conn, logger: queryLogger, databaseName: dsi.Database, tableName: tableName}, nil
}
