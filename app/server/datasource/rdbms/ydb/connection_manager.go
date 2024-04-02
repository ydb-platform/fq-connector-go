package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_sdk_config "github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*Connection)(nil)

type Connection struct {
	*sql.DB
	logger common.QueryLogger
}

type rows struct {
	*sql.Rows
}

func (r rows) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
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

func (c Connection) Query(ctx context.Context, query string, args ...any) (rdbms_utils.Rows, error) {
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
	endpoint := common.EndpointToString(dsi.Endpoint)
	dsn := sugar.DSN(endpoint, dsi.Database, dsi.UseTls)

	var cred ydb_sdk.Option

	if dsi.Credentials.GetToken() != nil {
		cred = ydb_sdk.WithAccessTokenCredentials(dsi.Credentials.GetToken().Value)
	} else if dsi.Credentials.GetBasic() != nil {
		cred = ydb_sdk.WithStaticCredentials(dsi.Credentials.GetBasic().Username, dsi.Credentials.GetBasic().Password)
	} else {
		cred = ydb_sdk.WithAnonymousCredentials()
	}

	logger.Debug("Trying to open YDB SDK connection", zap.String("dsn", dsn))

	ydbDriver, err := ydb_sdk.Open(ctx, dsn, cred, ydb_sdk.With(ydb_sdk_config.WithGrpcOptions(grpc.WithDisableServiceConfig())))
	if err != nil {
		return nil, fmt.Errorf("open driver error: %w", err)
	}

	ydbConn, err := ydb_sdk.Connector(ydbDriver, ydb_sdk.WithAutoDeclare(), ydb_sdk.WithPositionalArgs())
	if err != nil {
		return nil, fmt.Errorf("connector error: %w", err)
	}

	conn := sql.OpenDB(ydbConn)

	logger.Debug("Pinging database")

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

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

	logger.Debug("Connection is ready")

	queryLogger := c.QueryLoggerFactory.Make(logger)

	return &Connection{DB: conn, logger: queryLogger}, nil
}

func (*connectionManager) Release(logger *zap.Logger, conn rdbms_utils.Connection) {
	common.LogCloserError(logger, conn, "close clickhouse connection")
}

func NewConnectionManager(cfg rdbms_utils.ConnectionManagerBase) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: cfg}
}
