package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	ydb_sdk_config "github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*Connection)(nil)

type Connection struct {
	*sql.DB
	driver *ydb_sdk.Driver
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

func (c *Connection) Query(ctx context.Context, _ *zap.Logger, query string, args ...any) (rdbms_utils.Rows, error) {
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

func (c *Connection) Close() error {
	err1 := c.DB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err2 := c.driver.Close(ctx)

	if err1 != nil || err2 != nil {
		return fmt.Errorf("connection close err: %w; driver close err: %w", err1, err2)
	}

	return nil
}

var _ rdbms_utils.ConnectionManager = (*connectionManager)(nil)

type connectionManager struct {
	rdbms_utils.ConnectionManagerBase
	cfg *config.TYdbConfig
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

	openCtx, openCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(c.cfg.OpenConnectionTimeout))
	defer openCtxCancel()

	ydbOptions := []ydb_sdk.Option{
		cred,
		ydb_sdk.WithDialTimeout(common.MustDurationFromString(c.cfg.OpenConnectionTimeout)),
		ydb_sdk.WithBalancer(balancers.SingleConn()), // see YQ-3089
		ydb_sdk.With(ydb_sdk_config.WithGrpcOptions(grpc.WithDisableServiceConfig())),
	}

	// `u-` prefix is an implicit indicator of a dedicated YDB database.
	// We have to use underlay networks when accessing this kind of database in cloud,
	// so we add this prefix to every endpoint discovered.
	if c.cfg.UseUnderlayNetworkForDedicatedDatabases && strings.HasPrefix(endpoint, "u-") {
		ydbOptions = append(ydbOptions, ydb_sdk.WithNodeAddressMutator(
			func(address string) string {
				return "u-" + address
			},
		))
	}

	ydbDriver, err := ydb_sdk.Open(openCtx, dsn, ydbOptions...)
	if err != nil {
		return nil, fmt.Errorf("open driver error: %w", err)
	}

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

	pingCtx, pingCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(c.cfg.PingConnectionTimeout))
	defer pingCtxCancel()

	if err := conn.PingContext(pingCtx); err != nil {
		common.LogCloserError(logger, conn, "close YDB connection")
		return nil, fmt.Errorf("conn ping: %w", err)
	}

	logger.Debug("Connection is ready")

	queryLogger := c.QueryLoggerFactory.Make(logger)

	return &Connection{DB: conn, driver: ydbDriver, logger: queryLogger}, nil
}

func (*connectionManager) Release(_ context.Context, logger *zap.Logger, conn rdbms_utils.Connection) {
	common.LogCloserError(logger, conn, "close YDB connection")
}

func NewConnectionManager(
	cfg *config.TYdbConfig,
	base rdbms_utils.ConnectionManagerBase,
) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: base, cfg: cfg}
}
