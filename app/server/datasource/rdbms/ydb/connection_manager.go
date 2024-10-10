package ydb

import (
	"context"
	"fmt"
	"strings"

	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	ydb_sdk_config "github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

type ydbConnection interface {
	rdbms_utils.Connection
	getDriver() *ydb_sdk.Driver
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

	var ydbConn ydbConnection

	switch c.cfg.Mode {
	case config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE:
		ydbConn, err = newConnectionNative(ctx, dsi, ydbDriver)
	case config.TYdbConfig_MODE_TABLE_SERVICE_STDLIB_SCAN_QUERIES:
		ydbConn, err = newConnectionDatabaseSQL(ctx, logger, c.QueryLoggerFactory.Make(logger), c.cfg, dsi, ydbDriver)
	default:
		return nil, fmt.Errorf("unknown mode: %v", c.cfg.Mode)
	}

	if err != nil {
		return nil, fmt.Errorf("new connection: %w", err)
	}

	logger.Debug("Connection is ready")

	return ydbConn, nil
}

func (*connectionManager) Release(ctx context.Context, logger *zap.Logger, conn rdbms_utils.Connection) {
	common.LogCloserError(logger, conn, "close YDB connection")
}

func NewConnectionManager(
	cfg *config.TYdbConfig,
	base rdbms_utils.ConnectionManagerBase,
) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: base, cfg: cfg}
}
