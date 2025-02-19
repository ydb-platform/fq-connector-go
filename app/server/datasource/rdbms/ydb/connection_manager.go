package ydb

import (
	"context"
	"fmt"
	"strings"

	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_balancers "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	ydb_sdk_config "github.com/ydb-platform/ydb-go-sdk/v3/config"
	ydb_sugar "github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	yc "github.com/ydb-platform/ydb-go-yc"
	"go.uber.org/zap"
	"google.golang.org/grpc"

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
	params *rdbms_utils.ConnectionParams,
) ([]rdbms_utils.Connection, error) {
	dsi, ctx, logger := params.DataSourceInstance, params.Ctx, params.Logger
	endpoint := common.EndpointToString(dsi.Endpoint)
	dsn := ydb_sugar.DSN(endpoint, dsi.Database, ydb_sugar.WithSecure(dsi.UseTls))

	var cred ydb_sdk.Option

	if c.cfg.ServiceAccountKeyFileCredentials != "" {
		logger.Debug(
			"connector will use service account key file credentials for authorization",
			zap.String("path", c.cfg.ServiceAccountKeyFileCredentials),
		)

		cred = yc.WithServiceAccountKeyFileCredentials(
			c.cfg.ServiceAccountKeyFileCredentials,
			yc.WithEndpoint(common.EndpointToString(c.cfg.IamEndpoint)),
		)
	} else if dsi.Credentials.GetToken() != nil {
		logger.Debug("connector will use token for authorization")

		cred = ydb_sdk.WithAccessTokenCredentials(dsi.Credentials.GetToken().Value)
	} else if dsi.Credentials.GetBasic() != nil {
		logger.Debug("connector will use base auth credentials for authorization")

		cred = ydb_sdk.WithStaticCredentials(dsi.Credentials.GetBasic().Username, dsi.Credentials.GetBasic().Password)
	} else {
		logger.Warn("connector will not use any credentials for authorization")

		cred = ydb_sdk.WithAnonymousCredentials()
	}

	logger.Debug("trying to open YDB SDK connection", zap.String("dsn", dsn))

	openCtx, openCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(c.cfg.OpenConnectionTimeout))
	defer openCtxCancel()

	ydbOptions := []ydb_sdk.Option{
		cred,
		ydb_sdk.WithDialTimeout(common.MustDurationFromString(c.cfg.OpenConnectionTimeout)),
		ydb_sdk.WithBalancer(ydb_balancers.SingleConn()), // see YQ-3089
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
	case config.TYdbConfig_MODE_UNSPECIFIED:
		fallthrough
	case config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE:
		logger.Debug("connector will use Native SDK over Query Service")

		formatter := NewSQLFormatter(config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE, c.cfg.Pushdown)
		ydbConn = newConnectionNative(ctx, c.QueryLoggerFactory.Make(logger), dsi, params.TableName, ydbDriver, formatter)
	case config.TYdbConfig_MODE_TABLE_SERVICE_STDLIB_SCAN_QUERIES:
		logger.Debug("connector will use database/sql SDK with scan queries over Table Service")
		ydbConn, err = newConnectionDatabaseSQL(ctx, logger, c.QueryLoggerFactory.Make(logger), c.cfg, dsi, params.TableName, ydbDriver)
	default:
		return nil, fmt.Errorf("unknown mode: %v", c.cfg.Mode)
	}

	if err != nil {
		return nil, fmt.Errorf("new connection: %w", err)
	}

	logger.Debug("connection is ready")

	return []rdbms_utils.Connection{ydbConn}, nil
}

func (*connectionManager) Release(_ context.Context, logger *zap.Logger, cs []rdbms_utils.Connection) {
	for _, conn := range cs {
		common.LogCloserError(logger, conn, "close YDB connection")
	}
}

func NewConnectionManager(
	cfg *config.TYdbConfig,
	base rdbms_utils.ConnectionManagerBase,
) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: base, cfg: cfg}
}
