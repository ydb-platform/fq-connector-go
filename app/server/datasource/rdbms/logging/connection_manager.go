package logging

import (
	"context"
	"fmt"
	"sync"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"github.com/ydb-platform/fq-connector-go/common"
	"golang.org/x/sync/errgroup"

	"go.uber.org/zap"
)

var _ rdbms_utils.Connection = (*connection)(nil)

type connection struct {
	ydbConns []rdbms_utils.Connection
}

func (c *connection) Query(params *rdbms_utils.QueryParams) ([]rdbms_utils.Rows, error) {
	group := errgroup.Group{}

	var (
		rows  = make([]rdbms_utils.Rows, 0, len(c.ydbConns))
		mutex sync.Mutex
	)

	for _, ydbConn := range c.ydbConns {
		ydbConn := ydbConn

		group.Go(func() error {
			rs, err := ydbConn.Query(params)
			if err != nil {
				return fmt.Errorf("YDB connection query: %w", err)
			}

			mutex.Lock()
			defer mutex.Unlock()
			rows = append(rows, rs...)

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		for _, row := range rows {
			if err := row.Close(); err != nil {
				params.Logger.Error("close row", zap.Error(err))
			}
		}

		return nil, fmt.Errorf("group wait: %w", err)
	}

	return rows, nil
}

func (c *connection) Close() error {
	group := errgroup.Group{}

	for _, ydbConn := range c.ydbConns {
		ydbConn := ydbConn

		group.Go(func() error {
			return ydbConn.Close()
		})
	}

	return group.Wait()
}

var _ rdbms_utils.ConnectionManager = (*connectionManager)(nil)

type connectionManager struct {
	rdbms_utils.Connection
	resolver             Resolver
	ydbConnectionManager rdbms_utils.ConnectionManager
}

func (cm *connectionManager) Make(
	params *rdbms_utils.ConnectionParamsMakeParams,
) (rdbms_utils.Connection, error) {
	// turn log group name into YDB endpoint
	request := &resolveParams{
		folderId:     params.DataSourceInstance.GetLoggingOptions().GetFolderId(),
		logGroupName: params.TableName,
	}

	response, err := cm.resolver.resolve(request)
	if err != nil {
		return nil, fmt.Errorf("resolve YDB endpoint: %w", err)
	}

	var ydbConns []rdbms_utils.Connection

	for _, source := range response.sources {
		ydbConn, err := cm.makeConnectionFromYDBSource(params.Ctx, params.Logger, source)
		if err != nil {
			for _, ydbConn := range ydbConns {
				if err := ydbConn.Close(); err != nil {
					params.Logger.Error("close YDB connection", zap.Error(err))
				}
			}

			return nil, fmt.Errorf("make connection for YDB source: %w", err)
		}

		ydbConns = append(ydbConns, ydbConn)
	}

	return &connection{ydbConns: ydbConns}, nil
}

func (cm *connectionManager) makeConnectionFromYDBSource(
	ctx context.Context,
	logger *zap.Logger,
	source *ydbSource,
) (rdbms_utils.Connection, error) {
	logger.Debug("Resolved log group into YDB endpoint", source.ToZapFields()...)

	// prepare new data source instance describing the underlying YDB database
	ydbDataSourceInstance := &api_common.TGenericDataSourceInstance{
		Kind:     api_common.EGenericDataSourceKind_YDB,
		Endpoint: source.endpoint,
		Database: source.databaseName,
		// We set no credentials provided by user, because YDB connector accessing Logging database
		// should use own service account credentials.
		Credentials: nil,
		UseTls:      true,
	}

	// reannotate logger with new data source instance
	ydbLogger := common.AnnotateLoggerWithDataSourceInstance(logger, ydbDataSourceInstance)

	ydbParams := &rdbms_utils.ConnectionParamsMakeParams{
		Ctx:                ctx,
		Logger:             ydbLogger,
		DataSourceInstance: ydbDataSourceInstance,
	}

	// build YDB connection
	conn, err := cm.ydbConnectionManager.Make(ydbParams)
	if err != nil {
		return nil, fmt.Errorf("make YDB connection: %w", err)
	}

	return conn, nil
}

func (cm *connectionManager) Release(ctx context.Context, logger *zap.Logger, conn rdbms_utils.Connection) {
	cm.ydbConnectionManager.Release(ctx, logger, conn)
}

func NewConnectionManager(
	cfg *config.TLoggingConfig,
	base rdbms_utils.ConnectionManagerBase,
	resolver Resolver,
) rdbms_utils.ConnectionManager {
	return &connectionManager{
		ydbConnectionManager: ydb.NewConnectionManager(cfg.Ydb, base),
		resolver:             resolver,
	}
}
