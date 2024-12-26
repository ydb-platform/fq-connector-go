package logging

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"github.com/ydb-platform/fq-connector-go/common"

	"go.uber.org/zap"
)

type connectionManager struct {
	rdbms_utils.Connection
	resolver             Resolver
	ydbConnectionManager rdbms_utils.ConnectionManager
}

func (cm *connectionManager) Make(
	params *rdbms_utils.ConnectionManagerMakeParams,
) ([]rdbms_utils.Connection, error) {
	// Turn log group name into physical YDB endpoints
	// via static config or Logging API call.
	request := &resolveParams{
		ctx:          params.Ctx,
		logger:       params.Logger,
		folderId:     params.DataSourceInstance.GetLoggingOptions().GetFolderId(),
		logGroupName: params.TableName,
		iamToken:     params.DataSourceInstance.GetCredentials().GetToken().GetValue(),
	}

	response, err := cm.resolver.resolve(request)
	if err != nil {
		return nil, fmt.Errorf("resolve YDB endpoint: %w", err)
	}

	// Determine how much connections we need to create
	// taking into account optional limit.
	totalConnections := len(response.sources)
	if params.MaxConnections > 0 && params.MaxConnections < totalConnections {
		totalConnections = params.MaxConnections
	}

	var (
		group errgroup.Group
		cs    = make([]rdbms_utils.Connection, 0, totalConnections)
		mutex sync.Mutex
	)

	for i, src := range response.sources {
		// If connection limit is set, create only requested number of connections.
		if i >= totalConnections {
			break
		}

		src := src

		group.Go(func() error {
			conn, err := cm.makeConnectionFromYDBSource(params, src)
			if err != nil {
				return fmt.Errorf("make connection from YDB source: %w", err)
			}

			mutex.Lock()
			cs = append(cs, conn)
			mutex.Unlock()

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		for _, conn := range cs {
			if conn != nil {
				common.LogCloserError(params.Logger, conn, "close connection")
			}
		}

		return nil, fmt.Errorf("group wait: %w", err)
	}

	return cs, nil
}

func (cm *connectionManager) makeConnectionFromYDBSource(
	params *rdbms_utils.ConnectionManagerMakeParams,
	src *ydbSource,
) (rdbms_utils.Connection, error) {
	params.Logger.Debug("resolved log group into YDB endpoint", src.ToZapFields()...)

	// prepare new data source instance describing the underlying YDB database
	ydbDataSourceInstance := &api_common.TGenericDataSourceInstance{
		Kind:        api_common.EGenericDataSourceKind_YDB,
		Endpoint:    src.endpoint,
		Database:    src.databaseName,
		Credentials: nil,
		UseTls:      true,
	}

	// reannotate logger with new data source instance
	ydbLogger := common.AnnotateLoggerWithDataSourceInstance(params.Logger, ydbDataSourceInstance)

	conn, err := cm.ydbConnectionManager.Make(&rdbms_utils.ConnectionManagerMakeParams{
		Ctx:                params.Ctx,
		Logger:             ydbLogger,
		DataSourceInstance: ydbDataSourceInstance, // use resolved YDB database
		TableName:          src.tableName,         // use resolved YDB table
	})
	if err != nil {
		return nil, fmt.Errorf("make YDB connection: %w", err)
	}

	if len(conn) != 1 {
		return nil, fmt.Errorf("invalid number of YDB connections: %d", len(conn))
	}

	return conn[0], nil
}

func (cm *connectionManager) Release(ctx context.Context, logger *zap.Logger, cs []rdbms_utils.Connection) {
	cm.ydbConnectionManager.Release(ctx, logger, cs)
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
