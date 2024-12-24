package logging

import (
	"context"
	"fmt"

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

	params.Logger.Debug("Resolved log group into YDB endpoint", response.ToZapFields()...)

	// prepare new data source instance describing the underlying YDB database
	ydbDataSourceInstance := &api_common.TGenericDataSourceInstance{
		Kind:        api_common.EGenericDataSourceKind_YDB,
		Endpoint:    response.endpoint,
		Database:    response.databaseName,
		Credentials: params.DataSourceInstance.GetCredentials(),
		UseTls:      true,
	}

	// reannotate logger with new data source instance
	ydbLogger := common.AnnotateLoggerWithDataSourceInstance(params.Logger, ydbDataSourceInstance)

	ydbParams := &rdbms_utils.ConnectionManagerMakeParams{
		Ctx:                params.Ctx,
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
