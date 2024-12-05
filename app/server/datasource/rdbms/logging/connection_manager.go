package logging

import (
	"context"
	"fmt"

	"github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"

	"go.uber.org/zap"
)

type connectionManager struct {
	rdbms_utils.Connection
	resolver             resolver
	ydbConnectionManager rdbms_utils.ConnectionManager
}

func (cm *connectionManager) Make(
	params *rdbms_utils.ConnectionParams,
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

	// prepare new data source instance describing the underlying YDB database
	ydbDataSourceInstance := &common.TDataSourceInstance{
		Kind:        common.EDataSourceKind_YDB,
		Endpoint:    response.endpoint,
		Database:    response.databaseName,
		Credentials: params.DataSourceInstance.GetCredentials(),
		UseTls:      true,
	}

	ydbParams := &rdbms_utils.ConnectionParams{
		Ctx:                params.Ctx,
		Logger:             params.Logger,
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
) (rdbms_utils.ConnectionManager, error) {
	r, err := newResolver(cfg)
	if err != nil {
		return nil, fmt.Errorf("new resolver: %w", err)
	}

	return &connectionManager{
		ydbConnectionManager: ydb.NewConnectionManager(cfg.Ydb, base),
		resolver:             r,
	}, nil
}
