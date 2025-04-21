package logging

import (
	"context"
	"fmt"
	"math/rand"

	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"github.com/ydb-platform/fq-connector-go/common"
)

type connectionManager struct {
	rdbms_utils.Connection
	ydbConnectionManager rdbms_utils.ConnectionManager
	resolver             Resolver
}

func (cm *connectionManager) Make(
	params *rdbms_utils.ConnectionParams,
) ([]rdbms_utils.Connection, error) {
	switch params.QueryPhase {
	case rdbms_utils.QueryPhaseDescribeTable, rdbms_utils.QueryPhaseListSplits:
		cs, err := cm.makeConnectionForMetadata(params)
		if err != nil {
			return nil, fmt.Errorf("make connection for metadata: %w", err)
		}

		return cs, nil
	case rdbms_utils.QueryPhaseReadSplits:
		cs, err := cm.makeConnectionForData(params)
		if err != nil {
			return nil, fmt.Errorf("make connection to read split: %w", err)
		}

		return cs, nil
	default:
		return nil, fmt.Errorf("unknown query phase: %v", params.QueryPhase)
	}
}

func (cm *connectionManager) makeConnectionForMetadata(
	params *rdbms_utils.ConnectionParams,
) ([]rdbms_utils.Connection, error) {
	// Turn log group name into physical YDB endpoints
	// via static config or Cloud Logging API call.
	request := &resolveRequest{
		ctx:          params.Ctx,
		logger:       params.Logger,
		folderId:     params.DataSourceInstance.GetLoggingOptions().GetFolderId(),
		logGroupName: params.TableName,
		credentials:  params.DataSourceInstance.GetCredentials(),
	}

	response, err := cm.resolver.resolve(request)
	if err != nil {
		return nil, fmt.Errorf("resolve YDB endpoint: %w", err)
	}

	// Get exactly one
	rand.Shuffle(len(response.sources), func(i, j int) {
		response.sources[i], response.sources[j] = response.sources[j], response.sources[i]
	})

	src := response.sources[0]

	params.Logger.Info("resolved log group into YDB endpoint", src.ToZapFields()...)

	// prepare new data source instance describing the underlying YDB database
	dsi := &api_common.TGenericDataSourceInstance{
		Kind:        api_common.EGenericDataSourceKind_YDB,
		Endpoint:    src.endpoint,
		Database:    src.databaseName,
		Credentials: src.credentials, // may be overridden by other settings
		UseTls:      true,
	}

	cs, err := cm.makeConnectionFromDataSourceInstance(params, dsi, src.tableName)
	if err != nil {
		return nil, fmt.Errorf("make connection from data source instance: %w", err)
	}

	return cs, nil
}

func (cm *connectionManager) makeConnectionForData(
	params *rdbms_utils.ConnectionParams,
) ([]rdbms_utils.Connection, error) {
	// Deserialize split description
	var (
		splitDescription TSplitDescription
		err              error
	)

	if err = protojson.Unmarshal(params.Split.GetDescription(), &splitDescription); err != nil {
		return nil, fmt.Errorf("unmarshal split description: %w", err)
	}

	// currently OLAP YDB is the only backend
	src := splitDescription.GetYdb()

	// prepare new data source instance describing the underlying YDB database
	dsi := &api_common.TGenericDataSourceInstance{
		Kind:     api_common.EGenericDataSourceKind_YDB,
		Endpoint: src.Endpoint,
		Database: src.DatabaseName,
		// No need to fill credentials because special SA auth file will be used by connection manager.
		Credentials: nil,
		UseTls:      true,
	}

	cs, err := cm.makeConnectionFromDataSourceInstance(params, dsi, src.TableName)
	if err != nil {
		return nil, fmt.Errorf("make connection from data source instance: %w", err)
	}

	return cs, nil
}

func (cm *connectionManager) makeConnectionFromDataSourceInstance(
	params *rdbms_utils.ConnectionParams,
	dsi *api_common.TGenericDataSourceInstance,
	tableName string,
) ([]rdbms_utils.Connection, error) {
	// reannotate logger with new data source instance
	ydbLogger := common.AnnotateLoggerWithDataSourceInstance(params.Logger, dsi)

	cs, err := cm.ydbConnectionManager.Make(&rdbms_utils.ConnectionParams{
		Ctx:                params.Ctx,
		Logger:             ydbLogger,
		DataSourceInstance: dsi,
		TableName:          tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("make YDB connection: %w", err)
	}

	if len(cs) != 1 {
		return nil, fmt.Errorf("invalid number of YDB connections: %d", len(cs))
	}

	return cs, nil
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
