package logging

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"github.com/ydb-platform/fq-connector-go/common"

	"go.uber.org/zap"
)

type connectionManager struct {
	rdbms_utils.Connection
	ydbConnectionManager rdbms_utils.ConnectionManager
}

func (cm *connectionManager) Make(
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

	cs, err := cm.makeConnectionFromYDBSource(params, splitDescription.GetYdb())
	if err != nil {
		return nil, fmt.Errorf("make connection from YDB source: %w", err)
	}

	return cs, nil
}

func (cm *connectionManager) makeConnectionFromYDBSource(
	params *rdbms_utils.ConnectionParams,
	src *TSplitDescription_TYdb,
) ([]rdbms_utils.Connection, error) {
	// prepare new data source instance describing the underlying YDB database
	dsi := &api_common.TGenericDataSourceInstance{
		Kind:     api_common.EGenericDataSourceKind_YDB,
		Endpoint: src.Endpoint,
		Database: src.DatabaseName,
		// No need to fill credentials as special file token will be used by connection manager
		Credentials: nil,
		UseTls:      true,
	}

	// reannotate logger with new data source instance
	ydbLogger := common.AnnotateLoggerWithDataSourceInstance(params.Logger, dsi)

	cs, err := cm.ydbConnectionManager.Make(&rdbms_utils.ConnectionParams{
		Ctx:                params.Ctx,
		Logger:             ydbLogger,
		DataSourceInstance: dsi,              // use resolved YDB database
		TableName:          params.TableName, // use resolved YDB table
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
) rdbms_utils.ConnectionManager {
	return &connectionManager{
		ydbConnectionManager: ydb.NewConnectionManager(cfg.Ydb, base),
	}
}
