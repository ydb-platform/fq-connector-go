package clickhouse

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.ConnectionManager = (*connectionManager)(nil)

type connectionManager struct {
	rdbms_utils.ConnectionManagerBase
	cfg *config.TClickHouseConfig
}

func (c *connectionManager) Make(
	params *rdbms_utils.ConnectionParams,
) ([]rdbms_utils.Connection, error) {
	if params.DataSourceInstance.GetCredentials().GetBasic() == nil {
		return nil, errors.New("currently only basic auth is supported")
	}

	var (
		conn rdbms_utils.Connection
		err  error
	)

	switch params.DataSourceInstance.Protocol {
	case api_common.EGenericProtocol_NATIVE:
		conn, err = makeConnectionNative(
			params.Ctx, params.Logger, c.cfg, params.DataSourceInstance, params.TableName, c.QueryLoggerFactory.Make(params.Logger))
		if err != nil {
			return nil, fmt.Errorf("make connection native: %w", err)
		}
	case api_common.EGenericProtocol_HTTP:
		conn, err = makeConnectionHTTP(
			params.Ctx, params.Logger, c.cfg, params.DataSourceInstance, params.TableName, c.QueryLoggerFactory.Make(params.Logger))
		if err != nil {
			return nil, fmt.Errorf("make connection http: %w", err)
		}
	default:
		return nil, fmt.Errorf("can not run connection with protocol '%v'", params.DataSourceInstance.Protocol)
	}

	return []rdbms_utils.Connection{conn}, nil
}

func (*connectionManager) Release(_ context.Context, logger *zap.Logger, cs []rdbms_utils.Connection) {
	for _, conn := range cs {
		common.LogCloserError(logger, conn, "close clickhouse connection")
	}
}

func NewConnectionManager(
	cfg *config.TClickHouseConfig,
	base rdbms_utils.ConnectionManagerBase,
) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: base, cfg: cfg}
}
