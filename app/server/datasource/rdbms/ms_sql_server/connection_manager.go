package ms_sql_server

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.ConnectionManager = (*connectionManager)(nil)

type connectionManager struct {
	rdbms_utils.ConnectionManagerBase
	cfg *config.TMsSQLServerConfig
}

func (c *connectionManager) Make(
	params *rdbms_utils.ConnectionParamsMakeParams,
) (rdbms_utils.Connection, error) {
	dsi, ctx, logger := params.DataSourceInstance, params.Ctx, params.Logger

	if dsi.Protocol != api_common.EProtocol_NATIVE {
		return nil, fmt.Errorf("can not create MS SQL Server connection with protocol '%v'", dsi.Protocol)
	}

	openConnectionTimeout := int(common.MustDurationFromString(c.cfg.OpenConnectionTimeout).Seconds())

	connectString := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&connection+timeout=%d",
		dsi.Credentials.GetBasic().GetUsername(),
		dsi.Credentials.GetBasic().GetPassword(),
		dsi.GetEndpoint().GetHost(),
		uint16(dsi.GetEndpoint().GetPort()),
		dsi.Database,
		openConnectionTimeout,
	)

	if dsi.UseTls {
		connectString += "&encrypt=true&trustServerCertificate=false"
	} else {
		connectString += "&encrypt=disable"
	}

	db, err := sql.Open("sqlserver", connectString)
	if err != nil {
		return nil, fmt.Errorf("sql open: %w", err)
	}

	pingCtx, pingCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(c.cfg.PingConnectionTimeout))
	defer pingCtxCancel()

	err = db.PingContext(pingCtx)
	if err != nil {
		common.LogCloserError(logger, db, "close connection")
		return nil, fmt.Errorf("ping: %w", err)
	}

	queryLogger := c.QueryLoggerFactory.Make(logger)

	return &Connection{db, queryLogger}, nil
}

func (*connectionManager) Release(_ context.Context, logger *zap.Logger, conn rdbms_utils.Connection) {
	common.LogCloserError(logger, conn, "close connection")
}

func NewConnectionManager(
	cfg *config.TMsSQLServerConfig,
	base rdbms_utils.ConnectionManagerBase) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: base, cfg: cfg}
}
