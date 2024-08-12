package ms_sql_server

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.ConnectionManager = (*connectionManager)(nil)

type connectionManager struct {
	rdbms_utils.ConnectionManagerBase
	// TODO: cache of connections, remove unused connections with TTL
}

func (c *connectionManager) Make(
	ctx context.Context,
	logger *zap.Logger,
	dsi *api_common.TDataSourceInstance,
) (rdbms_utils.Connection, error) {
	if dsi.Protocol != api_common.EProtocol_NATIVE {
		return nil, fmt.Errorf("can not create MS SQL Server connection with protocol '%v'", dsi.Protocol)
	}

	connectString := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s",
		dsi.Credentials.GetBasic().GetUsername(),
		dsi.Credentials.GetBasic().GetPassword(),
		dsi.GetEndpoint().GetHost(),
		uint16(dsi.GetEndpoint().GetPort()),
		dsi.Database)

	// TODO: add tls
	// if dsi.UseTls {
	// 	connectString += "&encrypt=true&trustServerCertificate=true"
	// } else {
	// 	connectString += "&encrypt=false"
	// }

	db, err := sql.Open("sqlserver", connectString)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	pingCtx, pingCtxCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCtxCancel()

	err = db.PingContext(pingCtx)
	if err != nil {
		defer db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	queryLogger := c.QueryLoggerFactory.Make(logger)

	return &Connection{db, queryLogger}, nil
}

func (*connectionManager) Release(logger *zap.Logger, conn rdbms_utils.Connection) {
	common.LogCloserError(logger, conn, "close connection")
}

func NewConnectionManager(cfg rdbms_utils.ConnectionManagerBase) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: cfg}
}
