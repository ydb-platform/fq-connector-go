package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/godror/godror"
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
		return nil, fmt.Errorf("can not create Oracle connection with protocol '%v'", dsi.Protocol)
	}

	var err error

	// // godror
	var connParams godror.ConnectionParams

	connParams.Username = dsi.Credentials.GetBasic().GetUsername()
	connParams.Password = godror.NewPassword(dsi.Credentials.GetBasic().GetPassword())
	// TODO: review for safety
	// connectionString = <db_host>:<port>/<service_name>
	connParams.ConnectString = fmt.Sprintf("%s:%d/%s",
		dsi.GetEndpoint().GetHost(),
		uint16(dsi.GetEndpoint().GetPort()),
		"FREE") // TODO service name from config

	// TODO: add tls
	// if dsi.UseTls {
	//	connParams.UseTLS
	// } else {
	//
	// }

	db := sql.OpenDB(godror.NewConnector(connParams))

	// // go-ora
	// connStr := go_ora.BuildUrl(
	// 	dsi.GetEndpoint().GetHost(),
	// 	int(dsi.GetEndpoint().GetPort()),
	// 	"FREE", // TODO service name from config
	// 	dsi.Credentials.GetBasic().GetUsername(),
	// 	dsi.Credentials.GetBasic().GetPassword(),
	// 	nil,
	// )

	// db, err := sql.Open("oracle", connStr)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to open connection: %w", err)
	// }

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
	common.LogCloserError(logger, conn, "close Oracle connection")
}

func NewConnectionManager(cfg rdbms_utils.ConnectionManagerBase) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: cfg}
}
