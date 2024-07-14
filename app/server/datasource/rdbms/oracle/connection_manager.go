package oracle

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.uber.org/zap"

	go_ora "github.com/sijms/go-ora/v2"
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
	// var connParams godror.ConnectionParams

	// connParams.Username = dsi.Credentials.GetBasic().GetUsername()
	// connParams.Password = godror.NewPassword(dsi.Credentials.GetBasic().GetPassword())
	// // TODO: review for safety
	// // connectionString = <db_host>:<port>/<service_name>
	// connParams.ConnectString = fmt.Sprintf("%s:%d/%s",
	// 	dsi.GetEndpoint().GetHost(),
	// 	uint16(dsi.GetEndpoint().GetPort()),
	// 	"FREE") // TODO service name from config

	// // TODO: add tls
	// // if dsi.UseTls {
	// //	connParams.UseTLS
	// // } else {
	// //
	// // }

	// db := sql.OpenDB(godror.NewConnector(connParams))

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

	// go-ora native
	// connStr1 := go_ora.BuildUrl(
	// 	"127.0.0.1",
	// 	int(1522),
	// 	"FREE", // TODO service name from config
	// 	"C##ADMIN",
	// 	"password",
	// 	nil,
	// )
	// connStr1 = "oracle://C%23%23ADMIN:password@localhost:1522/FREE"
	creds := dsi.GetCredentials().GetBasic()
	ora_options := dsi.GetOraOptions()
	connStr1 := go_ora.BuildUrl(
		dsi.GetEndpoint().GetHost(),
		int(dsi.GetEndpoint().Port),
		ora_options.GetServiceName(), // TODO service name from config
		creds.GetUsername(),
		creds.GetPassword(),
		nil,
	)
	conn, err := go_ora.NewConnection(connStr1, nil)
	if err != nil {
		log.Fatal(err)
	}
	// check for error
	err = conn.Open()
	if err != nil {
		log.Fatal(err)
	}

	pingCtx, pingCtxCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCtxCancel()

	err = conn.Ping(pingCtx)
	if err != nil {
		defer conn.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	queryLogger := c.QueryLoggerFactory.Make(logger)

	return &Connection{conn, queryLogger}, nil
}

func (*connectionManager) Release(logger *zap.Logger, conn rdbms_utils.Connection) {
	common.LogCloserError(logger, conn, "close Oracle connection")
}

func NewConnectionManager(cfg rdbms_utils.ConnectionManagerBase) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: cfg}
}
