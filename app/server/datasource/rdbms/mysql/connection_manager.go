package mysql

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"

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
	_ context.Context,
	logger *zap.Logger,
	dsi *api_common.TDataSourceInstance,
) (rdbms_utils.Connection, error) {
	queryLogger := c.QueryLoggerFactory.Make(logger)

	endpoint := dsi.GetEndpoint()
	addr := fmt.Sprintf("%s:%d", endpoint.GetHost(), endpoint.GetPort())

	db := dsi.GetDatabase()

	creds := dsi.GetCredentials().GetBasic()
	user := creds.GetUsername()
	password := creds.GetPassword()

	conn, err := client.Connect(addr, user, password, db)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to connect to database: %s", err))
	}

	return &Connection{queryLogger, conn}, nil
}

func (*connectionManager) Release(logger *zap.Logger, conn rdbms_utils.Connection) {
	common.LogCloserError(logger, conn, "close mysql connection")
}

func NewConnectionManager(cfg rdbms_utils.ConnectionManagerBase) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: cfg}
}
