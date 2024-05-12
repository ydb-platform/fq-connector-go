package mysql

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

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
	ctx context.Context,
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

	dialer := &net.Dialer{}
	proto := "tcp"

	if strings.Contains(addr, "/") {
		return nil, errors.New("mysql: unix socket connections are unsupported")
	}

	conn, err := client.ConnectWithDialer(ctx, proto, addr, user, password, db, dialer.DialContext)
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
