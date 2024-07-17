package oracle

import (
	"context"
	"fmt"
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

	// go-ora native
	creds := dsi.GetCredentials().GetBasic()
	oraOptions := dsi.GetOraOptions()
	connStr1 := go_ora.BuildUrl(
		dsi.GetEndpoint().GetHost(),
		int(dsi.GetEndpoint().Port),
		oraOptions.GetServiceName(),
		creds.GetUsername(),
		creds.GetPassword(),
		nil,
	)

	conn, err := go_ora.NewConnection(connStr1, nil)
	if err != nil {
		return nil, fmt.Errorf("new go-ora connection: %w", err)
	}

	err = conn.Open()
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}

	pingCtx, pingCtxCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCtxCancel()

	err = conn.Ping(pingCtx)
	if err != nil {
		defer conn.Close()
		return nil, fmt.Errorf("ping database: %w", err)
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
