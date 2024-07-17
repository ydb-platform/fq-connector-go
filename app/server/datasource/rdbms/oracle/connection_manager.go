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

	// url options example:
	// https://github.com/sijms/go-ora/blob/78d53fdf18c31d74e7fc9e0ebe49ee1c6af0abda/README.md?plain=1#L1403C112-L1408
	// https://github.com/sijms/go-ora/blob/78d53fdf18c31d74e7fc9e0ebe49ee1c6af0abda/README.md?plain=1#L115-L137
	// urlOptions := map[string]string {
	// 	"TRACE FILE": "trace.log",
	// 	"AUTH TYPE":  "TCPS",
	// 	"SSL": "enable",
	// 	"SSL VERIFY": "FALSE",
	// 	"WALLET": "PATH TO WALLET".
	// }

	// go-ora native
	creds := dsi.GetCredentials().GetBasic()
	oraOptions := dsi.GetOraOptions()
	connStr := go_ora.BuildUrl(
		dsi.GetEndpoint().GetHost(),
		int(dsi.GetEndpoint().Port),
		oraOptions.GetServiceName(),
		creds.GetUsername(),
		creds.GetPassword(),
		nil,
	)

	conn, err := go_ora.NewConnection(connStr, nil)
	if err != nil {
		return nil, fmt.Errorf("new go-ora connection: %w", err)
	}

	err = conn.OpenWithContext(ctx)
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
