package oracle

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	go_ora "github.com/sijms/go-ora/v2"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.ConnectionManager = (*connectionManager)(nil)

type connectionManager struct {
	rdbms_utils.ConnectionManagerBase
	cfg *config.TOracleConfig
}

func (c *connectionManager) Make(
	params *rdbms_utils.ConnectionParams,
) (rdbms_utils.Connection, error) {
	dsi, ctx, logger := params.DataSourceInstance, params.Ctx, params.Logger
	if dsi.Protocol != api_common.EProtocol_NATIVE {
		return nil, fmt.Errorf("can not create Oracle connection with protocol '%v'", dsi.Protocol)
	}

	var err error

	urlOptions := make(map[string]string)
	if dsi.UseTls {
		// more information in YQ-3456
		urlOptions["SSL"] = "TRUE"
		urlOptions["AUTH TYPE"] = "TCPS"
	}

	// go-ora native
	creds := dsi.GetCredentials().GetBasic()
	oraOptions := dsi.GetOracleOptions()
	connStr := go_ora.BuildUrl(
		dsi.GetEndpoint().GetHost(),
		int(dsi.GetEndpoint().Port),
		oraOptions.GetServiceName(),
		creds.GetUsername(),
		creds.GetPassword(),
		urlOptions,
	)

	conn, err := go_ora.NewConnection(connStr, nil)
	if err != nil {
		return nil, fmt.Errorf("new go-ora connection: %w", err)
	}

	openCtx, openCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(c.cfg.OpenConnectionTimeout))
	defer openCtxCancel()

	err = conn.OpenWithContext(openCtx)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}

	pingCtx, pingCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(c.cfg.PingConnectionTimeout))
	defer pingCtxCancel()

	err = conn.Ping(pingCtx)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	queryLogger := c.QueryLoggerFactory.Make(logger)

	return &Connection{conn, queryLogger}, nil
}

func (*connectionManager) Release(_ context.Context, logger *zap.Logger, conn rdbms_utils.Connection) {
	common.LogCloserError(logger, conn, "close Oracle connection")
}

func NewConnectionManager(
	cfg *config.TOracleConfig,
	base rdbms_utils.ConnectionManagerBase,
) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: base, cfg: cfg}
}
