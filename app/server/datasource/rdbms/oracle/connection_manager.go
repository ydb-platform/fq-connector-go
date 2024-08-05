package oracle

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"time"

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
	// TODO: cache of connections, remove unused connections with TTL
}

func checkTLSConneciton(ctx context.Context, conn *go_ora.Connection) error {
	rows, err := conn.QueryContext(ctx, `SELECT sys_context('USERENV', 'NETWORK_PROTOCOL') as network_protocol FROM dual`, nil)
	if err != nil {
		return fmt.Errorf("query TLS connection: %w", err)
	}
	values := make([]driver.Value, 1)
	if err = rows.Next(values); err != nil {
		return fmt.Errorf("get rows TLS connection: %w", err)
	}

	if err = rows.Next(values); err != io.EOF {
		return fmt.Errorf("more than 1 row TLS connection")
	}

	if len(values) != 1 {
		return fmt.Errorf("more than 1 column in row TLS connection")
	}

	connType, ok := values[0].(string)
	if !ok {
		return fmt.Errorf("value is not a string: %+v", connType)
	}

	if connType != "tcps" {
		return fmt.Errorf("not TLS connection type: \"%s\"", connType)
	}
	return nil
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

	urlOptions := make(map[string]string)
	if dsi.UseTls {
		// more information in YQ-3456
		urlOptions["SSL"] = "TRUE"
		urlOptions["AUTH TYPE"] = "TCPS"
		urlOptions["WALLET"] = c.cfg.GetWalletPath()
		urlOptions["WALLET PASSWORD"] = c.cfg.GetWalletPassword()
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
		return nil, fmt.Errorf("oracle: new go-ora connection: %w", err)
	}

	err = conn.OpenWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("oracle: open connection: %w", err)
	}

	pingCtx, pingCtxCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCtxCancel()

	err = conn.Ping(pingCtx)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("oracle: ping database: %w", err)
	}

	err = checkTLSConneciton(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("oracle: TLS check: %w", err)
	}

	queryLogger := c.QueryLoggerFactory.Make(logger)

	return &Connection{conn, queryLogger}, nil
}

func (*connectionManager) Release(logger *zap.Logger, conn rdbms_utils.Connection) {
	common.LogCloserError(logger, conn, "close Oracle connection")
}

func NewConnectionManager(
	cfg *config.TOracleConfig,
	base rdbms_utils.ConnectionManagerBase,
) rdbms_utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: base, cfg: cfg}
}
