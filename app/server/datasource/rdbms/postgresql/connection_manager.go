package postgresql

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*Connection)(nil)

type rows struct {
	pgx.Rows
}

func (rows) NextResultSet() bool {
	return false
}

func (r rows) Close() error {
	r.Rows.Close()

	return nil
}

func (r rows) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	fields := r.FieldDescriptions()

	oids := make([]uint32, 0, len(fields))
	for _, field := range fields {
		oids = append(oids, field.DataTypeOID)
	}

	return transformerFromOIDs(oids, ydbTypes, cc)
}

type Connection struct {
	*pgx.Conn
	logger common.QueryLogger
}

func (c Connection) Close() error {
	return c.Conn.Close(context.TODO())
}

func (c Connection) Query(params *rdbms_utils.QueryParams) (rdbms_utils.Rows, error) {
	c.logger.Dump(params.QueryText, params.QueryArgs...)

	out, err := c.Conn.Query(params.Ctx, params.QueryText, params.QueryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	return rows{Rows: out}, nil
}

var _ rdbms_utils.ConnectionManager = (*connectionManager)(nil)

type connectionManager struct {
	rdbms_utils.ConnectionManagerBase
	schemaGetter func(dsi *api_common.TDataSourceInstance) string
	cfg          ConnectionManagerConfig
}

func (c *connectionManager) Make(
	ctx context.Context,
	logger *zap.Logger,
	dsi *api_common.TDataSourceInstance,
) (rdbms_utils.Connection, error) {
	if dsi.GetCredentials().GetBasic() == nil {
		return nil, fmt.Errorf("currently only basic auth is supported")
	}

	if dsi.Protocol != api_common.EProtocol_NATIVE {
		return nil, fmt.Errorf("can not create PostgreSQL connection with protocol '%v'", dsi.Protocol)
	}

	if socketType, _ := pgconn.NetworkAddress(dsi.GetEndpoint().GetHost(), uint16(dsi.GetEndpoint().GetPort())); socketType != "tcp" {
		return nil, fmt.Errorf("can not create PostgreSQL connection with socket type '%s'", socketType)
	}

	connStr := "dbname=DBNAME user=USER password=PASSWORD host=HOST port=5432"
	if dsi.UseTls {
		connStr += " sslmode=verify-full"
	} else {
		connStr += " sslmode=disable"
	}

	connCfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse connection config template: %w", err)
	}

	connCfg.Database = dsi.Database
	connCfg.Host = dsi.GetEndpoint().GetHost()
	connCfg.Port = uint16(dsi.GetEndpoint().GetPort())
	connCfg.User = dsi.Credentials.GetBasic().GetUsername()
	connCfg.Password = dsi.Credentials.GetBasic().GetPassword()

	if dsi.UseTls {
		connCfg.TLSConfig.ServerName = dsi.GetEndpoint().GetHost()
	}

	openCtx, openCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(c.cfg.GetOpenConnectionTimeout()))
	defer openCtxCancel()

	conn, err := pgx.ConnectConfig(openCtx, connCfg)
	if err != nil {
		return nil, fmt.Errorf("connect config: %w", err)
	}

	// set schema (public by default)

	searchPath := fmt.Sprintf("set search_path=%s", c.schemaGetter(dsi))

	if _, err = conn.Exec(openCtx, searchPath); err != nil {
		return nil, fmt.Errorf("exec: %w", err)
	}

	queryLogger := c.QueryLoggerFactory.Make(logger)

	return &Connection{conn, queryLogger}, nil
}

func (*connectionManager) Release(ctx context.Context, logger *zap.Logger, conn rdbms_utils.Connection) {
	if err := conn.(*Connection).Conn.DeallocateAll(ctx); err != nil {
		logger.Error("deallocate prepared statements", zap.Error(err))
	}

	common.LogCloserError(logger, conn, "close connection")
}

type ConnectionManagerConfig interface {
	GetOpenConnectionTimeout() string
}

func NewConnectionManager(
	cfg ConnectionManagerConfig,
	base rdbms_utils.ConnectionManagerBase,
	schemaGetter func(*api_common.TDataSourceInstance) string,
) rdbms_utils.ConnectionManager {
	return &connectionManager{
		ConnectionManagerBase: base,
		schemaGetter:          schemaGetter,
		cfg:                   cfg,
	}
}
