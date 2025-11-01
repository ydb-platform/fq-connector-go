package postgresql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*connection)(nil)

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

func (r rows) MakeTransformer(ydbColumns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error) {
	fields := r.FieldDescriptions()

	oids := make([]uint32, 0, len(fields))
	for _, field := range fields {
		oids = append(oids, field.DataTypeOID)
	}

	return transformerFromOIDs(oids, common.YDBColumnsToYDBTypes(ydbColumns), cc)
}

type connection struct {
	*pgx.Conn
	queryLogger        common.QueryLogger
	dataSourceInstance *api_common.TGenericDataSourceInstance
	tableName          string
}

func (c *connection) Close() error {
	return c.Conn.Close(context.TODO())
}

func (c *connection) Query(params *rdbms_utils.QueryParams) (*rdbms_utils.QueryResult, error) {
	c.queryLogger.Dump(params.QueryText, params.QueryArgs.Values()...)

	out, err := c.Conn.Query(params.Ctx, params.QueryText, params.QueryArgs.Values()...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	return &rdbms_utils.QueryResult{
		Rows: rows{Rows: out},
	}, nil
}

func (c *connection) DataSourceInstance() *api_common.TGenericDataSourceInstance {
	return c.dataSourceInstance
}

func (c *connection) TableName() string {
	return c.tableName
}

func (c *connection) Logger() *zap.Logger {
	return c.queryLogger.Logger
}

var _ rdbms_utils.ConnectionManager = (*connectionManager)(nil)

type connectionManager struct {
	rdbms_utils.ConnectionManagerBase
	schemaGetter func(dsi *api_common.TGenericDataSourceInstance) string
	cfg          ConnectionManagerConfig
}

func (c *connectionManager) Make(
	params *rdbms_utils.ConnectionParams,
) ([]rdbms_utils.Connection, error) {
	dsi, ctx, logger := params.DataSourceInstance, params.Ctx, params.Logger
	if dsi.GetCredentials().GetBasic() == nil {
		return nil, errors.New("currently only basic auth is supported")
	}

	if dsi.Protocol != api_common.EGenericProtocol_NATIVE {
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

	return []rdbms_utils.Connection{&connection{conn, queryLogger, dsi, params.TableName}}, nil
}

func (*connectionManager) Release(_ context.Context, logger *zap.Logger, cs []rdbms_utils.Connection) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for _, conn := range cs {
		if err := conn.(*connection).DeallocateAll(ctx); err != nil {
			logger.Error("deallocate prepared statements", zap.Error(err))
		}

		common.LogCloserError(logger, conn, "close connection")
	}
}

type ConnectionManagerConfig interface {
	GetOpenConnectionTimeout() string
}

func NewConnectionManager(
	cfg ConnectionManagerConfig,
	base rdbms_utils.ConnectionManagerBase,
	schemaGetter func(*api_common.TGenericDataSourceInstance) string,
) rdbms_utils.ConnectionManager {
	return &connectionManager{
		ConnectionManagerBase: base,
		schemaGetter:          schemaGetter,
		cfg:                   cfg,
	}
}
