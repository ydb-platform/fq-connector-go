package ms_sql_server

import (
	"database/sql"

	_ "github.com/denisenkom/go-mssqldb"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*Connection)(nil)

type Connection struct {
	db                 *sql.DB
	queryLogger        common.QueryLogger
	dataSourceInstance *api_common.TGenericDataSourceInstance
	tableName          string
}

func (c *Connection) Close() error {
	return c.db.Close()
}

func (c *Connection) DataSourceInstance() *api_common.TGenericDataSourceInstance {
	return c.dataSourceInstance
}

func (c *Connection) TableName() string {
	return c.tableName
}

func (c *Connection) Query(params *rdbms_utils.QueryParams) (*rdbms_utils.QueryResult, error) {
	c.queryLogger.Dump(params.QueryText, params.QueryArgs.Values()...)

	out, err := c.db.QueryContext(params.Ctx, params.QueryText, params.QueryArgs.Values()...)
	if err != nil {
		return nil, err
	}

	return &rdbms_utils.QueryResult{
		Rows: rows{out},
	}, nil
}

func (c *Connection) Logger() *zap.Logger {
	return c.queryLogger.Logger
}
