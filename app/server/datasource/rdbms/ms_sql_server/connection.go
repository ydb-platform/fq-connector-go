package ms_sql_server

import (
	"database/sql"

	_ "github.com/denisenkom/go-mssqldb"

	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*Connection)(nil)

type Connection struct {
	db     *sql.DB
	logger common.QueryLogger
}

func (c Connection) Close() error {
	return c.db.Close()
}

func (c Connection) Query(params *rdbms_utils.QueryParams) (rdbms_utils.Rows, error) {
	c.logger.Dump(params.QueryText, params.QueryArgs.Values()...)

	out, err := c.db.QueryContext(params.Ctx, params.QueryText, params.QueryArgs.Values()...)

	return rows{out}, err
}
