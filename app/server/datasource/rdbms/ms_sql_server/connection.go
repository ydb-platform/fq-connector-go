package ms_sql_server

import (
	"context"
	"database/sql"

	_ "github.com/denisenkom/go-mssqldb"
	"go.uber.org/zap"

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

func (c Connection) Query(ctx context.Context, _ *zap.Logger, query string, args ...any) (rdbms_utils.Rows, error) {
	c.logger.Dump(query, args...)

	out, err := c.db.QueryContext(ctx, query, args...)

	return rows{out}, err
}
