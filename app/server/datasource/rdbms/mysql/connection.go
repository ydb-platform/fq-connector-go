package mysql

import (
	"context"

	"github.com/go-mysql-org/go-mysql/client"

	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*Connection)(nil)

type Connection struct {
	logger common.QueryLogger
	conn   *client.Conn
}

func (c Connection) Close() error {
	return c.conn.Close()
}

func (c Connection) Query(_ context.Context, query string, args ...any) (rdbms_utils.Rows, error) {
	c.logger.Dump(query, args...)
	result, err := c.conn.Execute(query, args...)

	i := new(int)
	*i = -1

	return rows{result, i}, err
}
