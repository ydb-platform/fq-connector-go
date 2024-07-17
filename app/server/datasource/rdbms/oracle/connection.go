package oracle

import (
	"context"
	"database/sql/driver"
	"fmt"

	go_ora "github.com/sijms/go-ora/v2"

	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*Connection)(nil)

type Connection struct {
	conn   *go_ora.Connection
	logger common.QueryLogger
}

func (c Connection) Close() error {
	return c.conn.Close()
}

func (c Connection) Query(ctx context.Context, query string, args ...any) (rdbms_utils.Rows, error) {
	c.logger.Dump(query, args...)

	valueArgs := make([]driver.NamedValue, len(args))
	for i := 0; i < len(args); i++ {
		valueArgs[i].Value = args[i]
	}

	out, err := c.conn.QueryContext(ctx, query, valueArgs)
	if err != nil {
		return nil, fmt.Errorf("query context: %w", err)
	}

	rows, err := newRows(out)
	if err != nil {
		return nil, fmt.Errorf("new rows: %w", err)
	}

	return rows, nil
}
