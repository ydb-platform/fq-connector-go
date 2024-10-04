package oracle

import (
	"context"
	"database/sql/driver"
	"fmt"

	go_ora "github.com/sijms/go-ora/v2"
	"go.uber.org/zap"

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

func (c Connection) Query(ctx context.Context, _ *zap.Logger, query string, args ...any) (rdbms_utils.Rows, error) {
	c.logger.Dump(query, args...)

	valueArgs := make([]driver.NamedValue, len(args))
	for i := 0; i < len(args); i++ {
		valueArgs[i].Value = args[i]
		// TODO YQ-3455: research
		// 	for some reason query works with all Ordinal = 0
		// 	Golang docs states: Ordinal position of the parameter starting from one and is always set.
		//		If Name is empty, Ordinal value is used as parameter identifier:
		// 			https://pkg.go.dev/database/sql/driver#NamedValue
		valueArgs[i].Ordinal = i + 1
	}

	out, err := c.conn.QueryContext(ctx, query, valueArgs)
	if err != nil {
		return nil, fmt.Errorf("query with context: %w", err)
	}

	rows := newRows(out)

	return rows, nil
}
