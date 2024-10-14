package oracle

import (
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

func (c Connection) Query(queryParams *rdbms_utils.QueryParams) (rdbms_utils.Rows, error) {
	c.logger.Dump(queryParams.Text, queryParams.ArgsCollection.Args()...)

	valueArgs := make([]driver.NamedValue, queryParams.ArgsCollection.Count())
	for i := 0; i < len(queryParams.ArgsCollection.Args()); i++ {
		valueArgs[i].Value = queryParams.ArgsCollection.Get(i).Value
		// TODO YQ-3455: research
		// 	for some reason query works with all Ordinal = 0
		// 	Golang docs states: Ordinal position of the parameter starting from one and is always set.
		//		If Name is empty, Ordinal value is used as parameter identifier:
		// 			https://pkg.go.dev/database/sql/driver#NamedValue
		valueArgs[i].Ordinal = i + 1
	}

	out, err := c.conn.QueryContext(queryParams.Ctx, queryParams.Text, valueArgs)
	if err != nil {
		return nil, fmt.Errorf("query with context: %w", err)
	}

	rows := newRows(out)

	return rows, nil
}
