package mysql

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"

	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*Connection)(nil)

type Connection struct {
	logger            common.QueryLogger
	conn              *client.Conn
	rowBufferCapacity uint64
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func (c *Connection) Query(_ context.Context, query string, args ...any) (rdbms_utils.Rows, error) {
	c.logger.Dump(query, args...)

	results := make(chan rowData, c.rowBufferCapacity)

	r := &rows{results, nil, &mysql.Result{}, atomic.Bool{}}

	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return r, fmt.Errorf("mysql: failed to prepare query: %w", err)
	}

	r.busy.Store(true)

	go func() {
		defer close(r.rowChan)

		err = stmt.ExecuteSelectStreaming(
			r.result,
			// In per-row handler copy entire row. The driver re-uses memory allocated for single row,
			// so we need either to lock the row until the reader is done its reading and processing
			// or simply copy it. Otherwise data races are inevitable.
			func(row []mysql.FieldValue) error {
				newRow := make([]fieldValue, len(row))

				for i, r := range row {
					newRow[i].Type = r.Type
					val := r.Value()

					switch val.(type) {
					case []byte:
						newRow[i].Value = make([]byte, len(val.([]byte)))
						copy(newRow[i].Value.([]byte), val.([]byte))
					default:
						newRow[i].Value = val
					}
				}

				r.rowChan <- rowData{newRow, r.result.Fields}

				return nil
			},
			nil,
			args...,
		)

		r.busy.Store(false)
	}()

	return r, nil
}
