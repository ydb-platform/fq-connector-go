package mysql

import (
	"context"
	"fmt"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"

	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*Connection)(nil)

const rowBuffer = 512

type Connection struct {
	logger common.QueryLogger
	conn   *client.Conn
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func (c *Connection) Query(_ context.Context, query string, args ...any) (rdbms_utils.Rows, error) {
	c.logger.Dump(query, args...)

	results := make(chan rowData, rowBuffer)
	nextReady := make(chan any)

	var result mysql.Result
	r := &rows{results, nextReady, &result}

	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return r, fmt.Errorf("mysql: failed to prepare query: %w", err)
	}

	go func() {
		defer close(r.rowChan)
		defer close(r.nextReady)

		err = stmt.ExecuteSelectStreaming(
			r.result,
			// In per-row handler copy entire row. The driver re-uses internal,
			// so we need either to lock the row until the reader is done its reading and processing
			// or simply copy it. Otherwise data races are inevitable.
			func(row []mysql.FieldValue) error {
				nextReady <- struct{}{}

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

				c.logger.Debug("Writing row to channel")
				r.rowChan <- rowData{newRow, r.result.Fields}

				return nil
			},
			func(result *mysql.Result) error {
				r.result = result

				c.logger.Debug("Obtaining new result")

				return nil
			},
			args...,
		)

		c.logger.Debug("Reading from table is done")
	}()

	return r, nil
}
