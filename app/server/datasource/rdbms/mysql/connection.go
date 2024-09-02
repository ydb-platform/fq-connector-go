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

func (c *Connection) Query(ctx context.Context, query string, args ...any) (rdbms_utils.Rows, error) {
	c.logger.Dump(query, args...)

	results := make(chan rowData, c.rowBufferCapacity)
	result := &mysql.Result{}

	r := &rows{
		rowChan:                 results,
		lastRow:                 nil,
		transformerInitChan:     make(chan []uint8, 1),
		transformerInitFinished: atomic.Uint32{},
		inputFinished:           false,
	}

	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return r, fmt.Errorf("mysql: failed to prepare query: %w", err)
	}

	go func() {
		defer close(r.rowChan)

		err = stmt.ExecuteSelectStreaming(
			result,
			// In per-row handler copy entire row. The driver re-uses memory allocated for single row,
			// so we need either to lock the row until the reader is done its reading and processing
			// or simply copy it. Otherwise data races are inevitable.
			func(row []mysql.FieldValue) error {
				newRow := make([]fieldValue, len(row))

				for i, r := range row {
					newRow[i].valueType = r.Type
					val := r.Value()

					switch val.(type) {
					case []byte:
						newRow[i].value = make([]byte, len(val.([]byte)))
						copy(newRow[i].value.([]byte), val.([]byte))
					default:
						newRow[i].value = val
					}
				}

				r.maybeInitializeTransformer(result.Fields)

				select {
				case r.rowChan <- rowData{newRow, result.Fields}:
				case <-ctx.Done():
					return ctx.Err()
				}

				return nil
			},
			nil,
			args...,
		)
	}()

	return r, nil
}
