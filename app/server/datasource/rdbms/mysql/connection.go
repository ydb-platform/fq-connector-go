package mysql

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*connection)(nil)

type connection struct {
	logger       common.QueryLogger
	conn         *client.Conn
	cfg          *config.TMySQLConfig
	databaseName string
	tableName    string
}

func (c *connection) Close() error {
	return c.conn.Close()
}

func transformArgs(src *rdbms_utils.QueryArgs) []any {
	dst := make([]any, len(src.Values()))

	for i, v := range src.Values() {
		switch t := v.(type) {
		// MySQL driver does not accept time.Time objects
		case time.Time:
			// TODO: check if time.RFC3339 (without Nano) would be enough
			dst[i] = t.Format(time.RFC3339Nano)
		default:
			dst[i] = v
		}
	}

	return dst
}

func (c *connection) Query(params *rdbms_utils.QueryParams) (rdbms_utils.Rows, error) {
	c.logger.Dump(params.QueryText, params.QueryArgs.Values()...)

	results := make(chan rowData, c.cfg.ResultChanCapacity)
	result := &mysql.Result{}

	r := &rows{
		ctx:                     params.Ctx,
		cfg:                     c.cfg,
		logger:                  params.Logger,
		rowChan:                 results,
		errChan:                 make(chan error, 1),
		lastRow:                 nil,
		transformerInitChan:     make(chan []uint8, 1),
		transformerInitFinished: atomic.Uint32{},
		inputFinished:           false,
	}

	stmt, err := c.conn.Prepare(params.QueryText)
	if err != nil {
		return r, fmt.Errorf("mysql: failed to prepare query: %w", err)
	}

	go func() {
		defer close(r.rowChan)
		defer close(r.errChan)

		r.errChan <- stmt.ExecuteSelectStreaming(
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
				case <-params.Ctx.Done():
					return params.Ctx.Err()
				}

				return nil
			},
			nil,
			transformArgs(params.QueryArgs)...,
		)
	}()

	return r, nil
}

func (c *connection) From() (databaseName, tableName string) {
	return c.databaseName, c.tableName
}
