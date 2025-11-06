package mysql

import (
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*connection)(nil)

type connection struct {
	queryLogger        common.QueryLogger
	conn               *client.Conn
	cfg                *config.TMySQLConfig
	dataSourceInstance *api_common.TGenericDataSourceInstance
	tableName          string
}

func transformArgs(src *rdbms_utils.QueryArgs) ([]any, error) {
	dst := make([]any, len(src.Values()))

	for i, v := range src.Values() {
		switch t := v.(type) {
		// MySQL driver does not accept time.Time objects
		case time.Time:
			// TODO: check if time.RFC3339 (without Nano) would be enough
			dst[i] = t.Format(time.RFC3339Nano)
		default:
			rv := reflect.ValueOf(v)

			if rv.Kind() == reflect.Ptr {
				if rv.IsNil() {
					return dst, errors.New("nil pointer does not supported")
				}

				dst[i] = rv.Elem().Interface()
			} else {
				dst[i] = v
			}
		}
	}

	return dst, nil
}

func (c *connection) Query(params *rdbms_utils.QueryParams) (*rdbms_utils.QueryResult, error) {
	c.queryLogger.Dump(params.QueryText, params.QueryArgs.Values()...)

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
		return &rdbms_utils.QueryResult{Rows: r}, fmt.Errorf("mysql: failed to prepare query: %w", err)
	}

	args, err := transformArgs(params.QueryArgs)
	if err != nil {
		return &rdbms_utils.QueryResult{Rows: r}, fmt.Errorf("mysql: failed to prepare query: %w", err)
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
			args...,
		)
	}()

	return &rdbms_utils.QueryResult{
		Rows: r,
	}, nil
}

func (c *connection) Logger() *zap.Logger {
	return c.queryLogger.Logger
}

func (c *connection) DataSourceInstance() *api_common.TGenericDataSourceInstance {
	return c.dataSourceInstance
}

func (c *connection) TableName() string {
	return c.tableName
}

func (c *connection) Close() error {
	return c.conn.Close()
}
