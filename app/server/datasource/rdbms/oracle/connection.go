package oracle

import (
	"database/sql/driver"
	"fmt"

	go_ora "github.com/sijms/go-ora/v2"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Connection = (*connection)(nil)

type connection struct {
	conn               *go_ora.Connection
	queryLogger        common.QueryLogger
	dataSourceInstance *api_common.TGenericDataSourceInstance
	tableName          string
}

func (c *connection) Close() error {
	return c.conn.Close()
}

func (c *connection) Query(queryParams *rdbms_utils.QueryParams) (rdbms_utils.Rows, error) {
	c.queryLogger.Dump(queryParams.QueryText, queryParams.QueryArgs.Values()...)

	valueArgs := make([]driver.NamedValue, queryParams.QueryArgs.Count())
	for i := 0; i < len(queryParams.QueryArgs.Values()); i++ {
		valueArgs[i].Value = queryParams.QueryArgs.Get(i).Value
		// TODO YQ-3455: research
		// 	for some reason query works with all Ordinal = 0
		// 	Golang docs states: Ordinal position of the parameter starting from one and is always set.
		//		If Name is empty, Ordinal value is used as parameter identifier:
		// 			https://pkg.go.dev/database/sql/driver#NamedValue
		valueArgs[i].Ordinal = i + 1
	}

	out, err := c.conn.QueryContext(queryParams.Ctx, queryParams.QueryText, valueArgs)
	if err != nil {
		return nil, fmt.Errorf("query with context: %w", err)
	}

	rows := newRows(out)

	return rows, nil
}

func (c *connection) DataSourceInstance() *api_common.TGenericDataSourceInstance {
	return c.dataSourceInstance
}

func (c *connection) TableName() string {
	return c.tableName
}

func (c *connection) Logger() *zap.Logger {
	return c.queryLogger.Logger
}
