package logging

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
)

var _ rdbms_utils.Rows = (*rowsImpl)(nil)

type rowsImpl struct {
	rdbms_utils.Rows
}

func (r *rowsImpl) MakeTransformer(ydbColumns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(ydbColumns))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(ydbColumns))

	for _, ydbColumn := range ydbColumns {
		switch ydbColumn.Name {
		case levelColumnName:
			acceptors = append(acceptors, new(*int32))
			panic("NOT READY YET")
		case messageColumnName:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		case timestampColumnName:
			acceptors = append(acceptors, new(*time.Time))
			appenders = append(appenders, utils.MakeAppenderNullable[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()))
		case metaColumnName:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		}
	}

	return nil, nil
}

var _ rdbms_utils.Connection = (*connectionImpl)(nil)

type connectionImpl struct {
	rdbms_utils.Connection
}

func (c *connectionImpl) Query(params *rdbms_utils.QueryParams) (rdbms_utils.Rows, error) {
	ydbRows, err := c.Connection.Query(params)
	if err != nil {
		return nil, fmt.Errorf("ydb connection query: %w", err)
	}

	return &rowsImpl{Rows: ydbRows}, nil
}
