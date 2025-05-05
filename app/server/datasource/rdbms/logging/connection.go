package logging

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
)

var _ rdbms_utils.Rows = (*rowsImpl)(nil)

type rowsImpl struct {
	rdbms_utils.Rows
}

func (r *rowsImpl) MakeTransformer(ydbColumns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error) {
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
