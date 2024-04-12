package mysql

import (
	"io"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type rows struct {
	result *mysql.Result
	resIdx *int
}

func (rows) Close() error {
	return nil
}

func (rows) Err() error {
	return nil
}

func (r rows) Next() bool {
	if r.result == nil {
		return false
	}

	*r.resIdx++

	return *r.resIdx != r.result.Resultset.RowNumber()
}

func (rows) NextResultSet() bool {
	return false
}

func (r rows) Scan(dest ...any) error {
	if *r.resIdx >= r.result.Resultset.RowNumber() {
		return io.EOF
	}

	for i := 0; i < r.result.Resultset.ColumnNumber(); i++ {
		value, err := r.result.Resultset.GetValue(*r.resIdx, i)
		if err != nil {
			return err
		}

		switch dest[i].(type) {
		case *string:
			*dest[i].(*string) = string(value.([]byte))
		case *any:
			*dest[i].(*any) = value
		}
	}

	return nil
}

func (rows) MakeTransformer(_ []*Ydb.Type, _ conversion.Collection) (paging.RowTransformer[any], error) {
	return paging.NewRowTransformer[any](make([]any, 0), make([]func(acceptor any, builder array.Builder) error, 0), nil), nil
}
