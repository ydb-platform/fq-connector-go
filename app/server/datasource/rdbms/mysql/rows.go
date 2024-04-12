package mysql

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type rows struct {
	result *mysql.Result
	resIdx int
}

func (rows) Close() error {
	return nil
}

func (rows) Err() error {
	return nil
}

func (r rows) Next() bool {
	if r.result == nil || r.resIdx == len(r.result.Resultset.Values) {
		return false
	}

	r.resIdx++

	return true
}

func (rows) NextResultSet() bool {
	return false
}

func (rows) Scan(_ ...any) error {
	return nil
}

func (rows) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	return paging.NewRowTransformer[any](make([]any, 0), make([]func(acceptor any, builder array.Builder) error, 0), nil), nil
}
