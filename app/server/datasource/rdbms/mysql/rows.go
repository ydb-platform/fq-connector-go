package mysql

import (
	"io"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
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
		valueType := r.result.Resultset.Fields[i].Type

		if err != nil {
			return err
		}

		switch dest[i].(type) {
		case *string:
			*dest[i].(*string) = string(value.([]byte))
		// library uses uint64 to store EVERY non-string/[]bytes value
		default:
			switch valueType {
			case mysql.MYSQL_TYPE_LONG:
				tmp := new(int32)
				cast, ok := value.(int64)

				if ok {
					*tmp = int32(cast)
					*dest[i].(**int32) = tmp
				}

			default:
				panic("Not implemented yet")
			}
		}
	}

	return nil
}

func (r rows) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	fields := r.result.Fields
	ids := make([]uint8, 0, len(fields))

	for _, field := range fields {
		ids = append(ids, field.Type)
	}

	return transformerFromTypeIDs(ids, ydbTypes, cc)
}
