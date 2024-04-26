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

		switch valueType {
		// TODO: handle blobs separately
		case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING,
			mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB:
			*dest[i].(*string) = string(value.([]byte))
		case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_INT24:
			tmp := new(int32)
			if value != nil {
				*tmp = int32(value.(int64))
				*dest[i].(**int32) = tmp
			}
		case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_TINY:
			tmp := new(int16)
			if value != nil {
				*tmp = int16(value.(int64))
				*dest[i].(**int16) = tmp
			}
		case mysql.MYSQL_TYPE_FLOAT:
			tmp := new(float32)
			if value != nil {
				*tmp = float32(value.(float64))
				*dest[i].(**float32) = tmp
			}
		case mysql.MYSQL_TYPE_DOUBLE:
			tmp := new(float64)
			if value != nil {
				*tmp = float64(value.(float64))
				*dest[i].(**float64) = tmp
			}
		default:
			panic("Not implemented yet")
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
