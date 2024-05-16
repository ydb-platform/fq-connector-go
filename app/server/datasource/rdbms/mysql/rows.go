package mysql

import (
	"fmt"
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

func scanToDest(dest any, value any, valueType uint8, columnName string, fieldValueType mysql.FieldValueType) error {
	// if value == nil {
	// 	*dest.(*any) = nil
	// 	return nil
	// }

	switch valueType {
	case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
		*dest.(*string) = string(value.([]byte))
	case mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_TINY_BLOB:
		// Special case for table metadata
		if columnName == "DATA_TYPE" || columnName == "COLUMN_TYPE" {
			*dest.(*string) = string(value.([]byte))
		} else {
			*dest.(*[]byte) = value.([]byte)
		}
	case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_INT24:
		if fieldValueType == mysql.FieldValueTypeUnsigned {
			*dest.(*uint32) = uint32(value.(uint64))
		} else {
			*dest.(*int32) = int32(value.(int64))
		}
	// In MySQL bool is actually a tinyint(1)
	case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_TINY:
		if fieldValueType == mysql.FieldValueTypeUnsigned {
			*dest.(*uint16) = uint16(value.(uint64))
		} else {
			*dest.(*int16) = int16(value.(int64))
		}
	case mysql.MYSQL_TYPE_FLOAT:
		*dest.(*float32) = float32(value.(float64))
	case mysql.MYSQL_TYPE_DOUBLE:
		*dest.(*float64) = float64(value.(float64))
	default:
		return fmt.Errorf("mysql: datatype %v not implemented yet", valueType)
	}

	return nil
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

		valueType := r.result.Resultset.Fields[i].Type
		fieldValueType := r.result.Resultset.Values[*r.resIdx][i].Type
		columnName := string(r.result.Resultset.Fields[i].Name)

		if err = scanToDest(dest[i], value, valueType, columnName, fieldValueType); err != nil {
			return err
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
