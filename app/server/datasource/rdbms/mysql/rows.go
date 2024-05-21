package mysql

import (
	"fmt"
	"io"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"golang.org/x/exp/constraints"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
)

const (
	DATA_TYPE_COLUMN   = "DATA_TYPE"
	COLUMN_TYPE_COLUMN = "COLUMN_TYPE"
)

type fieldValue struct {
	Value any
	Type  mysql.FieldValueType
}

type rowData struct {
	Row    []fieldValue
	Fields []*mysql.Field
}

type rows struct {
	rowChan   chan rowData
	nextReady chan any
	result    *mysql.Result
}

func (r *rows) Close() error {
	r.result = nil
	return nil
}

func (*rows) Err() error {
	return nil
}

func (r *rows) Next() bool {
	next := <-r.nextReady

	return next != nil
}

func (*rows) NextResultSet() bool {
	return false
}

func scanToDest(dest any, value any, valueType uint8, columnName string, fieldValueType mysql.FieldValueType) error {
	switch valueType {
	case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
		scanStringValue[[]byte, string](dest, value, fieldValueType)
	case mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_TINY_BLOB:
		// Special case for table metadata
		if columnName == DATA_TYPE_COLUMN || columnName == COLUMN_TYPE_COLUMN {
			scanStringValue[[]byte, string](dest, value, fieldValueType)
		} else {
			scanStringValue[[]byte, []byte](dest, value, fieldValueType)
		}
	case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_INT24:
		if fieldValueType == mysql.FieldValueTypeUnsigned {
			scanNumberValue[uint64, uint32](dest, value, fieldValueType)
		} else {
			scanNumberValue[int64, int32](dest, value, fieldValueType)
		}
	// In MySQL bool is actually a tinyint(1)
	case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_TINY:
		if fieldValueType == mysql.FieldValueTypeUnsigned {
			scanNumberValue[uint64, uint16](dest, value, fieldValueType)
		} else {
			scanNumberValue[int64, int16](dest, value, fieldValueType)
		}
	case mysql.MYSQL_TYPE_FLOAT:
		scanNumberValue[float64, float32](dest, value, fieldValueType)
	case mysql.MYSQL_TYPE_DOUBLE:
		scanNumberValue[float64, float64](dest, value, fieldValueType)
	default:
		return fmt.Errorf("mysql: datatype %v not implemented yet", valueType)
	}

	return nil
}

type Number interface {
	constraints.Integer | constraints.Float
}

type StringLike interface {
	[]byte | string
}

func scanNumberValue[IN Number, OUT Number](dest, value any, fieldValueType mysql.FieldValueType) {
	if fieldValueType == mysql.FieldValueTypeNull {
		*dest.(**OUT) = nil

		return
	}

	v := OUT(value.(IN))

	if c, ok := dest.(**OUT); ok {
		*c = &v
	} else {
		*dest.(*OUT) = v
	}
}

func scanStringValue[IN StringLike, OUT StringLike](dest, value any, fieldValueType mysql.FieldValueType) {
	if fieldValueType == mysql.FieldValueTypeNull {
		*dest.(**OUT) = nil

		return
	}

	v := OUT(value.(IN))

	if c, ok := dest.(**OUT); ok {
		*c = &v
	} else {
		*dest.(*OUT) = v
	}
}

func (r *rows) Scan(dest ...any) error {
	row, ok := <-r.rowChan

	if !ok {
		return io.EOF
	}

	for i, val := range row.Row {
		value := val.Value

		valueType := row.Fields[i].Type
		fieldValueType := val.Type
		columnName := string(row.Fields[i].Name)

		if err := scanToDest(dest[i], value, valueType, columnName, fieldValueType); err != nil {
			return err
		}
	}

	return nil
}

func (*rows) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	ids := make([]uint8, 0, len(ydbTypes))

	return transformerFromTypeIDs(ids, ydbTypes, cc)
}
