package mysql

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"golang.org/x/exp/constraints"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

const (
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
	rowChan chan rowData
	lastRow *rowData
	result  *mysql.Result
	busy    atomic.Bool
}

func (r *rows) Close() error {
	r.result = nil
	return nil
}

func (*rows) Err() error {
	return nil
}

func (r *rows) Next() bool {
	next, ok := <-r.rowChan

	if ok {
		r.lastRow = &next
	} else {
		r.lastRow = nil
	}

	return ok
}

func (*rows) NextResultSet() bool {
	return false
}

func scanToDest(dest any, value any, valueType uint8, columnName string, fieldValueType mysql.FieldValueType) error {
	var err error

	switch valueType {
	case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
		err = scanStringValue[[]byte, string](dest, value, fieldValueType)
	case mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_TINY_BLOB:
		// Special case for table metadata
		if columnName == COLUMN_TYPE_COLUMN {
			err = scanStringValue[[]byte, string](dest, value, fieldValueType)
		} else {
			err = scanStringValue[[]byte, []byte](dest, value, fieldValueType)
		}
	case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_INT24:
		if fieldValueType == mysql.FieldValueTypeUnsigned {
			err = scanNumberValue[uint64, uint32](dest, value, fieldValueType)
		} else {
			err = scanNumberValue[int64, int32](dest, value, fieldValueType)
		}
	case mysql.MYSQL_TYPE_SHORT:
		if fieldValueType == mysql.FieldValueTypeUnsigned {
			err = scanNumberValue[uint64, uint16](dest, value, fieldValueType)
		} else {
			err = scanNumberValue[int64, int16](dest, value, fieldValueType)
		}
	// In MySQL bool is actually a tinyint(1)
	case mysql.MYSQL_TYPE_TINY:
		if fieldValueType == mysql.FieldValueTypeUnsigned {
			err = scanNumberValue[uint64, uint8](dest, value, fieldValueType)
		} else if _, ok := dest.(**bool); ok {
			err = scanBoolValue(dest, value, fieldValueType)
		} else {
			err = scanNumberValue[int64, int8](dest, value, fieldValueType)
		}
	case mysql.MYSQL_TYPE_FLOAT:
		err = scanNumberValue[float64, float32](dest, value, fieldValueType)
	case mysql.MYSQL_TYPE_DOUBLE:
		err = scanNumberValue[float64, float64](dest, value, fieldValueType)
	default:
		return fmt.Errorf("mysql: %w %v", common.ErrDataTypeNotSupported, valueType)
	}

	if err != nil {
		return fmt.Errorf("mysql: %w", err)
	}

	return nil
}

type number interface {
	constraints.Integer | constraints.Float
}

type stringLike interface {
	[]byte | string
}

func scanNumberValue[IN number, OUT number](dest, value any, fieldValueType mysql.FieldValueType) error {
	if fieldValueType == mysql.FieldValueTypeNull {
		*dest.(**OUT) = nil

		return nil
	}

	v := OUT(value.(IN))

	if c, ok := dest.(**OUT); ok {
		*c = &v
	} else {
		return fmt.Errorf("mysql: %w", common.ErrValueOutOfTypeBounds)
	}

	return nil
}

func scanStringValue[IN stringLike, OUT stringLike](dest, value any, fieldValueType mysql.FieldValueType) error {
	if fieldValueType == mysql.FieldValueTypeNull {
		*dest.(**OUT) = nil

		return nil
	}

	v := OUT(value.(IN))

	if c, ok := dest.(**OUT); ok {
		*c = &v
	} else if c, ok := dest.(*OUT); ok {
		*c = v
	} else {
		return fmt.Errorf("mysql: %w", common.ErrValueOutOfTypeBounds)
	}

	return nil
}

func scanBoolValue(dest, value any, fieldValueType mysql.FieldValueType) error {
	if fieldValueType == mysql.FieldValueTypeNull {
		*dest.(**bool) = nil

		return nil
	}

	v := value.(int64)
	b := v > 0

	if c, ok := dest.(**bool); ok {
		*c = &b
	} else {
		return fmt.Errorf("mysql: %w", common.ErrValueOutOfTypeBounds)
	}

	return nil
}

func (r *rows) Scan(dest ...any) error {
	if !r.busy.Load() && r.lastRow == nil {
		return io.EOF
	}

	for i, val := range r.lastRow.Row {
		value := val.Value

		valueType := r.lastRow.Fields[i].Type
		fieldValueType := val.Type
		columnName := string(r.lastRow.Fields[i].Name)

		if err := scanToDest(dest[i], value, valueType, columnName, fieldValueType); err != nil {
			return err
		}
	}

	return nil
}

func (*rows) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	return transformerFromTypeIDs(ydbTypes, cc)
}
