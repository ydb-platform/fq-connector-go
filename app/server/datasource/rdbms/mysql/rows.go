package mysql

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

const (
	COLUMN_TYPE_COLUMN   = "COLUMN_TYPE"
	METAINFO_SCHEMA_NAME = "information_schema"
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

//nolint:gocyclo
func scanToDest(dest any, value any, valueType uint8, flag uint16,
	fieldValueType mysql.FieldValueType) error {
	var err error

	switch valueType {
	case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
		err = scanStringValue[[]byte, string](dest, value, fieldValueType)
	case mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_TINY_BLOB:
		// MySQL returns both TEXT and BLOB types as []byte, so we have to check destination beforehand
		switch dest.(type) {
		case *string, **string:
			err = scanStringValue[[]byte, string](dest, value, fieldValueType)
		default:
			err = scanStringValue[[]byte, []byte](dest, value, fieldValueType)
		}
	case mysql.MYSQL_TYPE_LONGLONG:
		if flag == mysql.UNSIGNED_FLAG {
			err = scanNumberValue[uint64, uint64](dest, value, fieldValueType)
		} else {
			err = scanNumberValue[int64, int64](dest, value, fieldValueType)
		}
	case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_INT24:
		if flag == mysql.UNSIGNED_FLAG {
			err = scanNumberValue[uint64, uint32](dest, value, fieldValueType)
		} else {
			err = scanNumberValue[int64, int32](dest, value, fieldValueType)
		}
	case mysql.MYSQL_TYPE_SHORT:
		if flag == mysql.UNSIGNED_FLAG {
			err = scanNumberValue[uint64, uint16](dest, value, fieldValueType)
		} else {
			err = scanNumberValue[int64, int16](dest, value, fieldValueType)
		}
	// In MySQL bool is actually a tinyint(1)
	case mysql.MYSQL_TYPE_TINY:
		if flag == mysql.UNSIGNED_FLAG {
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

func scanNumberValue[IN number, OUT number](dest, value any, fieldValueType mysql.FieldValueType) error {
	if fieldValueType == mysql.FieldValueTypeNull {
		*dest.(**OUT) = nil

		return nil
	}

	switch dest := dest.(type) {
	case **OUT:
		if *dest == nil {
			*dest = new(OUT)
		}

		**dest = OUT(value.(IN))
	default:
		return fmt.Errorf("mysql: %w", common.ErrValueOutOfTypeBounds)
	}

	return nil
}

func scanStringValue[IN stringLike, OUT stringLike](dest, value any, fieldValueType mysql.FieldValueType) error {
	if fieldValueType == mysql.FieldValueTypeNull {
		*dest.(**OUT) = nil

		return nil
	}

	switch dest := dest.(type) {
	case **OUT:
		if *dest == nil {
			*dest = new(OUT)
		}

		**dest = OUT(value.(IN))
	case *OUT:
		*dest = OUT(value.(IN))
	default:
		return fmt.Errorf("mysql: %w", common.ErrValueOutOfTypeBounds)
	}

	return nil
}

func scanBoolValue(dest, value any, fieldValueType mysql.FieldValueType) error {
	if fieldValueType == mysql.FieldValueTypeNull {
		*dest.(**bool) = nil

		return nil
	}

	switch dest := dest.(type) {
	case **bool:
		if *dest == nil {
			*dest = new(bool)
		}

		**dest = value.(int64) > 0
	default:
		return fmt.Errorf("mysql: %w", common.ErrValueOutOfTypeBounds)
	}

	return nil
}

func (r *rows) Scan(dest ...any) error {
	if r.lastRow == nil && !r.busy.Load() {
		return io.EOF
	}

	for i, val := range r.lastRow.Row {
		value := val.Value

		valueType := r.lastRow.Fields[i].Type
		fieldValueType := val.Type
		flag := r.lastRow.Fields[i].Flag

		if err := scanToDest(dest[i], value, valueType, flag, fieldValueType); err != nil {
			return err
		}
	}

	return nil
}

func (r *rows) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	return transformerFromSQLTypes(nil, ydbTypes, cc)
}
