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
func scanToDest(dest any, value any, valueType uint8, columnName string, schema string, flag uint16,
	fieldValueType mysql.FieldValueType) error {
	var err error

	switch valueType {
	case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
		err = scanStringValue[[]byte, string](dest, value, fieldValueType)
	case mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_TINY_BLOB:
		// Special case for table metadata
		if columnName == COLUMN_TYPE_COLUMN && schema == METAINFO_SCHEMA_NAME {
			err = scanStringValue[[]byte, string](dest, value, fieldValueType)
		} else {
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

	v := OUT(value.(IN))

	switch dest := dest.(type) {
	case **OUT:
		*dest = &v
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

	v := OUT(value.(IN))

	switch dest := dest.(type) {
	case **OUT:
		*dest = &v
	case *OUT:
		*dest = v
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

	v := value.(int64)
	b := v > 0

	switch dest := dest.(type) {
	case **bool:
		*dest = &b
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
		columnName := string(r.lastRow.Fields[i].Name)
		schema := string(r.lastRow.Fields[i].Schema)
		flag := r.lastRow.Fields[i].Flag

		if err := scanToDest(dest[i], value, valueType, columnName, schema, flag, fieldValueType); err != nil {
			return err
		}
	}

	return nil
}

func (*rows) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	return transformerFromYdbTypes(ydbTypes, cc)
}
