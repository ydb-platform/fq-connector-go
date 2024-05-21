package mysql

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

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
		if fieldValueType == mysql.FieldValueTypeNull {
			*dest.(**string) = nil
		} else if c, ok := dest.(**string); ok {
			s := string(value.([]byte))
			*c = &s
		} else {
			*dest.(*string) = string(value.([]byte))
		}
	case mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_TINY_BLOB:
		// Special case for table metadata
		if columnName == DATA_TYPE_COLUMN || columnName == COLUMN_TYPE_COLUMN {
			*dest.(*string) = string(value.([]byte))
		} else if valueType == mysql.FieldValueTypeNull {
			*dest.(**[]byte) = nil
		} else if c, ok := dest.(**[]byte); ok {
			b := value.([]byte)
			*c = &b
		} else {
			*dest.(*[]byte) = value.([]byte)
		}
	case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_INT24:
		if fieldValueType == mysql.FieldValueTypeUnsigned {
			if fieldValueType == mysql.FieldValueTypeNull {
				*dest.(**uint32) = nil
			} else if c, ok := dest.(**uint32); ok {
				v := uint32(value.(uint64))
				*c = &v
			}
		} else if c, ok := dest.(**int32); ok {
			v := int32(value.(int64))
			*c = &v
		} else {
			*dest.(*int32) = int32(value.(int64))
		}
	// In MySQL bool is actually a tinyint(1)
	case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_TINY:
		if fieldValueType == mysql.FieldValueTypeUnsigned {
			if fieldValueType == mysql.FieldValueTypeNull {
				*dest.(**uint16) = nil
			} else if c, ok := dest.(**uint16); ok {
				v := uint16(value.(uint64))
				*c = &v
			}
		} else if c, ok := dest.(**int16); ok {
			v := int16(value.(int64))
			*c = &v
		} else {
			*dest.(*int16) = int16(value.(int64))
		}
	case mysql.MYSQL_TYPE_FLOAT:
		if fieldValueType == mysql.FieldValueTypeNull {
			*dest.(**float32) = nil
		} else if c, ok := dest.(**float32); ok {
			v := float32(value.(float64))
			*c = &v
		} else {
			*dest.(*float32) = float32(value.(float64))
		}
	case mysql.MYSQL_TYPE_DOUBLE:
		if fieldValueType == mysql.FieldValueTypeNull {
			*dest.(**float64) = nil
		} else if c, ok := dest.(**float64); ok {
			v := value.(float64)
			*c = &v
		} else {
			*dest.(*float64) = value.(float64)
		}
	default:
		return fmt.Errorf("mysql: datatype %v not implemented yet", valueType)
	}

	return nil
}

func (r *rows) Scan(dest ...any) error {
	row := <-r.rowChan
	// TODO: Somehow check if returned value is zero-value
	//       to produce EOF error

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
