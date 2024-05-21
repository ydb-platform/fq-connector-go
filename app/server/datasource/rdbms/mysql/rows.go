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

var counter = 1

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
	doneChan  chan any
	nextReady chan any
	result    *mysql.Result
	done      bool
}

func (r rows) Close() error {
	r.result = nil

	return nil
}

func (rows) Err() error {
	return nil
}

func (r rows) Next() bool {
	next := <-r.nextReady
	if next != nil {
		return true
	} else if len(r.rowChan) == 0 && !r.done {
		r.done = true
		r.doneChan <- struct{}{}
	}
	return false
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

func (rows) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	ids := make([]uint8, 0, len(ydbTypes))

	return transformerFromTypeIDs(ids, ydbTypes, cc)
}
