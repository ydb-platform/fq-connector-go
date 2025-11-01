package mysql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

type fieldValue struct {
	value     any
	valueType mysql.FieldValueType
}

type rowData struct {
	row    []fieldValue
	fields []*mysql.Field
}

var _ rdbms_utils.Rows = (*rows)(nil)

type rows struct {
	ctx    context.Context
	logger *zap.Logger

	errChan       chan error
	rowChan       chan rowData
	lastRow       *rowData
	inputFinished bool

	// This channel is used only once: when the first row arrives from the connection,
	// it's used to initialize transformer with column types (which are encoded with uint8 values)
	transformerInitChan     chan []uint8
	transformerInitFinished atomic.Uint32
	cfg                     *config.TMySQLConfig
}

func (*rows) Close() error { return nil }

func (*rows) Err() error { return nil }

func (r *rows) Next() bool {
	next, ok := <-r.rowChan

	if ok {
		r.lastRow = &next
	} else {
		r.lastRow = nil
		r.inputFinished = true
	}

	return ok
}

func (*rows) NextResultSet() bool {
	return false
}

func (r *rows) maybeInitializeTransformer(fields []*mysql.Field) {
	// Provide list of types in the resultset to initialize transformer
	if r.transformerInitFinished.CompareAndSwap(0, 1) {
		var mySQLTypes []uint8

		for i := range fields {
			t := fields[i].Type

			mySQLTypes = append(mySQLTypes, t)
		}

		select {
		case r.transformerInitChan <- mySQLTypes:
		case <-r.ctx.Done():
		}

		close(r.transformerInitChan)
	}
}

// To find out low-level type mapping table, see https://github.com/go-mysql-org/go-mysql/issues/770
//
//nolint:gocyclo
func scanToDest(
	dest any,
	value any,
	valueType uint8,
	flag uint16,
	fieldValueType mysql.FieldValueType,
) error {
	var err error

	switch valueType {
	case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_JSON:
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
	case mysql.MYSQL_TYPE_DATE:
		err = scanDateValue(dest, value, fieldValueType)
	case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP:
		err = scanDatetimeValue(dest, value, fieldValueType)
	default:
		return fmt.Errorf("type %d: %w", valueType, common.ErrDataTypeNotSupported)
	}

	if err != nil {
		return fmt.Errorf("scan value: %w", err)
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

func scanDateValue(dest, value any, fieldValueType mysql.FieldValueType) error {
	out := dest.(**time.Time)

	if fieldValueType == mysql.FieldValueTypeNull {
		*out = nil

		return nil
	}

	// TODO: time.Parse is quite slow, think about other solutions
	t, err := time.Parse("2006-01-02", string(value.([]byte)))
	if err != nil {
		return fmt.Errorf("time parse: %w", err)
	}

	*out = &t

	return nil
}

func scanDatetimeValue(dest, value any, fieldValueType mysql.FieldValueType) error {
	out := dest.(**time.Time)

	if fieldValueType == mysql.FieldValueTypeNull {
		*out = nil

		return nil
	}

	t, err := time.Parse("2006-01-02 15:04:05.999999", string(value.([]byte)))
	if err != nil {
		return fmt.Errorf("time parse: %w", err)
	}

	*out = &t

	return nil
}

func (r *rows) Scan(dest ...any) error {
	if r.inputFinished {
		return io.EOF
	}

	for i, val := range r.lastRow.row {
		value := val.value

		valueType := r.lastRow.fields[i].Type
		fieldValueType := val.valueType
		flag := r.lastRow.fields[i].Flag

		if err := scanToDest(dest[i], value, valueType, flag, fieldValueType); err != nil {
			return fmt.Errorf("scan to dest value #%d (%v): %w", i, val, err)
		}
	}

	return nil
}

func (r *rows) MakeTransformer(ydbColumns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error) {
	var (
		mySQLTypes []uint8
		ok         bool
	)

	select {
	case mySQLTypes, ok = <-r.transformerInitChan:
		if !ok {
			return nil, errors.New("mysql types are not ready")
		}
	case err := <-r.errChan:
		if err != nil {
			return nil, fmt.Errorf("error occurred during async reading: %w", err)
		}

		// nil error means that asynchronous reading was successfully finished
		// before the first line was received - the case of empty table
		r.logger.Warn("table seems to be empty")
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}

	return transformerFromSQLTypes(mySQLTypes, common.YDBColumnsToYDBTypes(ydbColumns), cc)
}
