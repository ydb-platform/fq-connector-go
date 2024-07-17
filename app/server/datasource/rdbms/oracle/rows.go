package oracle

import (
	"fmt"
	"io"
	"strconv"

	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Rows = (*rows)(nil)

type rows struct {
	rows             driver.Rows
	nextValuesBuffer []driver.Value

	inputFinished bool

	err error
}

func newRows(queryRows driver.Rows) rdbms_utils.Rows {
	return &rows{
		rows:             queryRows,
		nextValuesBuffer: make([]driver.Value, len(queryRows.Columns())),
		inputFinished:    false,
		err:              nil,
	}
}

func (r *rows) Next() bool {
	if r.inputFinished {
		return false
	}

	err := r.rows.Next(r.nextValuesBuffer)
	if err != nil {
		if err != io.EOF {
			r.err = fmt.Errorf("next row values: %w", err)
		} else {
			r.inputFinished = true
		}

		return false
	}

	return true
}

func (r *rows) Err() error { return r.err }

func scanNilToDest(dest any) error {
	switch d := dest.(type) {
	case **string:
		*d = nil

		return nil
	case **int64:
		*d = nil

		return nil
	}

	return fmt.Errorf("unsupported Scan, storing driver.Value type <nil> into type %T: %w", dest, common.ErrDataTypeNotSupported)
}

func scanToDest(dest, src any) error {
	// driver.Value can be only one of 6 standart types
	// https://pkg.go.dev/database/sql/driver#Value

	// partial copy of standart code:
	// https://cs.opensource.google/go/go/+/master:src/database/sql/convert.go;l=230
	switch s := src.(type) {
	case string:
		switch d := dest.(type) {
		case **string:
			if *d == nil {
				*d = new(string)
			}

			**d = s

			return nil
		case **int64:
			i, err := strconv.Atoi(s)
			if err != nil {
				return fmt.Errorf("unsupported scan, convert \"%s\"(string) to **int64: %w", s, err)
			}

			if *d == nil {
				*d = new(int64)
			}

			**d = int64(i)

			return nil
		}
	case nil:
		return scanNilToDest(dest)
	}

	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %T: %w", src, dest, common.ErrDataTypeNotSupported)
}

func (r *rows) Scan(dest ...any) error {
	if r.inputFinished {
		return io.EOF
	}

	for i, val := range r.nextValuesBuffer {
		if err := scanToDest(dest[i], val); err != nil {
			return fmt.Errorf("scan to dest column %d (starts from 1): %w", i+1, err)
		}
	}

	return nil
}

func (r *rows) Close() error {
	return r.rows.Close()
}

func (r *rows) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	// got from golang standart library, source:
	// https://cs.opensource.google/go/go/+/refs/tags/go1.22.5:src/database/sql/sql.go;l=3244
	prop, ok := r.rows.(driver.RowsColumnTypeDatabaseTypeName)
	if !ok {
		return nil, fmt.Errorf("can't cast driver.Rows to driver.RowsColumnTypeDatabaseTypeName")
	}

	typeNames := make([]string, 0, len(r.rows.Columns()))
	for i := 0; i < len(r.rows.Columns()); i++ {
		typeNames = append(typeNames, prop.ColumnTypeDatabaseTypeName(i))
	}

	transformer, err := transformerFromSQLTypes(typeNames, ydbTypes, cc)
	if err != nil {
		return nil, fmt.Errorf("transformer from sql types: %w", err)
	}

	return transformer, nil
}

func (*rows) NextResultSet() bool { return false }
