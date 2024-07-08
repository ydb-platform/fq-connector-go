package oracle

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
)

type rows struct {
	rows             driver.Rows
	nextValuesBuffer []driver.Value

	inputFinished bool

	err error
}

func NewRows(queryRows driver.Rows) (rows, error) {
	return rows{
		rows:             queryRows,
		nextValuesBuffer: make([]driver.Value, len(queryRows.Columns())),
		inputFinished:    false,
		err:              nil,
	}, nil
}

func (r rows) NextResultSet() bool {
	return false
}

func (r rows) Next() bool {
	err := r.rows.Next(r.nextValuesBuffer)
	if err != nil {
		if err != io.EOF {
			r.err = fmt.Errorf("oracle can't get next row calues: %w", err)
		} else {
			r.inputFinished = true
		}
		return false
	}
	return true
}

func (r rows) Err() error {
	return r.err
}

var errNilPtr = errors.New("destination pointer is nil")

func scanToDest(dest, src any) error { // TODO pass column type names and make type mapping
	// driver.Value can be only one of 6 standart types
	// https://pkg.go.dev/database/sql/driver#Value

	// partial copy of standart code:
	// https://cs.opensource.google/go/go/+/master:src/database/sql/convert.go;l=230
	switch s := src.(type) {
	case string:
		switch d := dest.(type) {
		case **string:
			if d == nil {
				return errNilPtr
			}
			if *d == nil {
				*d = new(string)
			}
			**d = s
			return nil
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = s
			return nil
		case **int64:
			if dest == nil {
				return errNilPtr
			}
			i, err := strconv.Atoi(s)
			if err != nil {
				return fmt.Errorf("oracle cant convert \"%s\"(string) to **int64: %w", s, err)
			}
			if *d == nil {
				*d = new(int64)
			}
			**d = int64(i)
			return nil
		}
	case int64:
		switch d := dest.(type) {
		case *int64:
			if d == nil {
				return errNilPtr
			}
			*d = s
			return nil
		}
	}
	return fmt.Errorf("oracle unsupported Scan, storing driver.Value type %T into type %T", src, dest) // TODO add dest and val types
}

func (r rows) Scan(dest ...any) error {
	if r.inputFinished {
		return io.EOF
	}
	// TODO think about standart implemetation
	// 	maybe error if Scan colled twice withoud Next

	// TODO maybe check length of buffer and dest to be equal
	// if len(dest) != len(r.nextValuesBuffer) {
	// 	return fmt.Errorf("oracle wanted %d args, but got %d", len(r.nextValuesBuffer), len(dest))
	// }

	for i, val := range r.nextValuesBuffer {

		if err := scanToDest(dest[i], val); err != nil {
			return err
		}
	}

	return nil
}

func (r rows) Close() error {
	return r.rows.Close()
}

func (r rows) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
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
