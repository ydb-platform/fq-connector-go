package mysql

import (
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
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

func (r rows) Scan(dest ...any) error {
	if *r.resIdx >= r.result.Resultset.RowNumber() {
		return io.EOF
	}

	for i := 0; i < r.result.Resultset.ColumnNumber(); i++ {
		value, err := r.result.Resultset.GetValue(*r.resIdx, i)
		valueType := r.result.Resultset.Fields[i].Type

		if err != nil {
			return err
		}

		switch dest[i].(type) {
		case *string:
			*dest[i].(*string) = string(value.([]byte))
		// library uses uint64 to store EVERY non-string/[]bytes value
		default:
			switch valueType {
			case mysql.MYSQL_TYPE_LONG:
				tmp := new(int32)
				*tmp = int32(value.(int64))
				*dest[i].(**int32) = tmp
			}
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

func transformerFromTypeIDs(ids []uint8, _ []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(ids))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(ids))

	for _, id := range ids {
		switch id {
		case mysql.MYSQL_TYPE_LONG:
			acceptors = append(acceptors, new(*int32))
			appenders = append(appenders, makeAppender[int32, int32, *array.Int32Builder](cc.Int32()))
		default:
			panic(fmt.Sprintf("Type %d not implemented yet!", id))
		}
	}

	return paging.NewRowTransformer(acceptors, appenders, nil), nil
}

func makeAppender[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](conv conversion.ValueConverter[IN, OUT]) func(acceptor any, builder array.Builder) error {
	return func(acceptor any, builder array.Builder) error {
		return appendValueToArrowBuilder[IN, OUT, AB](acceptor, builder, conv)
	}
}

func appendValueToArrowBuilder[IN common.ValueType, OUT common.ValueType, AB common.ArrowBuilder[OUT]](
	acceptor any,
	builder array.Builder,
	conv conversion.ValueConverter[IN, OUT],
) error {
	cast := acceptor.(**IN)
	fmt.Printf("%v\n", cast)

	if *cast == nil {
		builder.AppendNull()

		return nil
	}

	value := **cast

	out, err := conv.Convert(value)
	if err != nil {
		if errors.Is(err, common.ErrValueOutOfTypeBounds) {
			builder.AppendNull()

			return nil
		}

		return fmt.Errorf("convert value %v: %w", value, err)
	}

	//nolint:forcetypeassert
	builder.(AB).Append(out)
	*cast = nil

	return nil
}
