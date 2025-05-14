package logging

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
)

var _ paging.RowTransformer[any] = (*rowTransformer)(nil)

type rowTransformer struct {
	acceptors                       []any
	internalColumnNameToAcceptorsIx map[string]int
}

func (rt *rowTransformer) AppendToArrowBuilders(schema *arrow.Schema, builders []array.Builder) error {
	// these are external fields
	for _, field := range schema.Fields() {
		externalColumnName := field.Name()

		internalColumnName, exists := externalToInternalColumnName[externalColumnName]
		if !exists {
			return fmt.Errorf("uenxpected external column name '%s'", &externalColumnName)
		}

		switch externalColumnName {
		case levelColumnName:
			ix := rt.internalColumnNameToAcceptorsIx[internalColumnName]

			src, ok := rt.acceptors[ix].(**int32)
			if !ok {
				return fmt.Errorf("unexpected acceptor type %T", src)
			}

			typedBuilder, ok := builders[ix].(*array.StringBuilder)
			if !ok {
				return fmt.Errorf("unexpected builder type %T", builders[ix])
			}

			if *src == nil {
				typedBuilder.AppendNull()
				return nil
			}

			switch **src {
			case 1:
				typedBuilder.Append(levelTraceValue)
			case 2:
				typedBuilder.Append(levelDebugValue)
			case 3:
				typedBuilder.Append(levelInfoValue)
			case 4:
				typedBuilder.Append(levelWarnValue)
			case 5:
				typedBuilder.Append(levelErrorValue)
			case 6:
				typedBuilder.Append(levelFatalValue)
			default:
				return fmt.Errorf("unexpected level value %d", *src)
			}
		}
	}

	return nil
}

func (rt *rowTransformer) maybeInitAppenders() error {

}

func (rt *rowTransformer) GetAcceptors() []any {
	return rt.acceptors
}

func (rt *rowTransformer) SetAcceptors(_ []any) {
	panic("implementation error")
}

func makeRowTransformer(ydbColumns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(ydbColumns))
	internalColumnNamesToAcceptorsIx := make(map[string]int, len(ydbColumns))

	for i, ydbColumn := range ydbColumns {
		switch ydbColumn.Name {
		case levelColumnName:
			acceptors = append(acceptors, new(*int32))
			internalColumnNamesToAcceptorsIx[levelColumnName] = i
		case messageColumnName:
			acceptors = append(acceptors, new(*string))
			internalColumnNamesToAcceptorsIx[messageColumnName] = i
			// appenders = append(appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		case timestampColumnName:
			acceptors = append(acceptors, new(*time.Time))
			internalColumnNamesToAcceptorsIx[timestampColumnName] = i
			// appenders = append(appenders, utils.MakeAppenderNullable[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()))
		case jsonPayloadColumnName:
			acceptors = append(acceptors, new(*string))
			internalColumnNamesToAcceptorsIx[jsonPayloadColumnName] = i
			// appenders = append(appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		default:
			return nil, fmt.Errorf("unexpected column name '%s'", ydbColumn.Name)
		}
	}

	return &rowTransformer{
		acceptors:                       acceptors,
		internalColumnNameToAcceptorsIx: internalColumnNamesToAcceptorsIx,
	}, nil
}
