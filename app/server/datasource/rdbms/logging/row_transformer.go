package logging

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

var _ paging.RowTransformer[any] = (*rowTransformer)(nil)

type rowTransformer struct {
	acceptors                       []any
	internalColumnNameToAcceptorsIx map[string]int
	cc                              conversion.Collection
}

func (rt *rowTransformer) AppendToArrowBuilders(schema *arrow.Schema, builders []array.Builder) error {
	// json_payload internal columns contains various fields that are usefull
	// for construction of different columns
	var jsonPayloadParsed map[string]any
	jsonPayloadIx, exists := rt.internalColumnNameToAcceptorsIx[jsonPayloadColumnName]
	if exists {
		if err := json.Unmarshal([]byte(*(rt.acceptors[jsonPayloadIx]).(*string)), &jsonPayloadParsed); err != nil {
			return fmt.Errorf("json unmarshal column '%s': %w", jsonPayloadColumnName, err)
		}
	}

	// these are external fields
	for i, field := range schema.Fields() {
		externalColumnName := field.Name
		internalColumnName, exists := externalToInternalColumnName[externalColumnName]
		if !exists {
			return fmt.Errorf("uenxpected external column name '%s'", externalColumnName)
		}

		ix := rt.internalColumnNameToAcceptorsIx[internalColumnName]

		switch externalColumnName {
		case levelColumnName:
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
		case messageColumnName:
			err := utils.AppendValueToArrowBuilderNullable[string, string, *array.StringBuilder](rt.acceptors[i], builders[i], rt.cc.String())
			if err != nil {
				return fmt.Errorf("append value to arrow builder nullable for column '%s': %v", externalColumnName, err)
			}
		case timestampColumnName:
			err := utils.AppendValueToArrowBuilderNullable[time.Time, uint64, *array.Uint64Builder](rt.acceptors[i], builders[i], rt.cc.Timestamp())
			if err != nil {
				return fmt.Errorf("append value to arrow builder nullable for column '%s': %v", externalColumnName, err)
			}
		case projectColumnName, serviceColumnName, clusterColumnName:
			err := appendJsonPayloadField(jsonPayloadParsed, builders[i], externalColumnName)
			if err != nil {
				return fmt.Errorf("append json payload field '%s': %v", externalColumnName, err)
			}
		case jsonPayloadColumnName:
			err := utils.AppendValueToArrowBuilderNullable[string, string, *array.StringBuilder](rt.acceptors[i], builders[i], rt.cc.String())
			if err != nil {
				return fmt.Errorf("append value to arrow builder nullable for column '%s': %v", externalColumnName, err)
			}
		default:
			return fmt.Errorf("unexpected external field name '%s'", externalColumnName)
		}
	}

	return nil
}

func appendJsonPayloadField(jsonPayloadParsed map[string]any, builderUntyped array.Builder, fieldName string) error {
	builder, ok := builderUntyped.(*array.StringBuilder)
	if !ok {
		return fmt.Errorf("builder of an invalid type %T for column '%s'", builderUntyped, projectColumnName)
	}

	valueUntyped, exists := jsonPayloadParsed[projectColumnName]
	if !exists {
		builder.AppendNull()
		return nil
	}

	value, ok := valueUntyped.(string)
	if !ok {
		return fmt.Errorf("value of an invalid type %T for column '%s'", valueUntyped, projectColumnName)
	}

	builder.Append(value)

	return nil
}

func (rt *rowTransformer) GetAcceptors() []any {
	return rt.acceptors
}

func (rt *rowTransformer) SetAcceptors(_ []any) {
	panic("implementation error")
}

func makeTransformer(ydbColumns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(ydbColumns))

	fmt.Println(">>> makeTransformer", ydbColumns)

	// var internalColumnNames map[string]struct{}

	// for _, ydbColumn := range ydbColumns {
	// 	internalColumnNames[ydbColumn.Name] = struct{}{}
	// }

	internalColumnNamesToAcceptorsIx := make(map[string]int, len(ydbColumns))

	for i, ydbColumn := range ydbColumns {
		switch ydbColumn.Name {
		case levelColumnName:
			acceptors = append(acceptors, new(*int32))
			internalColumnNamesToAcceptorsIx[levelColumnName] = i
		case messageColumnName:
			acceptors = append(acceptors, new(*string))
			internalColumnNamesToAcceptorsIx[messageColumnName] = i
		case timestampColumnName:
			acceptors = append(acceptors, new(*time.Time))
			internalColumnNamesToAcceptorsIx[timestampColumnName] = i
		case jsonPayloadColumnName:
			acceptors = append(acceptors, new(*string))
			internalColumnNamesToAcceptorsIx[jsonPayloadColumnName] = i
		default:
			return nil, fmt.Errorf("unexpected column name '%s'", ydbColumn.Name)
		}
	}

	return &rowTransformer{
		acceptors:                       acceptors,
		internalColumnNameToAcceptorsIx: internalColumnNamesToAcceptorsIx,
	}, nil
}
