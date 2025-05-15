package logging

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
)

var _ paging.RowTransformer[any] = (*rowTransformer)(nil)

type rowTransformer struct {
	acceptors                       []any
	internalColumnNameToAcceptorsIx map[string]int
	cc                              conversion.Collection
}

//nolint:gocyclo,funlen
func (rt *rowTransformer) AppendToArrowBuilders(schema *arrow.Schema, builders []array.Builder) error {
	// 'json_payload' internal column contains various fields
	// that are useful for construction of different virtual columns
	var (
		jsonPayloadValue map[string]any
	)

	jsonPayloadIx, exists := rt.internalColumnNameToAcceptorsIx[jsonPayloadColumnName]
	if exists {
		acceptor := (rt.acceptors[jsonPayloadIx]).(**string)
		if *acceptor != nil {
			if err := json.Unmarshal([]byte(**acceptor), &jsonPayloadValue); err != nil {
				return fmt.Errorf("json unmarshal column '%s': %w", jsonPayloadColumnName, err)
			}
		}
	}

	var (
		metaValue   map[string]any
		labelsValue map[string]any
	)

	needMetaColumn := schema.HasField(metaColumnName)
	needLabelsColumn := schema.HasField(labelsColumnName)

	if len(jsonPayloadValue) != 0 && (needMetaColumn || needLabelsColumn) {
		// now fill meta and labels with fields from json_payload
		for key, val := range jsonPayloadValue {
			switch {
			case needLabelsColumn && strings.HasPrefix(key, labelsPrefix):
				if labelsValue == nil {
					labelsValue = make(map[string]any)
				}

				labelsValue[strings.TrimPrefix(key, labelsPrefix)] = val
			case needMetaColumn && strings.HasPrefix(key, metaPrefix):
				if metaValue == nil {
					metaValue = make(map[string]any)
				}

				metaValue[strings.TrimPrefix(key, metaPrefix)] = val
			}
		}
	}

	// iterate over external fields, but some of them will be extracted from the internal fields
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

			typedBuilder, ok := builders[i].(*array.StringBuilder)
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
			err := utils.AppendValueToArrowBuilderNullable[string, string, *array.StringBuilder](
				rt.acceptors[ix], builders[i], rt.cc.String())
			if err != nil {
				return fmt.Errorf("append value to arrow builder nullable for column '%s': %v", externalColumnName, err)
			}
		case timestampColumnName:
			err := utils.AppendValueToArrowBuilderNullable[time.Time, uint64, *array.Uint64Builder](
				rt.acceptors[ix], builders[i], rt.cc.Timestamp())
			if err != nil {
				return fmt.Errorf("append value to arrow builder nullable for column '%s': %v", externalColumnName, err)
			}
		case projectColumnName, serviceColumnName, clusterColumnName:
			err := appendJSONPayloadField(jsonPayloadValue, builders[i], externalColumnName)
			if err != nil {
				return fmt.Errorf("append json payload field '%s': %v", externalColumnName, err)
			}
		case metaColumnName:
			err := appendJSONDict(metaValue, builders[i])
			if err != nil {
				return fmt.Errorf("append json payload field '%s': %v", externalColumnName, err)
			}
		case labelsColumnName:
			err := appendJSONDict(labelsValue, builders[i])
			if err != nil {
				return fmt.Errorf("append json payload field '%s': %v", externalColumnName, err)
			}
		case jsonPayloadColumnName:
			err := utils.AppendValueToArrowBuilderNullable[string, string, *array.StringBuilder](
				rt.acceptors[ix], builders[i], rt.cc.String())
			if err != nil {
				return fmt.Errorf("append value to arrow builder nullable for column '%s': %v", externalColumnName, err)
			}
		default:
			return fmt.Errorf("unexpected external field name '%s'", externalColumnName)
		}
	}

	return nil
}

func appendJSONPayloadField(jsonPayloadParsed map[string]any, builderUntyped array.Builder, fieldName string) error {
	builder, ok := builderUntyped.(*array.StringBuilder)
	if !ok {
		return fmt.Errorf("builder of an invalid type %T", builderUntyped)
	}

	valueUntyped, exists := jsonPayloadParsed[fieldName]
	if !exists {
		builder.AppendNull()
		return nil
	}

	value, ok := valueUntyped.(string)
	if !ok {
		return fmt.Errorf("value of an invalid type %T for column '%s'", valueUntyped, fieldName)
	}

	builder.Append(value)

	return nil
}

func appendJSONDict(value map[string]any, builderUntyped array.Builder) error {
	builder, ok := builderUntyped.(*array.StringBuilder)
	if !ok {
		return fmt.Errorf("builder of an invalid type %T", builderUntyped)
	}

	if len(value) == 0 {
		builder.AppendNull()

		return nil
	}

	dump, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}

	builder.Append(unsafe.String(unsafe.SliceData(dump), len(dump)))

	return nil
}

func (rt *rowTransformer) GetAcceptors() []any {
	return rt.acceptors
}

func (rowTransformer) SetAcceptors(_ []any) {
	panic("implementation error")
}

func makeTransformer(ydbColumns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error) {
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
		cc:                              cc,
	}, nil
}
