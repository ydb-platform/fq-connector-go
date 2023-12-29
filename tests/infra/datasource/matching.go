package datasource

import (
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/common"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/stretchr/testify/require"
)

// Record is somewhat equivalent to arrow.Record
type Record struct {
	Columns []any
}

func (r *Record) MatchRecord(t *testing.T, schema *api_service_protos.TSchema, record arrow.Record) {
	for i, arrowField := range record.Schema().Fields() {
		ydbType := schema.Columns[i].Type
		switch ydbType.GetType().(type) {
		case *Ydb.Type_TypeId:
			matchColumns(t, arrowField, r.Columns[i], record.Column(i), false)
		case *Ydb.Type_OptionalType:
			matchColumns(t, arrowField, r.Columns[i], record.Column(i), true)
		default:
			require.FailNow(t, fmt.Sprintf("unexpected YDB type: %v", ydbType))
		}
	}
}

func matchColumns(t *testing.T, arrowField arrow.Field, expected any, actual arrow.Array, optional bool) {
	switch arrowField.Type.ID() {
	case arrow.INT8:
		matchArrays[int8, *array.Int8](t, arrowField.Name, expected, actual, optional)
	case arrow.INT16:
		matchArrays[int16, *array.Int16](t, arrowField.Name, expected, actual, optional)
	case arrow.INT32:
		matchArrays[int32, *array.Int32](t, arrowField.Name, expected, actual, optional)
	case arrow.INT64:
		matchArrays[int64, *array.Int64](t, arrowField.Name, expected, actual, optional)
	case arrow.UINT8:
		matchArrays[uint8, *array.Uint8](t, arrowField.Name, expected, actual, optional)
	case arrow.UINT16:
		matchArrays[uint16, *array.Uint16](t, arrowField.Name, expected, actual, optional)
	case arrow.UINT32:
		matchArrays[uint32, *array.Uint32](t, arrowField.Name, expected, actual, optional)
	case arrow.UINT64:
		matchArrays[uint64, *array.Uint64](t, arrowField.Name, expected, actual, optional)
	case arrow.FLOAT32:
		matchArrays[float32, *array.Float32](t, arrowField.Name, expected, actual, optional)
	case arrow.FLOAT64:
		matchArrays[float64, *array.Float64](t, arrowField.Name, expected, actual, optional)
	case arrow.STRING:
		matchArrays[string, *array.String](t, arrowField.Name, expected, actual, optional)
	case arrow.BINARY:
		matchArrays[[]byte, *array.Binary](t, arrowField.Name, expected, actual, optional)
	default:
		require.FailNow(t, fmt.Sprintf("unexpected arrow type: %v", arrowField.Type.ID().String()))
	}
}

func matchArrays[EXPECTED common.ValueType, ACTUAL common.ArrowArrayType[EXPECTED]](
	t *testing.T,
	columnName string,
	expectedRaw any,
	actualRaw arrow.Array,
	optional bool,
) {
	actual, ok := actualRaw.(ACTUAL)
	require.True(t, ok)

	if optional {
		expected, ok := expectedRaw.([]*EXPECTED)
		require.True(
			t, ok,
			fmt.Sprintf("invalid type for column %v: %T (%v)", columnName, expectedRaw, expectedRaw),
		)
		require.Equal(t, len(expected), actual.Len(),
			fmt.Sprintf("column:  %v\nexpected: %v\nactual:  %v\n", columnName, expected, actual),
		)

		for j := 0; j < len(expected); j++ {
			if expected[j] != nil {
				require.Equal(
					t, *expected[j], actual.Value(j),
					fmt.Sprintf("column:  %v\nexpected: %v\nactual:  %v\n", columnName, expected, actual))
			} else {
				require.True(t, actual.IsNull(j),
					fmt.Sprintf("column:  %v\nexpected: %v\nactual:  %v\n", columnName, expected, actual))
			}
		}
	} else {
		expected, ok := expectedRaw.([]EXPECTED)
		require.True(
			t, ok,
			fmt.Sprintf("invalid type for column %v: %T (%v)", columnName, expectedRaw, expectedRaw),
		)

		for j := 0; j < len(expected); j++ {
			require.Equal(
				t, expected[j], actual.Value(j),
				fmt.Sprintf("column:  %v\nexpected: %v\nactual:  %v\n", columnName, expected, actual))
		}
	}
}

type Table struct {
	SchemaYdb *api_service_protos.TSchema
	Records   []*Record // Large tables may consist of multiple records
}

func (tb *Table) MatchRecords(t *testing.T, records []arrow.Record) {
	require.Equal(t, len(tb.Records), len(records))

	for i := range tb.Records {
		tb.Records[i].MatchRecord(t, tb.SchemaYdb, records[i])
		records[i].Release()
	}
}
