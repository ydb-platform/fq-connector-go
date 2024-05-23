package datasource

import (
	"fmt"
	"sort"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/require"
)

// Record is somewhat equivalent to arrow.Record
// Store columns in map because order of columns in some datasource is undefined
// (i.e. in YDB - https://st.yandex-team.ru/KIKIMR-20836)
type Record struct {
	Columns map[string]any
}

type TableSchema struct {
	Columns map[string]*Ydb.Type
}

func (r *Record) MatchRecord(t *testing.T, record arrow.Record, schema *api_service_protos.TSchema) {
	record = swapColumns(record)
	record = sortTableByID(record)

	for i, arrowField := range record.Schema().Fields() {
		ydbType := schema.Columns[i].Type

		switch ydbType.GetType().(type) {
		case *Ydb.Type_TypeId:
			matchColumns(t, arrowField, r.Columns[arrowField.Name], record.Column(i), false)
		case *Ydb.Type_OptionalType:
			matchColumns(t, arrowField, r.Columns[arrowField.Name], record.Column(i), true)
		default:
			require.FailNow(t, fmt.Sprintf("unexpected YDB type: %v", ydbType))
		}
	}
}

func swapColumns(table arrow.Record) arrow.Record {
	idIndex := -1

	for i, field := range table.Schema().Fields() {
		if field.Name == "id" {
			idIndex = i
			break
		}
	}

	newColumns := make([]arrow.Array, table.NumCols())
	for i := range newColumns {
		if i == 0 {
			newColumns[i] = table.Column(idIndex)
		} else if i == idIndex {
			newColumns[i] = table.Column(0)
		} else {
			newColumns[i] = table.Column(i)
		}
	}

	fields := table.Schema().Fields()
	fields[0], fields[idIndex] = fields[idIndex], fields[0]
	newSchema := arrow.NewSchema(fields, nil)

	newTable := array.NewRecord(newSchema, newColumns, table.NumRows())

	return newTable
}

func processColumn[VT common.ValueType, ARRAY common.ArrowArrayType[VT]](table arrow.Record, colIdx int, restCols [][]any) {
	col := table.Column(colIdx).(ARRAY)
	numRows := int(table.NumRows())

	for rowIdx := int(0); rowIdx < numRows; rowIdx++ {
		if len(restCols[rowIdx]) == 0 {
			restCols[rowIdx] = make([]any, table.NumCols()-1)
		}

		if col.IsNull(rowIdx) {
			restCols[rowIdx][colIdx-1] = nil
		} else {
			restCols[rowIdx][colIdx-1] = col.Value(rowIdx)
		}
	}
}

type tableRow struct {
	ID   int32
	Rest []any
}

func appendToBuilder[VT common.ValueType](builder common.ArrowBuilder[VT], val any) {
	if val == nil {
		builder.AppendNull()
	} else {
		builder.Append(val.(VT))
	}
}

// This code creates a new instance of a table with the desired order of columns.
// The main purpose is to sort the table by the ID column while preserving the order of the other columns.
// This is necessary because the columns in Greenplum come in random order, and it is necessary to sort them
//
//nolint:funlen,gocyclo
func sortTableByID(table arrow.Record) arrow.Record {
	records := make([]tableRow, table.NumRows())

	idCol := table.Column(0).(*array.Int32)

	restCols := make([][]any, table.NumRows())

	for colIdx := 1; colIdx < int(table.NumCols()); colIdx++ {
		switch table.Column(colIdx).(type) {
		case *array.Int8:
			processColumn[int8, *array.Int8](table, colIdx, restCols)
		case *array.Int16:
			processColumn[int16, *array.Int16](table, colIdx, restCols)
		case *array.Int32:
			processColumn[int32, *array.Int32](table, colIdx, restCols)
		case *array.Int64:
			processColumn[int64, *array.Int64](table, colIdx, restCols)
		case *array.Uint8:
			processColumn[uint8, *array.Uint8](table, colIdx, restCols)
		case *array.Uint16:
			processColumn[uint16, *array.Uint16](table, colIdx, restCols)
		case *array.Uint32:
			processColumn[uint32, *array.Uint32](table, colIdx, restCols)
		case *array.Uint64:
			processColumn[uint64, *array.Uint64](table, colIdx, restCols)
		case *array.Float32:
			processColumn[float32, *array.Float32](table, colIdx, restCols)
		case *array.Float64:
			processColumn[float64, *array.Float64](table, colIdx, restCols)
		case *array.String:
			processColumn[string, *array.String](table, colIdx, restCols)
		case *array.Binary:
			processColumn[[]byte, *array.Binary](table, colIdx, restCols)
		default:
			panic("UNSUPPORTED TYPE")
		}
	}

	for i := int64(0); i < table.NumRows(); i++ {
		records[i] = tableRow{
			ID:   idCol.Value(int(i)),
			Rest: restCols[i],
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].ID < records[j].ID
	})

	pool := memory.NewGoAllocator()
	idBuilder := array.NewInt32Builder(pool)
	restBuilders := make([]array.Builder, table.NumCols()-1)

	for _, r := range records {
		idBuilder.Append(r.ID)

		for colIdx, val := range r.Rest {
			if restBuilders[colIdx] == nil {
				switch table.Column(colIdx + 1).(type) {
				case *array.Int8:
					restBuilders[colIdx] = array.NewInt8Builder(pool)
				case *array.Int16:
					restBuilders[colIdx] = array.NewInt16Builder(pool)
				case *array.Int32:
					restBuilders[colIdx] = array.NewInt32Builder(pool)
				case *array.Int64:
					restBuilders[colIdx] = array.NewInt64Builder(pool)
				case *array.Uint8:
					restBuilders[colIdx] = array.NewUint8Builder(pool)
				case *array.Uint16:
					restBuilders[colIdx] = array.NewUint16Builder(pool)
				case *array.Uint32:
					restBuilders[colIdx] = array.NewUint32Builder(pool)
				case *array.Uint64:
					restBuilders[colIdx] = array.NewUint64Builder(pool)
				case *array.Float32:
					restBuilders[colIdx] = array.NewFloat32Builder(pool)
				case *array.Float64:
					restBuilders[colIdx] = array.NewFloat64Builder(pool)
				case *array.String:
					restBuilders[colIdx] = array.NewStringBuilder(pool)
				case *array.Null:
					restBuilders[colIdx] = array.NewNullBuilder(pool)
				case *array.Binary:
					restBuilders[colIdx] = array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
				default:
					panic("UNSUPPORTED TYPE")
				}
			}

			switch builder := restBuilders[colIdx].(type) {
			case *array.Int32Builder:
				appendToBuilder(builder, val)
			case *array.Int64Builder:
				appendToBuilder(builder, val)
			case *array.StringBuilder:
				appendToBuilder(builder, val)
			case *array.Int16Builder:
				appendToBuilder(builder, val)
			case *array.Uint8Builder:
				appendToBuilder(builder, val)
			case *array.Float32Builder:
				appendToBuilder(builder, val)
			case *array.Float64Builder:
				appendToBuilder(builder, val)
			case *array.Uint64Builder:
				appendToBuilder(builder, val)
			case *array.Uint16Builder:
				appendToBuilder(builder, val)
			case *array.NullBuilder:
				builder.AppendNull()
			case *array.BinaryBuilder:
				appendToBuilder(builder, val)
			case *array.Int8Builder:
				appendToBuilder(builder, val)
			case *array.Uint32Builder:
				appendToBuilder(builder, val)
			default:
				panic("UNSUPPORTED TYPE")
			}
		}
	}

	idArr := idBuilder.NewArray()
	defer idArr.Release()

	restArrs := make([]arrow.Array, len(restBuilders))
	for idx, builder := range restBuilders {
		restArrs[idx] = builder.NewArray()
	}

	cols := append([]arrow.Array{idArr}, restArrs...)
	schema := table.Schema()
	newTable := array.NewRecord(schema, cols, int64(idArr.Len()))

	for idx := range restBuilders {
		restArrs[idx].Release()
	}

	return newTable
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
			fmt.Sprintf("invalid type for column %v: want %T, got %T", columnName, expectedRaw, expected),
		)
		require.Equal(t, len(expected), actual.Len(),
			fmt.Sprintf("column:  %v\nexpected: %v\nactual:  %v\n", columnName, expected, actual),
		)

		for j := 0; j < len(expected); j++ {
			if expected[j] != nil {
				require.Equal(
					t, *expected[j], actual.Value(j),
					fmt.Sprintf(
						"expected val: %v\nactual val: %v\ncolumn:  %v\nexpected: %v\nactual:  %v\n",
						*expected[j],
						actual.Value(j),
						columnName,
						expected,
						actual),
				)
			} else {
				require.True(t, actual.IsNull(j),
					fmt.Sprintf("column:  %v\nexpected: %v\nactual:  %v\n", columnName, expected, actual))
			}
		}
	} else {
		expected, ok := expectedRaw.([]EXPECTED)
		require.True(
			t, ok,
			fmt.Sprintf("invalid type for column %v: want %T, got %T", columnName, expectedRaw, expected),
		)

		require.Equal(t, len(expected), actual.Len(),
			fmt.Sprintf("column:  %v\nexpected: %v\nactual:  %v\n", columnName, expected, actual),
		)

		for j := 0; j < len(expected); j++ {
			require.Equal(
				t, expected[j], actual.Value(j),
				fmt.Sprintf("column:  %v\nexpected: %v\nactual:  %v\n", columnName, expected, actual))
		}
	}
}

type Table struct {
	Name    string
	Schema  *TableSchema
	Records []*Record // Large tables may consist of multiple records
}

func (tb *Table) MatchRecords(t *testing.T, records []arrow.Record, schema *api_service_protos.TSchema) {
	require.Equal(t, len(tb.Records), len(records))

	for i := range tb.Records {
		tb.Records[i].MatchRecord(t, records[i], schema)
		records[i].Release()
	}
}

func (tb *Table) MatchSchema(t *testing.T, schema *api_service_protos.TSchema) {
	require.Equal(t, len(schema.Columns), len(tb.Schema.Columns),
		fmt.Sprintf(
			"incorrect number of column, expected: %d\nactual:   %d\n",
			len(tb.Schema.Columns),
			len(schema.Columns),
		))

	for _, column := range schema.Columns {
		require.Equal(t, column.Type, tb.Schema.Columns[column.Name],
			fmt.Sprintf(
				"incorrect column types, expected: %v\nactual:   %v\n",
				tb.Schema,
				schema,
			))
	}
}
