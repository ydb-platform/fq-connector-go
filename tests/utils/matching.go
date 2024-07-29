package utils

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"golang.org/x/exp/constraints"
	"google.golang.org/protobuf/proto"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/require"
)

// type ArrowIDBuilder[T constraints.Integer] interface {
// 	array.Int64Builder | array.Int32Builder
// 	Append(T)
// }

// Record is somewhat equivalent to arrow.Record.
// Store columns in map because order of columns in some datasource is undefined.
// (i.e. in YDB - https://st.yandex-team.ru/KIKIMR-20836)
type Record[T constraints.Integer, K array.Int64Builder | array.Int32Builder] struct {
	Columns map[string]any
}

// TODO FIXME: remove after debug
// type Record struct {
// 	Columns map[string]any
// }

type TableSchema struct {
	Columns map[string]*Ydb.Type
}

func (r *Record[T, K]) MatchRecord(t *testing.T, receivedRecord arrow.Record, receivedSchema *api_service_protos.TSchema) {
	// Modify received table for the purpose of correct matching of expected vs actual results.
	recordWithColumnOrderFixed, schemaWithColumnOrderFixed := swapColumns(receivedRecord, receivedSchema)
	// recordWithRowsSorted := sortTableByID(recordWithColumnOrderFixed)
	recordWithRowsSorted := sortTableByID[T, K](recordWithColumnOrderFixed) // TODO FIXME: remove after debug

	for i, arrowField := range recordWithRowsSorted.Schema().Fields() {
		ydbType := schemaWithColumnOrderFixed.Columns[i].Type

		switch ydbType.GetType().(type) {
		case *Ydb.Type_TypeId:
			matchColumns(t, arrowField, r.Columns[arrowField.Name], recordWithRowsSorted.Column(i), false)
		case *Ydb.Type_OptionalType:
			matchColumns(t, arrowField, r.Columns[arrowField.Name], recordWithRowsSorted.Column(i), true)
		default:
			require.FailNow(t, fmt.Sprintf("unexpected YDB type: %v", ydbType))
		}
	}
}

// The swapColumns function swaps the “id” column with the first column in the Apache Arrow table.
// This is needed for further contract in the sortTableByID function,
// where the column with the name `id` should always come first.
func swapColumns(table arrow.Record, schema *api_service_protos.TSchema) (arrow.Record, *api_service_protos.TSchema) {
	idIndex := -1

	for i, field := range table.Schema().Fields() {
		if field.Name == "id" {
			idIndex = i
			break
		}
		// TODO: FIXME: remove after debug
		if field.Name == "ID" {
			idIndex = i
			break
		}
	}
	// TODO: FIXME: remove after debug
	// fmt.Printf("table arrow.Record: %v\n", table)

	// build new record with the correct order of columns
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
	newTable := array.NewRecord(arrow.NewSchema(fields, nil), newColumns, table.NumRows())

	// fix order in table schema as well
	newSchema := proto.Clone(schema).(*api_service_protos.TSchema)
	newSchema.Columns[0], newSchema.Columns[idIndex] = newSchema.Columns[idIndex], newSchema.Columns[0]

	return newTable, newSchema
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

type tableRow[T constraints.Integer] struct {
	// ID int32
	// ID int64
	// type tableRow[T constraints.Integer] struct {
	ID   T // TODO: FIXME: remove after debug
	Rest []any
}

func appendToBuilder[VT common.ValueType](builder common.ArrowBuilder[VT], val any) {
	if val == nil {
		builder.AppendNull()
	} else {
		builder.Append(val.(VT))
	}
}

// TODO: FIXME: remove after debug
// // possible to make generic implementation, but then it will need to make a lot of changes to all tests
type arrowIDCol[T constraints.Integer] struct {
	idCol arrow.Array
}

func newTableIDColumn[T constraints.Integer](arr arrow.Array) arrowIDCol[T] {
	return arrowIDCol[T]{arr}
}

func (c arrowIDCol[T]) mustValue(i int) T {
	switch col := c.idCol.(type) {
	case *array.Int32:
		return T(col.Value(i))
	case *array.Int64:
		return T(col.Value(i))
	default:
		panic(fmt.Sprintf("Get value id value from arrowIDCol for %T", col))
	}
}

type arrowIDBuilder[T constraints.Integer, K array.Int64Builder | array.Int32Builder] struct {
	builder array.Builder
}

func newArrowIDBuilder[T constraints.Integer, K array.Int64Builder | array.Int32Builder](pool memory.Allocator, idCol any) arrowIDBuilder[T, K] {
	switch idCol.(type) {
	case arrowIDCol[int64]:
		concBuilder := array.NewInt64Builder(pool)
		return arrowIDBuilder[T, K]{builder: concBuilder}
	case arrowIDCol[int32]:
		concBuilder := array.NewInt32Builder(pool)
		return arrowIDBuilder[T, K]{builder: concBuilder}
	default:
		panic(fmt.Sprintf("New Arrow ID Builder with ID col type %T", idCol))
	}
}

func (b arrowIDBuilder[T, K]) mustAppend(val T) {
	switch b := b.builder.(type) {
	case *array.Int64Builder:
		b.Append(int64(val))
		return
	case *array.Int32Builder:
		b.Append(int32(val))
	default:
		panic(fmt.Sprintf("While append unknown builder %T", b))
	}
}

func (b arrowIDBuilder[_, _]) newArray() arrow.Array {
	return b.builder.NewArray()
}

// This code creates a new instance of a table with the desired order of columns.
// The main purpose is to sort the table by the ID column while preserving the order of the other columns.
// This is necessary because the columns in Greenplum come in random order, and it is necessary to sort them
//

// nolint:funlen,gocyclo
func sortTableByID[T constraints.Integer, K array.Int64Builder | array.Int32Builder](table arrow.Record) arrow.Record {
	records := make([]tableRow[T], table.NumRows())

	// //nolint:funlen,gocyclo
	// func sortTableByID(table arrow.Record) arrow.Record {
	// 	records := make([]tableRow, table.NumRows())

	// idCol := table.Column(0).(*array.Int32)
	// idCol := table.Column(0).(*array.Int64) // TODO: FIXME: remove after debug
	idCol := newTableIDColumn[T](table.Column(0))

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
		// TODO: FIXME: remove after debug
		records[i] = tableRow[T]{
			ID: idCol.mustValue(int(i)),
			// records[i] = tableRow{
			// 	ID:   idCol.Value(int(i)),
			Rest: restCols[i],
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].ID < records[j].ID
	})

	pool := memory.NewGoAllocator()
	// idBuilder := array.NewInt32Builder(pool)
	idBuilder := newArrowIDBuilder[T, K](pool, idCol)
	// idBuilder := array.NewInt64Builder(pool) // TODO: FIXME: remove after debug
	restBuilders := make([]array.Builder, table.NumCols()-1)

	for _, r := range records {
		idBuilder.mustAppend(r.ID) // TODO: FIXME: remove after debug
		// idBuilder.Append(r.ID)

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
			case *array.Int8Builder:
				appendToBuilder(builder, val)
			case *array.Int16Builder:
				appendToBuilder(builder, val)
			case *array.Int32Builder:
				appendToBuilder(builder, val)
			case *array.Int64Builder:
				appendToBuilder(builder, val)
			case *array.Uint8Builder:
				appendToBuilder(builder, val)
			case *array.Uint16Builder:
				appendToBuilder(builder, val)
			case *array.Uint32Builder:
				appendToBuilder(builder, val)
			case *array.Uint64Builder:
				appendToBuilder(builder, val)
			case *array.Float32Builder:
				appendToBuilder(builder, val)
			case *array.Float64Builder:
				appendToBuilder(builder, val)
			case *array.StringBuilder:
				appendToBuilder(builder, val)
			case *array.NullBuilder:
				builder.AppendNull()
			case *array.BinaryBuilder:
				appendToBuilder(builder, val)
			default:
				panic("UNSUPPORTED TYPE")
			}
		}
	}

	idArr := idBuilder.newArray() // TODO FIXME: remove after debug
	// idArr := idBuilder.NewArray()
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
		if strings.Contains(arrowField.Name, "json") {
			matchJSONArrays(t, arrowField.Name, expected, actual.(*array.String), optional)
		} else {
			matchArrays[string, *array.String](t, arrowField.Name, expected, actual, optional)
		}
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
			fmt.Sprintf("invalid type for column %v: %T", columnName, expectedRaw),
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

// A separate function matching JSON columns appeared due to MySQL:
// it stores JSONs as binary objects (BSON) and reorders JSON fields.
func matchJSONArrays(
	t *testing.T,
	columnName string,
	expectedRaw any,
	actual *array.String,
	optional bool,
) {
	if optional {
		expected, ok := expectedRaw.([]*string)
		require.True(
			t, ok,
			fmt.Sprintf("invalid type for column %v: %T", columnName, expectedRaw),
		)
		require.Equal(t, len(expected), actual.Len(),
			fmt.Sprintf("column:  %v\nexpected: %v\nactual:  %v\n", columnName, expected, actual),
		)

		for j := 0; j < len(expected); j++ {
			if expected[j] != nil {
				require.JSONEq(
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
		expected, ok := expectedRaw.([]string)
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

type Table[T constraints.Integer, K array.Int64Builder | array.Int32Builder] struct { // TODO FIXME: remove after debug
	// type Table struct {
	Name   string
	Schema *TableSchema
	// Records []*Record // Large tables may consist of multiple records // TODO FIXME: remove after debug
	Records []*Record[T, K] // Large tables may consist of multiple records // TODO FIXME: remove after debug
}

func (tb *Table[_, _]) MatchRecords(t *testing.T, records []arrow.Record, schema *api_service_protos.TSchema) {
	require.Equal(t, len(tb.Records), len(records))

	for i := range tb.Records {
		tb.Records[i].MatchRecord(t, records[i], schema)
		records[i].Release()
	}
}

func (tb *Table[_, _]) MatchSchema(t *testing.T, schema *api_service_protos.TSchema) {
	require.Equal(t, len(tb.Schema.Columns), len(schema.Columns),
		fmt.Sprintf(
			"incorrect number of column, expected: %d\nactual:   %d\n",
			len(tb.Schema.Columns),
			len(schema.Columns),
		))

	for _, column := range schema.Columns {
		require.Equal(t, tb.Schema.Columns[column.Name], column.Type,
			fmt.Sprintf(
				"incorrect column types, expected: %v\nactual:   %v\n",
				tb.Schema,
				schema,
			))
	}
}
