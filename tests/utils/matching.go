package utils

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/require"
)

type ArrowIDBuilder[ID TableIDTypes] interface {
	*array.Int64Builder | *array.Int32Builder
	Append(ID)
	NewArray() arrow.Array
	Release()
}

// Record is somewhat equivalent to arrow.Record.
// Store columns in map because order of columns in some datasources is undefined.
// (i.e. in YDB - https://st.yandex-team.ru/KIKIMR-20836)
type Record[ID TableIDTypes, IDBUILDER ArrowIDBuilder[ID]] struct {
	Columns map[string]any
}

type TableSchema struct {
	Columns map[string]*Ydb.Type
}

func (r *Record[ID, IDBUILDER]) MatchRecord(
	t *testing.T,
	receivedRecord arrow.Record,
	receivedSchema *api_service_protos.TSchema,
	idArrBuilder IDBUILDER) {
	fmt.Printf("\nMatchRecord: receivedRecord=%v, receivedSchema=%v\n", receivedRecord, receivedSchema)
	if receivedRecord == nil {
		fmt.Printf("MatchRecord: receivedRecord is nil\n")
		return
	}
	fmt.Printf("MatchRecord: record schema fields=%v\n", receivedRecord.Schema().Fields())
	fmt.Printf("MatchRecord: record num columns=%d, num rows=%d\n", receivedRecord.NumCols(), receivedRecord.NumRows())

	// Modify received table for the purpose of correct matching of expected vs actual results.
	recordWithColumnOrderFixed, schemaWithColumnOrderFixed := swapColumns(receivedRecord, receivedSchema)
	fmt.Printf("MatchRecord: after swapColumns record=%v, schema=%v\n", recordWithColumnOrderFixed, schemaWithColumnOrderFixed)
	recordWithRowsSorted := sortTableByID(recordWithColumnOrderFixed, idArrBuilder)
	fmt.Printf("MatchRecord: after sortTableByID record=%v\n", recordWithRowsSorted)

	for i, arrowField := range recordWithRowsSorted.Schema().Fields() {
		ydbType := schemaWithColumnOrderFixed.Columns[i].Type
		fmt.Printf("MatchRecord: processing field %s with type %v\n", arrowField.Name, ydbType)

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

// The swapColumns function swaps the "id" column with the first column in the Apache Arrow table.
// This is needed for further contract in the sortTableByID function,
// where the column with the name `id` should always come first.
func swapColumns(table arrow.Record, schema *api_service_protos.TSchema) (arrow.Record, *api_service_protos.TSchema) {
	fmt.Printf("\nswapColumns: table=%v, schema=%v\n", table, schema)
	if table == nil {
		fmt.Printf("swapColumns: table is nil\n")
		return nil, schema
	}

	fmt.Printf("swapColumns: table schema fields=%v\n", table.Schema().Fields())
	fmt.Printf("swapColumns: table num columns=%d, num rows=%d\n", table.NumCols(), table.NumRows())

	idIndex := -1
	for i, field := range table.Schema().Fields() {
		fmt.Printf("swapColumns: checking field %s at index %d\n", field.Name, i)
		if field.Name == "id" || field.Name == "ID" || field.Name == "COL_00_ID" || field.Name == "_id" || field.Name == "key" || field.Name == "COL_00_KEY" {
			idIndex = i
			break
		}
	}
	fmt.Printf("swapColumns: found id index=%d\n", idIndex)

	if idIndex == -1 {
		fmt.Printf("swapColumns: no id column found, returning original table\n")
		return table, schema
	}

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

	fmt.Printf("swapColumns: returning new table with %d columns and %d rows\n", newTable.NumCols(), newTable.NumRows())
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

type tableRow[ID TableIDTypes] struct {
	ID   ID
	Rest []any
}

func appendToBuilder[VT common.ValueType](builder common.ArrowBuilder[VT], val any) {
	if val == nil {
		builder.AppendNull()
	} else {
		builder.Append(val.(VT))
	}
}

type arrowIDCol[ID TableIDTypes] struct {
	idCol arrow.Array
}

func newTableIDColumn[ID TableIDTypes](arr arrow.Array) arrowIDCol[ID] {
	return arrowIDCol[ID]{arr}
}

func (c arrowIDCol[ID]) mustValue(i int) ID {
	switch col := c.idCol.(type) {
	case *array.Int32:
		return ID(col.Value(i))
	case *array.Int64:
		return ID(col.Value(i))
	case *array.String:
		// Для строковых ID используем строковое сравнение
		return ID(i) // Возвращаем индекс для сортировки
	case *array.Binary:
		// Для бинарных ID используем строковое сравнение
		return ID(i) // Возвращаем индекс для сортировки
	default:
		panic(fmt.Sprintf("Get value id value from arrowIDCol for %T", col))
	}
}

// This code creates a new instance of a table with the desired order of columns.
// The main purpose is to sort the table by the ID column while preserving the order of the other columns.
// This is necessary because the columns in Greenplum come in random order, and it is necessary to sort them
//

//nolint:funlen,gocyclo
func sortTableByID[ID TableIDTypes, IDBUILDER ArrowIDBuilder[ID]](table arrow.Record, idBuilder IDBUILDER) arrow.Record {
	records := make([]tableRow[ID], table.NumRows())

	idCol := newTableIDColumn[ID](table.Column(0))

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
			panic(fmt.Sprintf("UNSUPPORTED TYPE: %T", table.Column(colIdx)))
		}
	}

	for i := int64(0); i < table.NumRows(); i++ {
		records[i] = tableRow[ID]{
			ID:   idCol.mustValue(int(i)),
			Rest: restCols[i],
		}
	}

	// Сортируем по строковому значению ID
	switch idCol.idCol.(type) {
	case *array.String:
		sort.Slice(records, func(i, j int) bool {
			valI := idCol.idCol.(*array.String).Value(int(records[i].ID))
			valJ := idCol.idCol.(*array.String).Value(int(records[j].ID))
			return valI < valJ
		})
	case *array.Binary:
		sort.Slice(records, func(i, j int) bool {
			valI := string(idCol.idCol.(*array.Binary).Value(int(records[i].ID)))
			valJ := string(idCol.idCol.(*array.Binary).Value(int(records[j].ID)))
			return valI < valJ
		})
	default:
		sort.Slice(records, func(i, j int) bool {
			return records[i].ID < records[j].ID
		})
	}

	pool := memory.NewGoAllocator()
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
					panic(fmt.Sprintf("UNSUPPORTED TYPE: %T", table.Column(colIdx+1)))
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
				panic(fmt.Sprintf("UNSUPPORTED BUILDER TYPE: %T", builder))
			}
		}
	}

	idArr := idBuilder.NewArray()
	defer idArr.Release()

	restArrs := make([]arrow.Array, len(restBuilders))
	for idx, builder := range restBuilders {
		restArrs[idx] = builder.NewArray()
	}

	// Сохраняем оригинальные типы колонок
	fields := table.Schema().Fields()
	cols := append([]arrow.Array{table.Column(0)}, restArrs...)
	newTable := array.NewRecord(arrow.NewSchema(fields, nil), cols, int64(idArr.Len()))

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
		// Конвертируем бинарные данные в строки
		binaryArray := actual.(*array.Binary)
		stringArray := array.NewStringBuilder(memory.NewGoAllocator())
		for i := 0; i < binaryArray.Len(); i++ {
			if binaryArray.IsNull(i) {
				stringArray.AppendNull()
			} else {
				stringArray.Append(string(binaryArray.Value(i)))
			}
		}
		stringArrayResult := stringArray.NewArray()
		defer stringArrayResult.Release()

		// Сортируем строки
		if arrowField.Name == "key" {
			expectedStrings := expected.([]string)
			sort.Strings(expectedStrings)
		}

		matchArrays[string, *array.String](t, arrowField.Name, expected, stringArrayResult, optional)
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
			fmt.Sprintf("invalid type for column %v: expected=%T, actual=%T", columnName, expectedRaw, actualRaw),
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

type Table[T TableIDTypes, K ArrowIDBuilder[T]] struct {
	Name                  string
	Schema                *TableSchema
	Records               []*Record[T, K] // Large tables may consist of multiple records
	IDArrayBuilderFactory func() K
}

func (tb *Table[T, K]) MatchRecords(t *testing.T, records []arrow.Record, schema *api_service_protos.TSchema) {
	require.Equal(t, len(tb.Records), len(records))

	for i := range tb.Records {
		idArrayBuilder := tb.IDArrayBuilderFactory()
		tb.Records[i].MatchRecord(t, records[i], schema, idArrayBuilder)
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
