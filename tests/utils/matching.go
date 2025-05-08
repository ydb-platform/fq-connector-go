package utils

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

type ArrowIDBuilder[ID TableIDTypes] interface {
	*array.Int64Builder | *array.Int32Builder | *array.BinaryBuilder | *array.StringBuilder
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
	// Modify received table for the purpose of correct matching of expected vs actual results.
	recordWithColumnOrderFixed, schemaWithColumnOrderFixed := swapColumns(receivedRecord, receivedSchema)
	recordWithRowsSorted := sortTableByID(recordWithColumnOrderFixed, idArrBuilder)

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

// The swapColumns function swaps the "id" column with the first column in the Apache Arrow table.
// This is needed for further contract in the sortTableByID function,
// where the column with the name `id` should always come first.
func swapColumns(table arrow.Record, schema *api_service_protos.TSchema) (arrow.Record, *api_service_protos.TSchema) {
	idIndex := -1

	for i, field := range table.Schema().Fields() {
		if field.Name == "id" || field.Name == "ID" || field.Name == "COL_00_ID" || field.Name == "_id" || field.Name == "key" {
			idIndex = i
			break
		}
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
		return any(col.Value(i)).(ID)
	case *array.Int64:
		return any(col.Value(i)).(ID)
	case *array.Binary:
		return any(col.Value(i)).(ID)
	case *array.String:
		return any(col.Value(i)).(ID)
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
		switch col := table.Column(colIdx).(type) {
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
		//nolint:revive
		case *array.Struct:
			// Обработка для структурных типов
			numRows := int(table.NumRows())
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				if len(restCols[rowIdx]) == 0 {
					restCols[rowIdx] = make([]any, table.NumCols()-1)
				}

				// Для struct мы сохраняем его как есть
				if col.IsNull(rowIdx) {
					restCols[rowIdx][colIdx-1] = nil
				} else {
					// Создаем структуру для сохранения значений полей
					structData := make(map[string]any)

					for fieldIdx := 0; fieldIdx < col.NumField(); fieldIdx++ {
						fieldName := col.DataType().(*arrow.StructType).Field(fieldIdx).Name

						if col.Field(fieldIdx).IsNull(rowIdx) {
							structData[fieldName] = nil
						} else {
							// Получаем значение в зависимости от типа поля
							switch field := col.Field(fieldIdx).(type) {
							case *array.Uint8:
								structData[fieldName] = field.Value(rowIdx)
							case *array.Int32:
								structData[fieldName] = field.Value(rowIdx)
							case *array.Int64:
								structData[fieldName] = field.Value(rowIdx)
							case *array.Uint64:
								structData[fieldName] = field.Value(rowIdx)
							case *array.Float32:
								structData[fieldName] = field.Value(rowIdx)
							case *array.Float64:
								structData[fieldName] = field.Value(rowIdx)
							case *array.String:
								structData[fieldName] = field.Value(rowIdx)
							case *array.Binary:
								structData[fieldName] = field.Value(rowIdx)
							default:
								panic(fmt.Sprintf("Expected fieldBuilder to have supported types but got %T", field))
							}
						}
					}

					restCols[rowIdx][colIdx-1] = structData
				}
			}
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

	var idType ID
	if _, ok := any(idType).(int32); ok {
		sort.Slice(records, func(i, j int) bool {
			return any(records[i].ID).(int32) < any(records[j].ID).(int32)
		})
	} else if _, ok := any(idType).(int64); ok {
		sort.Slice(records, func(i, j int) bool {
			return any(records[i].ID).(int64) < any(records[j].ID).(int64)
		})
	} else if _, ok := any(idType).([]byte); ok {
		sort.Slice(records, func(i, j int) bool {
			return bytes.Compare(any(records[i].ID).([]byte), any(records[j].ID).([]byte)) < 0
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
				case *array.Struct:
					// Создаем новый StructBuilder на основе существующего типа
					structType := table.Column(colIdx + 1).DataType().(*arrow.StructType)
					restBuilders[colIdx] = array.NewStructBuilder(pool, structType)
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
			case *array.StructBuilder:
				// Обработка структуры
				if val == nil {
					builder.AppendNull()
				} else {
					// Конвертируем val в map[string]any
					structData, ok := val.(map[string]any)
					if !ok {
						panic(fmt.Sprintf("Expected map[string]any but got %T", val))
					}

					builder.Append(true) // Начинаем новую структуру

					// Для каждого поля в структуре добавляем значение
					for fieldIdx := 0; fieldIdx < builder.NumField(); fieldIdx++ {
						fieldName := builder.Type().(*arrow.StructType).Field(fieldIdx).Name
						fieldBuilder := builder.FieldBuilder(fieldIdx)

						fieldValue := structData[fieldName]
						if fieldValue == nil {
							fieldBuilder.AppendNull()
							continue
						}

						switch fb := fieldBuilder.(type) {
						case *array.Uint8Builder:
							uint8val, ok := fieldValue.(uint8)
							if !ok {
								panic(fmt.Sprintf("Expected uint8 but got %T", uint8val))
							}

							fb.Append(uint8val)
						case *array.Int32Builder:
							int32val, ok := fieldValue.(int32)
							if !ok {
								panic(fmt.Sprintf("Expected int32 but got %T", int32val))
							}

							fb.Append(int32val)
						case *array.Int64Builder:
							int64val, ok := fieldValue.(int64)
							if !ok {
								panic(fmt.Sprintf("Expected int64 but got %T", int64val))
							}

							fb.Append(int64val)
						case *array.Uint64Builder:
							uint64val, ok := fieldValue.(uint64)
							if !ok {
								panic(fmt.Sprintf("Expected uint64 but got %T", uint64val))
							}

							fb.Append(uint64val)
						case *array.Float32Builder:
							float32val, ok := fieldValue.(float32)
							if !ok {
								panic(fmt.Sprintf("Expected float32 but got %T", float32val))
							}

							fb.Append(float32val)
						case *array.Float64Builder:
							float64val, ok := fieldValue.(float64)
							if !ok {
								panic(fmt.Sprintf("Expected float64 but got %T", float64val))
							}

							fb.Append(float64val)
						case *array.StringBuilder:
							strval, ok := fieldValue.(string)
							if !ok {
								panic(fmt.Sprintf("Expected string but got %T", strval))
							}

							fb.Append(strval)
						case *array.BinaryBuilder:
							bval, ok := fieldValue.([]byte)
							if !ok {
								panic(fmt.Sprintf("Expected []byte but got %T", bval))
							}

							fb.Append(bval)
						default:
							panic(fmt.Sprintf("Expected fieldBuilder to have supported types but got %T", fb))
						}
					}
				}
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
	case arrow.STRUCT:
		matchStructArrays(t, arrowField.Name, expected, actual.(*array.Struct), optional)
	default:
		require.FailNow(t, fmt.Sprintf("unexpected arrow type: %v", arrowField.Type.ID().String()))
	}
}

// matchStructArrays обрабатывает сравнение структурных типов Arrow
// Важно: для интеграции с YDB все структуры должны быть optional (обернуты в OptionalType),
// а все поля внутри структур должны быть nullable. Эта функция проверяет оба этих условия,
// чтобы гарантировать правильную передачу данных между YDB и Arrow.
func matchStructArrays(
	t *testing.T,
	columnName string,
	expectedRaw any,
	actual *array.Struct,
	optional bool,
) {
	require.True(t, optional, "Struct columns must be optional in Arrow")

	// Дополнительная проверка, что все поля структуры также являются nullable
	dataType := actual.DataType().(*arrow.StructType)
	for i := 0; i < len(dataType.Fields()); i++ {
		field := dataType.Field(i)
		require.True(t, field.Nullable, fmt.Sprintf("struct field %s must be nullable", field.Name))
	}

	// Для структурных типов мы проверяем каждое поле отдельно
	expectedStructsBytes, ok := expectedRaw.([]map[string]*any)
	require.True(t, ok, fmt.Sprintf("invalid type for struct column %v: expected=[]map[string]any, got %T",
		columnName, expectedRaw))

	// Новый формат - []map[string]any
	require.Equal(t, len(expectedStructsBytes), actual.Len(),
		fmt.Sprintf("struct column:  %v\nexpected length: %d\nactual length:  %d\n",
			columnName, len(expectedStructsBytes), actual.Len()),
	)

	for i := 0; i < len(expectedStructsBytes); i++ {
		// Если ожидается nil, проверяем что значение в Arrow тоже null
		if expectedStructsBytes[i] == nil {
			require.True(t, actual.IsNull(i),
				fmt.Sprintf("struct column:  %v\nexpected NULL at index %d, got non-NULL\n", columnName, i))
			continue
		}

		// Если ожидается не-nil, проверяем что значение в Arrow не null
		require.False(t, actual.IsNull(i),
			fmt.Sprintf("struct column:  %v\nexpected non-NULL at index %d, got NULL\n", columnName, i))

		// Проверяем каждое поле структуры
		expectedStruct := expectedStructsBytes[i]

		for fieldIdx := 0; fieldIdx < actual.NumField(); fieldIdx++ {
			fieldName := actual.DataType().(*arrow.StructType).Field(fieldIdx).Name
			fieldArray := actual.Field(fieldIdx)

			// Получаем ожидаемое значение для поля
			expectedFieldValue := expectedStruct[fieldName]

			// Если поле не существует или null, проверяем что Arrow тоже null
			if expectedFieldValue == nil {
				require.True(t, fieldArray.IsNull(i),
					fmt.Sprintf("struct field %s: expected NULL at row %d, got non-NULL", fieldName, i))
				continue
			}

			// Проверяем значение в зависимости от типа поля
			switch field := fieldArray.(type) {
			case *array.Uint8:
				require.Equal(t, *expectedFieldValue, field.Value(i),
					fmt.Sprintf("Field %s values mismatch at row %d", fieldName, i))
			case *array.Int32:
				require.Equal(t, *expectedFieldValue, field.Value(i),
					fmt.Sprintf("Field %s values mismatch at row %d", fieldName, i))
			case *array.Int64:
				require.Equal(t, *expectedFieldValue, field.Value(i),
					fmt.Sprintf("Field %s values mismatch at row %d", fieldName, i))
			case *array.Uint64:
				require.Equal(t, *expectedFieldValue, field.Value(i),
					fmt.Sprintf("Field %s values mismatch at row %d", fieldName, i))
			case *array.Float32:
				require.Equal(t, *expectedFieldValue, field.Value(i),
					fmt.Sprintf("Field %s values mismatch at row %d", fieldName, i))
			case *array.Float64:
				require.Equal(t, *expectedFieldValue, field.Value(i),
					fmt.Sprintf("Field %s values mismatch at row %d", fieldName, i))
			case *array.String:
				require.Equal(t, *expectedFieldValue, field.Value(i),
					fmt.Sprintf("Field %s values mismatch at row %d", fieldName, i))
			case *array.Binary:
				require.Equal(t, *expectedFieldValue, field.Value(i),
					fmt.Sprintf("Field %s values mismatch at row %d", fieldName, i))
			default:
				// Другие типы полей можно добавить при необходимости
				require.FailNow(t, fmt.Sprintf("unsupported field type for %s: %T", fieldName, field))
			}
		}
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
