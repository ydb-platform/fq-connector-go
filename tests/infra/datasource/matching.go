package datasource

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
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
	sorted, _ := sortRecordByID(record)

	fmt.Println(sorted)

	for i, arrowField := range record.Schema().Fields() {
		columnName := schema.Columns[i].Name
		ydbType := schema.Columns[i].Type

		switch ydbType.GetType().(type) {
		case *Ydb.Type_TypeId:
			matchColumns(t, arrowField, r.Columns[columnName], sorted.Column(i), false)
		case *Ydb.Type_OptionalType:
			matchColumns(t, arrowField, r.Columns[columnName], sorted.Column(i), true)
		default:
			require.FailNow(t, fmt.Sprintf("unexpected YDB type: %v", ydbType))
		}
	}
}

func sortRecordByID(record arrow.Record) (arrow.Record, error) {
	if record == nil {
		return nil, errors.New("record is nil")
	}

	if record.NumCols() == 0 {
		return nil, errors.New("record has no columns")
	}

	idColumn := record.Column(0)
	if idColumn == nil {
		return nil, errors.New("id column is nil")
	}

	type idIndexPair struct {
		id    string
		index int64
	}

	numRows := record.NumRows()
	pairs := make([]idIndexPair, numRows)

	for i := int64(0); i < numRows; i++ {
		pairs[i] = idIndexPair{
			id:    idColumn.ValueStr(int(i)),
			index: i,
		}
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].id < pairs[j].id
	})

	var sortedColumns = make([]arrow.Array, record.NumCols())
	for i := range sortedColumns {
		col := record.Column(i)
		if col == nil {
			return nil, fmt.Errorf("column %d is nil", i)
		}
		col.Retain()
		sortedColumns[i] = col
	}

	for i := int64(0); i < numRows; i++ {
		for j := 0; j < int(record.NumCols()); j++ {
			sortedColumns[j].Release()
			sortedColumns[j] = nil
		}
		for j := 0; j < int(record.NumCols()); j++ {
			rowSlice := record.NewSlice(pairs[i].index, pairs[i].index+1)
			if rowSlice == nil {
				return nil, errors.New("row slice is nil")
			}
			col := rowSlice.Column(j)
			if col == nil {
				return nil, fmt.Errorf("column %d is nil in row slice", j)
			}
			col.Retain()
			sortedColumns[j] = col
		}
	}

	var sortedRecord arrow.Record
	var err error

	for i, col := range sortedColumns {
		sortedRecord, err = sortedRecord.SetColumn(i, col)
		if err != nil {
			return nil, err
		}
		col.Release()
	}

	return sortedRecord, nil
}

func (r *Record) sortColumnsByID() {
	ids := r.Columns["id"].([]*int32)

	log.Println(ids)

	indexes := make([]int, len(ids))
	for i := range ids {
		indexes[i] = i
	}

	sort.Slice(indexes, func(i, j int) bool {
		return *ids[indexes[i]] < *ids[indexes[j]]
	})

	sortedColumns := make(map[string]any)

	for colName, colData := range r.Columns {
		slice, ok := colData.([]any)
		if !ok {
			sortedColumns[colName] = colData
			continue
		}

		sortedSlice := make([]any, len(slice))
		for i, index := range indexes {
			sortedSlice[i] = slice[index]
		}
		sortedColumns[colName] = sortedSlice
	}

	log.Println(r.Columns)
	log.Println(sortedColumns)

	r.Columns = sortedColumns
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
