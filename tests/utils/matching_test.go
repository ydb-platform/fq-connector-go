package utils

import (
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
)

func TestSortTableByID(t *testing.T) {
	pool := memory.NewGoAllocator()

	t.Run("Test single row table", func(t *testing.T) {
		idBuilder := array.NewInt32Builder(pool)
		idBuilder.Append(1)
		idArr := idBuilder.NewArray()

		defer idArr.Release()

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		}, nil)
		table := array.NewRecord(schema, []arrow.Array{idArr}, 1)

		sortedTable := sortTableByID(table)

		require.Equal(t, int64(1), sortedTable.NumRows())
		require.Equal(t, int32(1), sortedTable.Column(0).(*array.Int32).Value(0))
		require.Equal(t, table.Schema(), sortedTable.Schema())
	})

	t.Run("Test multiple rows table", func(t *testing.T) {
		idBuilder := array.NewInt32Builder(pool)
		idBuilder.AppendValues([]int32{3, 1, 2}, nil)
		idArr := idBuilder.NewArray()

		defer idArr.Release()

		stringBuilder := array.NewStringBuilder(pool)
		stringBuilder.AppendValues([]string{"three", "one", "two"}, nil)
		stringArr := stringBuilder.NewArray()

		defer stringArr.Release()

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "value", Type: arrow.BinaryTypes.String},
		}, nil)
		table := array.NewRecord(schema, []arrow.Array{idArr, stringArr}, 3)

		sortedTable := sortTableByID(table)

		expectedIDValues := []int32{1, 2, 3}
		expectedStringValues := []string{"one", "two", "three"}

		require.Equal(t, int64(3), sortedTable.NumRows())

		for i := int64(0); i < sortedTable.NumRows(); i++ {
			require.Equal(t, expectedIDValues[i], sortedTable.Column(0).(*array.Int32).Value(int(i)))
			require.Equal(t, expectedStringValues[i], sortedTable.Column(1).(*array.String).Value(int(i)))
		}
	})

	t.Run("Test with different data types", func(t *testing.T) {
		idBuilder := array.NewInt32Builder(pool)
		idBuilder.AppendValues([]int32{2, 3, 1}, nil)
		idArr := idBuilder.NewArray()

		defer idArr.Release()

		int64Builder := array.NewInt64Builder(pool)
		int64Builder.AppendValues([]int64{200, 300, 100}, nil)
		int64Arr := int64Builder.NewArray()

		defer int64Arr.Release()

		float32Builder := array.NewFloat32Builder(pool)
		float32Builder.AppendValues([]float32{2.2, 3.3, 1.1}, nil)
		float32Arr := float32Builder.NewArray()

		defer float32Arr.Release()

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "value1", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value2", Type: arrow.PrimitiveTypes.Float32},
		}, nil)
		table := array.NewRecord(schema, []arrow.Array{idArr, int64Arr, float32Arr}, 3)

		sortedTable := sortTableByID(table)

		expectedIDValues := []int32{1, 2, 3}
		expectedInt64Values := []int64{100, 200, 300}
		expectedFloat32Values := []float32{1.1, 2.2, 3.3}

		require.Equal(t, int64(3), sortedTable.NumRows())

		for i := int64(0); i < sortedTable.NumRows(); i++ {
			require.Equal(t, expectedIDValues[i], sortedTable.Column(0).(*array.Int32).Value(int(i)))
			require.Equal(t, expectedInt64Values[i], sortedTable.Column(1).(*array.Int64).Value(int(i)))
			require.Equal(t, expectedFloat32Values[i], sortedTable.Column(2).(*array.Float32).Value(int(i)))
		}
	})

	t.Run("Test with optional int values", func(t *testing.T) {
		idBuilder := array.NewInt32Builder(pool)
		idBuilder.AppendValues([]int32{3, 1, 2}, nil)
		idArr := idBuilder.NewArray()

		defer idArr.Release()

		int32Builder := array.NewInt32Builder(pool)
		int32Builder.AppendValues([]int32{30, 0, 20}, []bool{true, false, true})
		int32Arr := int32Builder.NewArray()

		defer int32Arr.Release()

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "value", Type: arrow.PrimitiveTypes.Int32},
		}, nil)
		table := array.NewRecord(schema, []arrow.Array{idArr, int32Arr}, 3)

		sortedTable := sortTableByID(table)

		expectedIDValues := []int32{1, 2, 3}
		expectedInt32Values := []*int32{nil, ptr.Int32(20), ptr.Int32(30)}

		require.Equal(t, int64(3), sortedTable.NumRows())

		for i := int64(0); i < sortedTable.NumRows(); i++ {
			require.Equal(t, expectedIDValues[i], sortedTable.Column(0).(*array.Int32).Value(int(i)))

			if expectedInt32Values[i] == nil {
				require.True(t, sortedTable.Column(1).IsNull(int(i)))
			} else {
				require.Equal(t, *expectedInt32Values[i], sortedTable.Column(1).(*array.Int32).Value(int(i)))
			}
		}
	})
}
