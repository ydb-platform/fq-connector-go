package ydb

import (
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"golang.org/x/exp/constraints"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var memPool memory.Allocator = memory.NewGoAllocator()

// key - test case name
// value - table description
var tables = map[string]*test_utils.Table[int32, *array.Int32Builder]{
	"simple": {
		Name:                  "simple",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":   common.MakePrimitiveType(Ydb.Type_INT32),
				"col1": common.MakePrimitiveType(Ydb.Type_STRING),
				"col2": common.MakePrimitiveType(Ydb.Type_INT32),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id": []int32{1, 2, 3, 4, 5},
					"col1": [][]byte{
						[]byte("ydb_a"),
						[]byte("ydb_b"),
						[]byte("ydb_c"),
						[]byte("ydb_d"),
						[]byte("ydb_e"),
					},
					"col2": []int32{10, 20, 30, 40, 50},
				},
			},
		},
	},

	"primitives": {
		Name:                  "primitives",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":               common.MakePrimitiveType(Ydb.Type_INT32),
				"col_01_bool":      common.MakePrimitiveType(Ydb.Type_BOOL),
				"col_02_int8":      common.MakePrimitiveType(Ydb.Type_INT8),
				"col_03_int16":     common.MakePrimitiveType(Ydb.Type_INT16),
				"col_04_int32":     common.MakePrimitiveType(Ydb.Type_INT32),
				"col_05_int64":     common.MakePrimitiveType(Ydb.Type_INT64),
				"col_06_uint8":     common.MakePrimitiveType(Ydb.Type_UINT8),
				"col_07_uint16":    common.MakePrimitiveType(Ydb.Type_UINT16),
				"col_08_uint32":    common.MakePrimitiveType(Ydb.Type_UINT32),
				"col_09_uint64":    common.MakePrimitiveType(Ydb.Type_UINT64),
				"col_10_float":     common.MakePrimitiveType(Ydb.Type_FLOAT),
				"col_11_double":    common.MakePrimitiveType(Ydb.Type_DOUBLE),
				"col_12_string":    common.MakePrimitiveType(Ydb.Type_STRING),
				"col_13_utf8":      common.MakePrimitiveType(Ydb.Type_UTF8),
				"col_14_date":      common.MakePrimitiveType(Ydb.Type_DATE),
				"col_15_datetime":  common.MakePrimitiveType(Ydb.Type_DATETIME),
				"col_16_timestamp": common.MakePrimitiveType(Ydb.Type_TIMESTAMP),
				"col_17_json":      common.MakePrimitiveType(Ydb.Type_JSON),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":            []int32{1},
					"col_01_bool":   []uint8{0}, // []bool{false}
					"col_02_int8":   []int8{1},
					"col_03_int16":  []int16{-2},
					"col_04_int32":  []int32{3},
					"col_05_int64":  []int64{-4},
					"col_06_uint8":  []uint8{5},
					"col_07_uint16": []uint16{6},
					"col_08_uint32": []uint32{7},
					"col_09_uint64": []uint64{8},
					"col_10_float":  []float32{9.9},
					"col_11_double": []float64{-10.10},
					"col_12_string": [][]byte{[]byte("ая")},
					"col_13_utf8":   []string{"az"},
					"col_14_date": []uint16{
						common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC),
						),
					},
					"col_15_datetime": []uint32{
						common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, time.UTC),
						),
					},
					"col_16_timestamp": []uint64{
						common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC),
						),
					},
					"col_17_json": []string{"{ \"friends\" : " + // TODO: Add unicode tests
						"[{\"name\": \"James Holden\",\"age\": 35}," +
						"{\"name\": \"Naomi Nagata\",\"age\": 30}]}"},
				},
			},
		},
	},

	"optionals": {
		Name:                  "optionals",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":               common.MakePrimitiveType(Ydb.Type_INT32),
				"col_01_bool":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"col_02_int8":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT8)),
				"col_03_int16":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				"col_04_int32":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_05_int64":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"col_06_uint8":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT8)),
				"col_07_uint16":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT16)),
				"col_08_uint32":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT32)),
				"col_09_uint64":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT64)),
				"col_10_float":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				"col_11_double":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"col_12_string":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_13_utf8":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_14_date":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				"col_15_datetime":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
				"col_16_timestamp": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"col_17_json":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_JSON)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":            []int32{1, 2},
					"col_01_bool":   []*uint8{ptr.Uint8(1), nil}, // []*bool{true, nil}
					"col_02_int8":   []*int8{ptr.Int8(1), nil},
					"col_03_int16":  []*int16{ptr.Int16(-2), nil},
					"col_04_int32":  []*int32{ptr.Int32(3), nil},
					"col_05_int64":  []*int64{ptr.Int64(-4), nil},
					"col_06_uint8":  []*uint8{ptr.Uint8(5), nil},
					"col_07_uint16": []*uint16{ptr.Uint16(6), nil},
					"col_08_uint32": []*uint32{ptr.Uint32(7), nil},
					"col_09_uint64": []*uint64{ptr.Uint64(8), nil},
					"col_10_float":  []*float32{ptr.Float32(9.9), nil},
					"col_11_double": []*float64{ptr.Float64(-10.10), nil},
					"col_12_string": []*[]byte{ptr.T[[]byte]([]byte("ая")), nil},
					"col_13_utf8":   []*string{ptr.String("az"), nil},
					"col_14_date": []*uint16{
						ptr.Uint16(common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC),
						)),
						nil,
					},
					"col_15_datetime": []*uint32{
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, time.UTC),
						)),
						nil,
					},
					"col_16_timestamp": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC),
						)),
						nil,
					},
					"col_17_json": []*string{ptr.String("{ \"friends\" : " + // TODO: Add unicode tests
						"[{\"name\": \"James Holden\",\"age\": 35}," +
						"{\"name\": \"Naomi Nagata\",\"age\": 30}]}"), nil},
				},
			},
		},
	},

	"datetime_format_yql": {
		Name:                  "datetime",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":               common.MakePrimitiveType(Ydb.Type_INT32),
				"col_01_date":      common.MakePrimitiveType(Ydb.Type_DATE),
				"col_02_datetime":  common.MakePrimitiveType(Ydb.Type_DATETIME),
				"col_03_timestamp": common.MakePrimitiveType(Ydb.Type_TIMESTAMP),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id": []int32{1},
					"col_01_date": []uint16{
						common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC),
						),
					},

					"col_02_datetime": []uint32{
						common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, time.UTC),
						),
					},
					"col_03_timestamp": []uint64{
						common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123456000, time.UTC),
						),
					},
				},
			},
		},
	},

	"datetime_format_yql_pushdown_timestamp_EQ": {
		Name:                  "datetime",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":               common.MakePrimitiveType(Ydb.Type_INT32),
				"col_01_date":      common.MakePrimitiveType(Ydb.Type_DATE),
				"col_02_datetime":  common.MakePrimitiveType(Ydb.Type_DATETIME),
				"col_03_timestamp": common.MakePrimitiveType(Ydb.Type_TIMESTAMP),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id": []int32{1},
					"col_01_date": []uint16{
						common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC),
						),
					},

					"col_02_datetime": []uint32{
						common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, time.UTC),
						),
					},
					"col_03_timestamp": []uint64{
						common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123456000, time.UTC),
						),
					},
				},
			},
		},
	},

	// YQ-3338: YDB connector always returns date / time columns in YQL_FORMAT,
	// 	because it always fits YDB's date / time type value ranges
	"datetime_format_string": {
		Name:                  "datetime",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":               common.MakePrimitiveType(Ydb.Type_INT32),
				"col_01_date":      common.MakePrimitiveType(Ydb.Type_DATE),
				"col_02_datetime":  common.MakePrimitiveType(Ydb.Type_DATETIME),
				"col_03_timestamp": common.MakePrimitiveType(Ydb.Type_TIMESTAMP),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id": []int32{1},
					"col_01_date": []uint16{
						common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC),
						),
					},

					"col_02_datetime": []uint32{
						common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, time.UTC),
						),
					},
					"col_03_timestamp": []uint64{
						common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123456000, time.UTC),
						),
					},
				},
			},
		},
	},

	"pushdown_comparison_L": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []int32{1},
					"col_01_int":  []*int32{ptr.Int32(10)},
					"col_02_text": []*string{ptr.T("a")},
				},
			},
		},
	},

	"pushdown_comparison_LE": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []int32{1, 2},
					"col_01_int":  []*int32{ptr.Int32(10), ptr.Int32(20)},
					"col_02_text": []*string{ptr.T("a"), ptr.T("b")},
				},
			},
		},
	},
	"pushdown_comparison_EQ": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []int32{2},
					"col_01_int":  []*int32{ptr.Int32(20)},
					"col_02_text": []*string{ptr.T("b")},
				},
			},
		},
	},
	// YQ-3711:
	// SELECT * FROM table WHERE x = NULL never returns rows.
	"pushdown_comparison_EQ_NULL": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records:               []*test_utils.Record[int32, *array.Int32Builder]{},
	},
	"pushdown_comparison_GE": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []int32{2, 3},
					"col_01_int":  []*int32{ptr.Int32(20), ptr.Int32(30)},
					"col_02_text": []*string{ptr.T("b"), ptr.T("c")},
				},
			},
		},
	},
	"pushdown_comparison_G": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []int32{1, 2, 3},
					"col_01_int":  []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30)},
					"col_02_text": []*string{ptr.T("a"), ptr.T("b"), ptr.T("c")},
				},
			},
		},
	},
	"pushdown_comparison_NE": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []int32{2, 3, 4},
					"col_01_int":  []*int32{ptr.Int32(20), ptr.Int32(30), nil},
					"col_02_text": []*string{ptr.T("b"), ptr.T("c"), nil},
				},
			},
		},
	},
	"pushdown_comparison_NULL": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []int32{4},
					"col_01_int":  []*int32{nil},
					"col_02_text": []*string{nil},
				},
			},
		},
	},
	"pushdown_comparison_NOT_NULL": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []int32{1, 2, 3},
					"col_01_int":  []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30)},
					"col_02_text": []*string{ptr.T("a"), ptr.T("b"), ptr.T("c")},
				},
			},
		},
	},
	"pushdown_conjunction": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []int32{2, 3},
					"col_01_int":  []*int32{ptr.Int32(20), ptr.Int32(30)},
					"col_02_text": []*string{ptr.T("b"), ptr.T("c")},
				},
			},
		},
	},
	"pushdown_disjunction": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []int32{1, 2, 3},
					"col_01_int":  []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30)},
					"col_02_text": []*string{ptr.T("a"), ptr.T("b"), ptr.T("c")},
				},
			},
		},
	},
	"pushdown_negation": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []int32{4},
					"col_01_int":  []*int32{nil},
					"col_02_text": []*string{nil},
				},
			},
		},
	},
	"pushdown_strings_utf8": {
		Name:                  "pushdown_strings",
		Schema:                pushdownStringsSchemaYdb(),
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":            []int32{1},
					"col_01_int":    []*int32{ptr.Int32(10)},
					"col_02_utf8":   []*string{ptr.String("a")},
					"col_03_string": []*[]byte{ptr.T([]byte("a"))},
				},
			},
		},
	},
	"pushdown_strings_string": {
		Name:                  "pushdown_strings",
		Schema:                pushdownStringsSchemaYdb(),
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":            []int32{2},
					"col_01_int":    []*int32{ptr.Int32(20)},
					"col_02_utf8":   []*string{ptr.String("b")},
					"col_03_string": []*[]byte{ptr.T([]byte("b"))},
				},
			},
		},
	},
	"large": {
		Name:                  "large",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id": common.MakePrimitiveType(Ydb.Type_INT32),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id": makeRange[int32](0, 1005),
				},
			},
		},
	},
	"parent/child": {
		Name:                  "parent/child",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":  common.MakePrimitiveType(Ydb.Type_INT32),
				"col": common.MakePrimitiveType(Ydb.Type_UTF8),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":  []int32{1, 2, 3, 4, 5},
					"col": []string{"a", "b", "c", "d", "e"},
				},
			},
		},
	},
	// YQ-3949
	"json_document": {
		Name:                  "json_document",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":   common.MakePrimitiveType(Ydb.Type_INT32),
				"data": common.MakePrimitiveType(Ydb.Type_JSON),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id": []int32{1, 2},
					"data": []string{
						"{\"key1\":\"value1\"}",
						"{\"key2\":\"value2\"}",
					},
				},
			},
		},
	},
	// YQ-4255
	"pushdown_starts_with": {
		Name:                  "pushdown_like",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownLikeSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":            []int32{1},
					"col_01_string": []*[]byte{ptr.Bytes([]byte("abc"))},
					"col_02_utf8":   []*string{ptr.String("абв")},
				},
			},
		},
	},
	"pushdown_ends_with": {
		Name:                  "pushdown_like",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownLikeSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":            []int32{2},
					"col_01_string": []*[]byte{ptr.Bytes([]byte("def"))},
					"col_02_utf8":   []*string{ptr.String("где")},
				},
			},
		},
	},
	"pushdown_contains": {
		Name:                  "pushdown_like",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownLikeSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":            []int32{3},
					"col_01_string": []*[]byte{ptr.Bytes([]byte("ghi"))},
					"col_02_utf8":   []*string{ptr.String("ёжз")},
				},
			},
		},
	},
}

func pushdownSchemaYdb() *test_utils.TableSchema {
	return &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			"id":          common.MakePrimitiveType(Ydb.Type_INT32),
			"col_01_int":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			"col_02_text": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
		},
	}
}

func pushdownStringsSchemaYdb() *test_utils.TableSchema {
	return &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			"id":            common.MakePrimitiveType(Ydb.Type_INT32),
			"col_01_int":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			"col_02_utf8":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			"col_03_string": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
		},
	}
}

func pushdownLikeSchemaYdb() *test_utils.TableSchema {
	return &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			"id":            common.MakePrimitiveType(Ydb.Type_INT32),
			"col_01_string": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
			"col_02_utf8":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
		},
	}
}

func makeRange[T constraints.Integer](minValue, maxValue T) []T {
	result := make([]T, maxValue-minValue+1)
	for i := range result {
		result[i] = minValue + T(i)
	}

	return result
}

func newInt32IDArrayBuilder(pool memory.Allocator) func() *array.Int32Builder {
	return func() *array.Int32Builder {
		return array.NewInt32Builder(pool)
	}
}
