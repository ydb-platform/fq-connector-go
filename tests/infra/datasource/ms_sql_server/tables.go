package ms_sql_server

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var memPool memory.Allocator = memory.NewGoAllocator()

var tables = map[string]*test_utils.Table[int32, *array.Int32Builder]{
	"simple": {
		Name:                  "simple",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col1": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col2": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id": []*int32{
						ptr.Int32(1),
						ptr.Int32(2),
						ptr.Int32(3),
					},
					"col1": []*string{
						ptr.String("ms_sql_server_a"),
						ptr.String("ms_sql_server_b"),
						ptr.String("ms_sql_server_c"),
					},
					"col2": []*int32{
						ptr.Int32(10),
						ptr.Int32(20),
						ptr.Int32(30),
					},
				},
			},
		},
	},
	"primitives": {
		Name:                  "primitives",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_01_bit":       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"col_02_tinyint":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT8)),
				"col_03_smallint":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				"col_04_int":       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_05_bigint":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"col_06_float":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"col_07_real":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				"col_08_char":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_09_varchar":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_10_text":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_11_nchar":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_12_nvarchar":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_13_ntext":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_14_binary":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_15_varbinary": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_16_image":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":              []*int32{ptr.Int32(0), ptr.Int32(1), ptr.Int32(2)},
					"col_01_bit":      []*uint8{ptr.Uint8(1), nil, ptr.Uint8(0)},
					"col_02_tinyint":  []*int8{ptr.Int8(2), nil, ptr.Int8(2)},
					"col_03_smallint": []*int16{ptr.Int16(3), nil, ptr.Int16(-3)},
					"col_04_int":      []*int32{ptr.Int32(4), nil, ptr.Int32(-4)},
					"col_05_bigint":   []*int64{ptr.Int64(5), nil, ptr.Int64(-5)},
					"col_06_float":    []*float64{ptr.Float64(6.6), nil, ptr.Float64(-6.6)},
					"col_07_real":     []*float32{ptr.Float32(7.7), nil, ptr.Float32(-7.7)},
					"col_08_char":     []*string{ptr.String("az      "), nil, ptr.String("????    ")}, // '????' hide unicode symbols
					"col_09_varchar":  []*string{ptr.String("az"), nil, ptr.String("????")},           // '????' hide unicode symbols
					"col_10_text":     []*string{ptr.String("az"), nil, ptr.String("????")},           // '????' hide unicode symbols
					"col_11_nchar":    []*string{ptr.String("az      "), nil, ptr.String("буки    ")},
					"col_12_nvarchar": []*string{ptr.String("az"), nil, ptr.String("буки")},
					"col_13_ntext":    []*string{ptr.String("az"), nil, ptr.String("буки")},
					"col_14_binary": []*[]byte{
						ptr.T([]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF}),
						nil,
						ptr.T([]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF}),
					},
					"col_15_varbinary": []*[]byte{
						ptr.T([]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF}),
						nil,
						ptr.T([]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF}),
					},
					"col_16_image": []*[]byte{
						ptr.T([]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF}),
						nil,
						ptr.T([]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF}),
					},
				},
			},
		},
	},
	/*
		"datetime_format_yql": {
			Name: "datetimes",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: &test_utils.TableSchema{
				Columns: map[string]*Ydb.Type{
					"id":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
					"col_01_date":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
					"col_02_datetime":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
					"col_03_timestamp": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				},
			},
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					// In YQL mode, PG datetime values exceeding YQL date/datetime/timestamp type bounds
					// are returned as NULL
					Columns: map[string]any{
						"id": []*int32{
							ptr.Int32(1),
							ptr.Int32(2),
							ptr.Int32(3),
						},
						"col_01_date": []*uint16{
							nil,
							ptr.Uint16(common.MustTimeToYDBType[uint16](
								common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
							ptr.Uint16(common.MustTimeToYDBType[uint16](
								common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
						},
						"col_02_datetime": []*uint64{
							nil,
							ptr.Uint64(common.MustTimeToYDBType[uint64](
								common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC))),
							ptr.Uint64(common.MustTimeToYDBType[uint64](
								common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 0, time.UTC))),
						},
						"col_03_timestamp": []*uint64{
							nil,
							ptr.Uint64(common.MustTimeToYDBType[uint64](
								common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC))),
							ptr.Uint64(common.MustTimeToYDBType[uint64](
								common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 0, time.UTC))),
						},
					},
				},
			},
		},
		"datetime_format_string": {
			Name: "datetimes",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: &test_utils.TableSchema{
				Columns: map[string]*Ydb.Type{
					"id":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
					"col_01_date":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
					"col_02_datetime":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
					"col_03_timestamp": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
			},
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					// In string mode, PG time values exceeding YQL date/datetime/timestamp type bounds
					// are returned without saturating them to the epoch start
					Columns: map[string]any{
						"id": []*int32{
							ptr.Int32(1),
							ptr.Int32(2),
							ptr.Int32(3),
						},
						"col_01_date": []*string{
							ptr.String("1950-05-27"),
							ptr.String("1988-11-20"),
							ptr.String("2023-03-21"),
						},
						"col_02_datetime": []*string{
							ptr.String("1950-05-27T01:02:03.111111Z"),
							ptr.String("1988-11-20T12:55:28.123Z"),
							ptr.String("2023-03-21T11:21:31Z"),
						},
						"col_03_timestamp": []*string{
							nil,
							ptr.String("1988-11-20T12:55:28.123Z"),
							ptr.String("2023-03-21T11:21:31Z"),
						},
					},
				},
			},
		},
		"pushdown_comparison_L": {
			Name:   "pushdown",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: pushdownSchemaYdb(),
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					Columns: map[string]any{
						"id":             []*int32{ptr.Int32(1)},
						"int_column":     []*int32{ptr.Int32(10)},
						"varchar_column": []*string{ptr.T("a")},
					},
				},
			},
		},
		"pushdown_comparison_LE": {
			Name:   "pushdown",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: pushdownSchemaYdb(),
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					Columns: map[string]any{
						"id":             []*int32{ptr.Int32(1), ptr.Int32(2)},
						"int_column":     []*int32{ptr.Int32(10), ptr.Int32(20)},
						"varchar_column": []*string{ptr.T("a"), ptr.T("b")},
					},
				},
			},
		},
		"pushdown_comparison_EQ": {
			Name:   "pushdown",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: pushdownSchemaYdb(),
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					Columns: map[string]any{
						"id":             []*int32{ptr.Int32(2)},
						"int_column":     []*int32{ptr.Int32(20)},
						"varchar_column": []*string{ptr.T("b")},
					},
				},
			},
		},
		"pushdown_comparison_GE": {
			Name:   "pushdown",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: pushdownSchemaYdb(),
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					Columns: map[string]any{
						"id":             []*int32{ptr.Int32(2), ptr.Int32(3)},
						"int_column":     []*int32{ptr.Int32(20), ptr.Int32(30)},
						"varchar_column": []*string{ptr.T("b"), ptr.T("c")},
					},
				},
			},
		},
		"pushdown_comparison_G": {
			Name:   "pushdown",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: pushdownSchemaYdb(),
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					Columns: map[string]any{
						"id":             []*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3)},
						"int_column":     []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30)},
						"varchar_column": []*string{ptr.T("a"), ptr.T("b"), ptr.T("c")},
					},
				},
			},
		},
		"pushdown_comparison_NE": {
			Name:   "pushdown",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: pushdownSchemaYdb(),
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					Columns: map[string]any{
						"id":             []*int32{ptr.Int32(2), ptr.Int32(3), ptr.Int32(4)},
						"int_column":     []*int32{ptr.Int32(20), ptr.Int32(30), nil},
						"varchar_column": []*string{ptr.T("b"), ptr.T("c"), nil},
					},
				},
			},
		},
		"pushdown_comparison_NULL": {
			Name:   "pushdown",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: pushdownSchemaYdb(),
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					Columns: map[string]any{
						"id":             []*int32{ptr.Int32(4)},
						"int_column":     []*int32{nil},
						"varchar_column": []*string{nil},
					},
				},
			},
		},
		"pushdown_comparison_NOT_NULL": {
			Name:   "pushdown",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: pushdownSchemaYdb(),
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					Columns: map[string]any{
						"id":             []*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3)},
						"int_column":     []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30)},
						"varchar_column": []*string{ptr.T("a"), ptr.T("b"), ptr.T("c")},
					},
				},
			},
		},
		"pushdown_conjunction": {
			Name:   "pushdown",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: pushdownSchemaYdb(),
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					Columns: map[string]any{
						"id":             []*int32{ptr.Int32(2), ptr.Int32(3)},
						"int_column":     []*int32{ptr.Int32(20), ptr.Int32(30)},
						"varchar_column": []*string{ptr.T("b"), ptr.T("c")},
					},
				},
			},
		},
		"pushdown_disjunction": {
			Name:   "pushdown",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: pushdownSchemaYdb(),
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					Columns: map[string]any{
						"id":             []*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3)},
						"int_column":     []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30)},
						"varchar_column": []*string{ptr.T("a"), ptr.T("b"), ptr.T("c")},
					},
				},
			},
		},
		"pushdown_negation": {
			Name:   "pushdown",
			IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
			Schema: pushdownSchemaYdb(),
			Records: []*test_utils.Record[int32, *array.Int32Builder]{
				{
					Columns: map[string]any{
						"id":             []*int32{ptr.Int32(4)},
						"int_column":     []*int32{nil},
						"varchar_column": []*string{nil},
					},
				},
			},
		},
	*/
}

// func pushdownSchemaYdb() *test_utils.TableSchema {
// 	return &test_utils.TableSchema{
// 		Columns: map[string]*Ydb.Type{
// 			"id":             common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
// 			"int_column":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
// 			"varchar_column": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
// 		},
// 	}
// }

func newInt32IDArrayBuilder(pool memory.Allocator) func() *array.Int32Builder {
	return func() *array.Int32Builder {
		return array.NewInt32Builder(pool)
	}
}
