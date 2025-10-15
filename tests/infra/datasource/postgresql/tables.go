package postgresql

import (
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var memPool memory.Allocator = memory.NewGoAllocator()

var tablesIDInt32 = map[string]*test_utils.Table[int32, *array.Int32Builder]{
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
					"id": []*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3), ptr.Int32(4), ptr.Int32(5)},
					"col1": []*string{
						ptr.String("pg_a"),
						ptr.String("pg_b"),
						ptr.String("pg_c"),
						ptr.String("pg_d"),
						ptr.String("pg_e"),
					},
					"col2": []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30), ptr.Int32(40), ptr.Int32(50)},
				},
			},
		},
	},
	"primitives": {
		Name:                  "primitives",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":                         common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_01_bool":                common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"col_02_smallint":            common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				"col_03_int2":                common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				"col_04_smallserial":         common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				"col_05_serial2":             common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				"col_06_integer":             common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_07_int":                 common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_08_int4":                common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_09_serial":              common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_10_serial4":             common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_11_bigint":              common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"col_12_int8":                common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"col_13_bigserial":           common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"col_14_serial8":             common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"col_15_real":                common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				"col_16_float4":              common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				"col_17_double_precision":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"col_18_float8":              common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"col_19_bytea":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_20_character_n":         common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_21_character_varying_n": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_22_text":                common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_23_timestamp":           common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"col_24_date":                common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				"col_25_json":                common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_JSON)),
				"col_26_uuid":                common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_27_numeric_int":         common.MakeOptionalType(common.MakeDecimalType(10, 0)),
				"col_28_numeric_rational":    common.MakeOptionalType(common.MakeDecimalType(4, 2)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3)},
					"col_01_bool": []*uint8{ptr.Uint8(0), ptr.Uint8(1), nil},
					"col_02_smallint": []*int16{
						ptr.Int16(2),
						ptr.Int16(-2),
						nil,
					},
					"col_03_int2": []*int16{
						ptr.Int16(3),
						ptr.Int16(-3),
						nil,
					},
					"col_04_smallserial": []*int16{
						ptr.Int16(1),
						ptr.Int16(2),
						ptr.Int16(3),
					},
					"col_05_serial2": []*int16{
						ptr.Int16(1),
						ptr.Int16(2),
						ptr.Int16(3),
					},
					"col_06_integer": []*int32{
						ptr.Int32(6),
						ptr.Int32(-6),
						nil,
					},
					"col_07_int": []*int32{
						ptr.Int32(7),
						ptr.Int32(-7),
						nil,
					},
					"col_08_int4": []*int32{
						ptr.Int32(8),
						ptr.Int32(-8),
						nil,
					},
					"col_09_serial": []*int32{
						ptr.Int32(1),
						ptr.Int32(2),
						ptr.Int32(3),
					},
					"col_10_serial4": []*int32{
						ptr.Int32(1),
						ptr.Int32(2),
						ptr.Int32(3),
					},
					"col_11_bigint": []*int64{
						ptr.Int64(11),
						ptr.Int64(-11),
						nil,
					},
					"col_12_int8": []*int64{
						ptr.Int64(12),
						ptr.Int64(-12),
						nil,
					},
					"col_13_bigserial": []*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					"col_14_serial8": []*int64{
						ptr.Int64(1),
						ptr.Int64(2),
						ptr.Int64(3),
					},
					"col_15_real": []*float32{
						ptr.Float32(15.15),
						ptr.Float32(-15.15),
						nil,
					},
					"col_16_float4": []*float32{
						ptr.Float32(16.16),
						ptr.Float32(-16.16),
						nil,
					},
					"col_17_double_precision": []*float64{
						ptr.Float64(17.17),
						ptr.Float64(-17.17),
						nil,
					},
					"col_18_float8": []*float64{
						ptr.Float64(18.18),
						ptr.Float64(-18.18),
						nil,
					},
					"col_19_bytea": []*[]byte{
						ptr.T[[]byte]([]byte("az")),
						ptr.T[[]byte]([]byte("буки")),
						nil,
					},
					"col_20_character_n": []*string{
						ptr.String("az                  "),
						ptr.String("буки                "),
						nil,
					},
					"col_21_character_varying_n": []*string{
						ptr.String("az"),
						ptr.String("буки"),
						nil,
					},
					"col_22_text": []*string{
						ptr.String("az"),
						ptr.String("буки"),
						nil,
					},
					"col_23_timestamp": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 456000000, time.UTC))),
						nil,
					},
					"col_24_date": []*uint16{
						ptr.Uint16(common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(1988, 11, 20, 12, 55, 28, 0, time.UTC))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(2023, 03, 21, 11, 21, 31, 0, time.UTC))),
						nil,
					},
					"col_25_json": []*string{
						ptr.String("{ \"friends\": " +
							"[{\"name\": \"James Holden\",\"age\": 35}," +
							"{\"name\": \"Naomi Nagata\",\"age\": 30}]}"),
						ptr.String("{ \"TODO\" : \"unicode\" }"),
						nil,
					},
					"col_26_uuid": []*[]byte{
						ptr.T([]byte(string("dce06500-b56b-412b-bc39-f9fafb602663"))),
						ptr.T([]byte(string("b18cafa2-9892-4515-843d-e8ee9bd9a858"))),
						nil,
					},
					"col_27_numeric_int": []*[]byte{
						ptr.T([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
						ptr.T([]byte{254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}),
						nil,
					},
					"col_28_numeric_rational": []*[]byte{
						ptr.T([]byte{87, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
						ptr.T([]byte{82, 247, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}),
						nil,
					},
				},
			},
		},
	},
	"datetime_format_yql": {
		Name:                  "datetime",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_01_timestamp": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"col_02_date":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
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
					"col_01_timestamp": []*uint64{
						nil,
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 456000000, time.UTC))),
					},
					"col_02_date": []*uint16{
						nil,
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
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
				"id":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_01_timestamp": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"col_02_date":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				// In YQL mode, PG datetime values exceeding YQL date/datetime/timestamp type bounds
				// are returned as NULL
				Columns: map[string]any{
					"id": []*int32{
						ptr.Int32(2),
					},
					"col_01_timestamp": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC))),
					},
					"col_02_date": []*uint16{
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
					},
				},
			},
		},
	},
	"datetime_format_string": {
		Name:                  "datetime",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_01_timestamp": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_02_date":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
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
					"col_01_timestamp": []*string{
						ptr.String("1950-05-27T01:02:03.111111Z"),
						ptr.String("1988-11-20T12:55:28.123Z"),
						ptr.String("2023-03-21T11:21:31.456Z"),
					},
					"col_02_date": []*string{
						ptr.String("1950-05-27"),
						ptr.String("1988-11-20"),
						ptr.String("2023-03-21"),
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
					"id":          []*int32{ptr.Int32(1)},
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
					"id":          []*int32{ptr.Int32(1), ptr.Int32(2)},
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
					"id":          []*int32{ptr.Int32(2)},
					"col_01_int":  []*int32{ptr.Int32(20)},
					"col_02_text": []*string{ptr.T("b")},
				},
			},
		},
	},
	"pushdown_comparison_GE": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []*int32{ptr.Int32(2), ptr.Int32(3)},
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
					"id":          []*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3)},
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
					"id":          []*int32{ptr.Int32(2), ptr.Int32(3), ptr.Int32(4)},
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
					"id":          []*int32{ptr.Int32(4)},
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
					"id":          []*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3)},
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
					"id":          []*int32{ptr.Int32(2), ptr.Int32(3)},
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
					"id":          []*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3)},
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
					"id":          []*int32{ptr.Int32(4)},
					"col_01_int":  []*int32{nil},
					"col_02_text": []*string{nil},
				},
			},
		},
	},
	"pushdown_unsupported_filtering_optional": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":          []*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3), ptr.Int32(4)},
					"col_01_int":  []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30), nil},
					"col_02_text": []*string{ptr.T("a"), ptr.T("b"), ptr.T("c"), nil},
				},
			},
		},
	},
	"pushdown_unsupported_filtering_mandatory": {
		Name:                  "pushdown",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownSchemaYdb(),
		Records:               []*test_utils.Record[int32, *array.Int32Builder]{},
	},
	"pushdown_decimal_int_EQ": {
		Name:                  "pushdown_decimal",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownDecimalSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id": []*int32{ptr.Int32(1)},
					"col_27_numeric_int": []*[]byte{
						ptr.T([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
					},
					"col_28_numeric_rational": []*[]byte{
						ptr.T([]byte{87, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
					},
				},
			},
		},
	},
	"pushdown_decimal_rational_EQ": {
		Name:                  "pushdown_decimal",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema:                pushdownDecimalSchemaYdb(),
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id": []*int32{ptr.Int32(2)},
					"col_27_numeric_int": []*[]byte{
						ptr.T([]byte{254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}),
					},
					"col_28_numeric_rational": []*[]byte{
						ptr.T([]byte{82, 247, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}),
					},
				},
			},
		},
	},
	"primary_key_int": {
		Name:                  "primary_key_int",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"text_col": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{
			{
				Columns: map[string]any{
					"id":       []*int32{ptr.Int32(1)},
					"text_col": []*string{ptr.String("a")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*int32{ptr.Int32(2)},
					"text_col": []*string{ptr.String("b")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*int32{ptr.Int32(3)},
					"text_col": []*string{ptr.String("c")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*int32{ptr.Int32(4)},
					"text_col": []*string{ptr.String("d")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*int32{ptr.Int32(5)},
					"text_col": []*string{ptr.String("e")},
				},
			},
		},
	},
}

var tablesIDInt64 = map[string]*test_utils.Table[int64, *array.Int64Builder]{
	"primary_key_bigint": {
		Name:                  "primary_key_bigint",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"text_col": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int64, *array.Int64Builder]{
			{
				Columns: map[string]any{
					"id":       []*int64{ptr.Int64(1)},
					"text_col": []*string{ptr.String("a")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*int64{ptr.Int64(2)},
					"text_col": []*string{ptr.String("b")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*int64{ptr.Int64(3)},
					"text_col": []*string{ptr.String("c")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*int64{ptr.Int64(4)},
					"text_col": []*string{ptr.String("d")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*int64{ptr.Int64(5)},
					"text_col": []*string{ptr.String("e")},
				},
			},
		},
	},
}

var tablesIDDecimal = map[string]*test_utils.Table[[]byte, *array.FixedSizeBinaryBuilder]{
	"primary_key_numeric_10_0": {
		Name:                  "primary_key_numeric_10_0",
		IDArrayBuilderFactory: newFixedSizeBinaryBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":       common.MakeOptionalType(common.MakeDecimalType(10, 0)),
				"text_col": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[[]byte, *array.FixedSizeBinaryBuilder]{
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},
					"text_col": []*string{ptr.String("a")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},
					"text_col": []*string{ptr.String("b")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},
					"text_col": []*string{ptr.String("c")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},
					"text_col": []*string{ptr.String("d")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},
					"text_col": []*string{ptr.String("e")},
				},
			},
		},
	},
	"primary_key_numeric_4_2": {
		Name:                  "primary_key_numeric_4_2",
		IDArrayBuilderFactory: newFixedSizeBinaryBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":       common.MakeOptionalType(common.MakeDecimalType(4, 2)),
				"text_col": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[[]byte, *array.FixedSizeBinaryBuilder]{
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})}, // 1.00
					"text_col": []*string{ptr.String("a")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{250, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})}, // 2.50
					"text_col": []*string{ptr.String("b")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{119, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})}, // 3.75
					"text_col": []*string{ptr.String("c")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{169, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})}, // 4.25
					"text_col": []*string{ptr.String("d")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{87, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})}, // 5.99
					"text_col": []*string{ptr.String("e")},
				},
			},
		},
	},
	"primary_key_numeric_unconstrained": {
		Name:                  "primary_key_numeric",
		IDArrayBuilderFactory: newFixedSizeBinaryBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":       common.MakeOptionalType(common.MakeDecimalType(35, 0)), // default type for unconstrained numerics
				"text_col": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[[]byte, *array.FixedSizeBinaryBuilder]{
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},
					"text_col": []*string{ptr.String("a")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},
					"text_col": []*string{ptr.String("b")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},
					"text_col": []*string{ptr.String("c")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},
					"text_col": []*string{ptr.String("d")},
				},
			},
			{
				Columns: map[string]any{
					"id":       []*[]byte{ptr.T([]byte{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},
					"text_col": []*string{ptr.String("e")},
				},
			},
		},
	},
}

// Schema for decimal pushdown tests
func pushdownDecimalSchemaYdb() *test_utils.TableSchema {
	return &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			"id":                      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			"col_27_numeric_int":      common.MakeOptionalType(common.MakeDecimalType(10, 0)),
			"col_28_numeric_rational": common.MakeOptionalType(common.MakeDecimalType(4, 2)),
		},
	}
}

func pushdownSchemaYdb() *test_utils.TableSchema {
	return &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			"id":          common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			"col_01_int":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			"col_02_text": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
		},
	}
}

func newInt32IDArrayBuilder(pool memory.Allocator) func() *array.Int32Builder {
	return func() *array.Int32Builder {
		return array.NewInt32Builder(pool)
	}
}

func newInt64IDArrayBuilder(pool memory.Allocator) func() *array.Int64Builder {
	return func() *array.Int64Builder {
		return array.NewInt64Builder(pool)
	}
}

func newFixedSizeBinaryBuilder(pool memory.Allocator) func() *array.FixedSizeBinaryBuilder {
	return func() *array.FixedSizeBinaryBuilder {
		return array.NewFixedSizeBinaryBuilder(pool, &arrow.FixedSizeBinaryType{ByteWidth: 16})
	}
}
