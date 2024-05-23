package postgresql

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
)

var tables = map[string]*datasource.Table{
	"simple": {
		Name: "simple",
		Schema: &datasource.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col1": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col2": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			},
		},
		Records: []*datasource.Record{
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
		Name: "primitives",
		Schema: &datasource.TableSchema{
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
			},
		},
		Records: []*datasource.Record{
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
				},
			},
		},
	},
	"datetime_format_yql": {
		Name: "datetime",
		Schema: &datasource.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_01_timestamp": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"col_02_date":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
			},
		},
		Records: []*datasource.Record{
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
	"datetime_format_string": {
		Name: "datetime",
		Schema: &datasource.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_01_timestamp": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_02_date":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*datasource.Record{
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
		Name:   "pushdown",
		Schema: pushdownSchemaYdb(),
		Records: []*datasource.Record{
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
		Name:   "pushdown",
		Schema: pushdownSchemaYdb(),
		Records: []*datasource.Record{
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
		Name:   "pushdown",
		Schema: pushdownSchemaYdb(),
		Records: []*datasource.Record{
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
		Name:   "pushdown",
		Schema: pushdownSchemaYdb(),
		Records: []*datasource.Record{
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
		Name:   "pushdown",
		Schema: pushdownSchemaYdb(),
		Records: []*datasource.Record{
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
		Name:   "pushdown",
		Schema: pushdownSchemaYdb(),
		Records: []*datasource.Record{
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
		Name:   "pushdown",
		Schema: pushdownSchemaYdb(),
		Records: []*datasource.Record{
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
		Name:   "pushdown",
		Schema: pushdownSchemaYdb(),
		Records: []*datasource.Record{
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
		Name:   "pushdown",
		Schema: pushdownSchemaYdb(),
		Records: []*datasource.Record{
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
		Name:   "pushdown",
		Schema: pushdownSchemaYdb(),
		Records: []*datasource.Record{
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
		Name:   "pushdown",
		Schema: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: map[string]any{
					"id":          []*int32{ptr.Int32(4)},
					"col_01_int":  []*int32{nil},
					"col_02_text": []*string{nil},
				},
			},
		},
	},
}

func pushdownSchemaYdb() *datasource.TableSchema {
	return &datasource.TableSchema{
		Columns: map[string]*Ydb.Type{
			"id":          common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			"col_01_int":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			"col_02_text": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
		},
	}
}
