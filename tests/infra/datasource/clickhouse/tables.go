package clickhouse

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
)

// key - test case name
// value - table description
var tables = map[string]*datasource.Table{
	"simple": {
		Name: "simple",
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col1",
					Type: common.MakePrimitiveType(Ydb.Type_STRING),
				},
				{
					Name: "col2",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{1, 2, 3, 4, 5},
					[][]byte{
						[]byte("ch_a"),
						[]byte("ch_b"),
						[]byte("ch_c"),
						[]byte("ch_d"),
						[]byte("ch_e"),
					},
					[]int32{10, 20, 30, 40, 50},
				},
			},
		},
	},
	"primitives": {
		Name: "primitives",
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col_01_boolean",
					Type: common.MakePrimitiveType(Ydb.Type_BOOL),
				},
				{
					Name: "col_02_int8",
					Type: common.MakePrimitiveType(Ydb.Type_INT8),
				},
				{
					Name: "col_03_uint8",
					Type: common.MakePrimitiveType(Ydb.Type_UINT8),
				},
				{
					Name: "col_04_int16",
					Type: common.MakePrimitiveType(Ydb.Type_INT16),
				},
				{
					Name: "col_05_uint16",
					Type: common.MakePrimitiveType(Ydb.Type_UINT16),
				},
				{
					Name: "col_06_int32",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col_07_uint32",
					Type: common.MakePrimitiveType(Ydb.Type_UINT32),
				},
				{
					Name: "col_08_int64",
					Type: common.MakePrimitiveType(Ydb.Type_INT64),
				},
				{
					Name: "col_09_uint64",
					Type: common.MakePrimitiveType(Ydb.Type_UINT64),
				},
				{
					Name: "col_10_float32",
					Type: common.MakePrimitiveType(Ydb.Type_FLOAT),
				},
				{
					Name: "col_11_float64",
					Type: common.MakePrimitiveType(Ydb.Type_DOUBLE),
				},
				{
					Name: "col_12_string",
					Type: common.MakePrimitiveType(Ydb.Type_STRING),
				},
				{
					Name: "col_13_string",
					Type: common.MakePrimitiveType(Ydb.Type_STRING),
				},
				{
					Name: "col_14_date",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
				{
					Name: "col_15_date32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
				{
					Name: "col_16_datetime",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
				},
				{
					Name: "col_17_datetime64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{1, 2},
					[]uint8{0, 1}, // []bool{false, true}
					[]int8{2, -2},
					[]uint8{3, 3},
					[]int16{4, -4},
					[]uint16{5, 5},
					[]int32{6, -6},
					[]uint32{7, 7},
					[]int64{8, -8},
					[]uint64{9, 9},
					[]float32{10.10, -10.10},
					[]float64{11.11, -11.11},
					[][]byte{[]byte("az"), []byte("буки")},
					[][]byte{
						append([]byte("az"), make([]byte, 11)...),
						append([]byte("буки"), make([]byte, 5)...),
					},
					[]*uint16{
						ptr.Uint16(
							common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(
							common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
					},
					[]*uint16{
						ptr.Uint16(common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
					},
					[]*uint32{
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, time.UTC))),
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(2023, 03, 21, 11, 21, 31, 0, time.UTC))),
					},
					[]*uint64{
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 456000000, time.UTC))),
					},
				},
			},
		},
	},
	"optionals": {
		Name: "optionals",
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col_01_boolean",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				},
				{
					Name: "col_02_int8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT8)),
				},
				{
					Name: "col_03_uint8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT8)),
				},
				{
					Name: "col_04_int16",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				},
				{
					Name: "col_05_uint16",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT16)),
				},
				{
					Name: "col_06_int32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_07_uint32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT32)),
				},
				{
					Name: "col_08_int64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				},
				{
					Name: "col_09_uint64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT64)),
				},
				{
					Name: "col_10_float32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				},
				{
					Name: "col_11_float64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				},
				{
					Name: "col_12_string",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				},
				{
					Name: "col_13_string",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				},
				{
					Name: "col_14_date",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
				{
					Name: "col_15_date32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
				{
					Name: "col_16_datetime",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
				},
				{
					Name: "col_17_datetime64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{1, 2, 3},
					[]*uint8{ptr.Uint8(0), ptr.Uint8(1), nil}, // []bool{false, true, nil}
					[]*int8{ptr.Int8(2), ptr.Int8(-2), nil},
					[]*uint8{ptr.Uint8(3), ptr.Uint8(3), nil},
					[]*int16{ptr.Int16(4), ptr.Int16(-4), nil},
					[]*uint16{ptr.Uint16(5), ptr.Uint16(5), nil},
					[]*int32{ptr.Int32(6), ptr.Int32(-6), nil},
					[]*uint32{ptr.Uint32(7), ptr.Uint32(7), nil},
					[]*int64{ptr.Int64(8), ptr.Int64(-8), nil},
					[]*uint64{ptr.Uint64(9), ptr.Uint64(9), nil},
					[]*float32{ptr.Float32(10.10), ptr.Float32(-10.10), nil},
					[]*float64{ptr.Float64(11.11), ptr.Float64(-11.11), nil},
					[]*[]byte{ptr.T[[]byte]([]byte("az")), ptr.T[[]byte]([]byte("буки")), nil},
					[]*[]byte{
						ptr.T[[]byte](
							append([]byte("az"), make([]byte, 11)...),
						),
						ptr.T[[]byte](
							append([]byte("буки"), make([]byte, 5)...),
						),
						nil,
					},
					[]*uint16{
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
						nil,
					},
					[]*uint16{
						ptr.Uint16(
							common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(
							common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
						nil,
					},
					[]*uint32{
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, time.UTC))),
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(2023, 03, 21, 11, 21, 31, 0, time.UTC))),
						nil,
					},
					[]*uint64{
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 456000000, time.UTC))),
						nil,
					},
				},
			},
		},
	},
	"datetime_format_yql": {
		Name: "datetime",
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col_01_date",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
				{
					Name: "col_02_date32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
				{
					Name: "col_03_datetime",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
				},
				{
					Name: "col_04_datetime64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				// In YQL mode, CH time values exceeding YQL date/datetime/timestamp type bounds
				// are handled in two ways:
				// 1. if value exceeds CH own type bounds, min or max time is returned
				// 2. if value exceeds only YQL type bounds, nil is returned
				Columns: []any{
					[]int32{1, 2, 3},
					[]*uint16{
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1970, 01, 01, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
					},
					[]*uint16{
						nil,
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 0, 0, 0, 0, time.UTC))),
					},
					[]*uint32{
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1970, 01, 01, 0, 0, 0, 0, time.UTC))),
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, time.UTC))),
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(2023, 03, 21, 11, 21, 31, 0, time.UTC))),
					},
					[]*uint64{
						nil,
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123456780, time.UTC))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 987654320, time.UTC))),
					},
				},
			},
		},
	},
	"datetime_format_string": {
		Name: "datetime",
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col_01_date",
					Type: (common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
				{
					Name: "col_02_date32",
					Type: (common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
				{
					Name: "col_03_datetime",
					Type: (common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
				{
					Name: "col_04_datetime64",
					Type: (common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				// In string mode, CH time values exceeding YQL date/datetime/timestamp type bounds
				// are saturated to the epoch start 1970.01.01 because Connector tries to imitate
				// ClickHouse behavior.
				Columns: []any{
					[]int32{1, 2, 3},
					[]string{"1970-01-01", "1988-11-20", "2023-03-21"},
					[]string{"1950-05-27", "1988-11-20", "2023-03-21"},
					[]string{"1970-01-01T00:00:00Z", "1988-11-20T12:55:28Z", "2023-03-21T11:21:31Z"},
					[]string{"1950-05-27T01:02:03.1111Z", "1988-11-20T12:55:28.12345678Z", "2023-03-21T11:21:31.98765432Z"},
				},
			},
		},
	},
	"pushdown_comparison_L": {
		Name:      "pushdown",
		SchemaYdb: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{1},
					[]*int32{ptr.Int32(10)},
					[]*[]byte{ptr.T([]byte("a"))},
				},
			},
		},
	},
	"pushdown_comparison_LE": {
		Name:      "pushdown",
		SchemaYdb: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{1, 2},
					[]*int32{ptr.Int32(10), ptr.Int32(20)},
					[]*[]byte{ptr.T([]byte("a")), ptr.T([]byte("b"))},
				},
			},
		},
	},
	"pushdown_comparison_EQ": {
		Name:      "pushdown",
		SchemaYdb: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{2},
					[]*int32{ptr.Int32(20)},
					[]*[]byte{ptr.T([]byte("b"))},
				},
			},
		},
	},
	"pushdown_comparison_GE": {
		Name:      "pushdown",
		SchemaYdb: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{2, 3},
					[]*int32{ptr.Int32(20), ptr.Int32(30)},
					[]*[]byte{ptr.T([]byte("b")), ptr.T([]byte("c"))},
				},
			},
		},
	},
	"pushdown_comparison_G": {
		Name:      "pushdown",
		SchemaYdb: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{1, 2, 3},
					[]*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30)},
					[]*[]byte{ptr.T([]byte("a")), ptr.T([]byte("b")), ptr.T([]byte("c"))},
				},
			},
		},
	},
	"pushdown_comparison_NE": {
		Name:      "pushdown",
		SchemaYdb: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{2, 3, 4},
					[]*int32{ptr.Int32(20), ptr.Int32(30), nil},
					[]*[]byte{ptr.T([]byte("b")), ptr.T([]byte("c")), nil},
				},
			},
		},
	},
	"pushdown_comparison_NULL": {
		Name:      "pushdown",
		SchemaYdb: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{4},
					[]*int32{nil},
					[]*[]byte{nil},
				},
			},
		},
	},
	"pushdown_comparison_NOT_NULL": {
		Name:      "pushdown",
		SchemaYdb: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{1, 2, 3},
					[]*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30)},
					[]*[]byte{ptr.T([]byte("a")), ptr.T([]byte("b")), ptr.T([]byte("c"))},
				},
			},
		},
	},
	"pushdown_conjunction": {
		Name:      "pushdown",
		SchemaYdb: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{2, 3},
					[]*int32{ptr.Int32(20), ptr.Int32(30)},
					[]*[]byte{ptr.T([]byte("b")), ptr.T([]byte("c"))},
				},
			},
		},
	},
	"pushdown_disjunction": {
		Name:      "pushdown",
		SchemaYdb: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{1, 2, 3},
					[]*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30)},
					[]*[]byte{ptr.T([]byte("a")), ptr.T([]byte("b")), ptr.T([]byte("c"))},
				},
			},
		},
	},
	"pushdown_negation": {
		Name:      "pushdown",
		SchemaYdb: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{4},
					[]*int32{nil},
					[]*[]byte{nil},
				},
			},
		},
	},
	"pushdown_between": {
		Name:      "pushdown",
		SchemaYdb: pushdownSchemaYdb(),
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int32{2},
					[]*int32{ptr.Int32(20)},
					[]*[]byte{ptr.T([]byte("b"))},
				},
			},
		},
	},
}

func pushdownSchemaYdb() *api_service_protos.TSchema {
	return &api_service_protos.TSchema{
		Columns: []*Ydb.Column{
			{Name: "id", Type: common.MakePrimitiveType(Ydb.Type_INT32)},
			{Name: "col_01_int32", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32))},
			{Name: "col_02_string", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING))},
		},
	}
}
