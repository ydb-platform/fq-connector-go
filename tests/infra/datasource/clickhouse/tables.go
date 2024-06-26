package clickhouse

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

// timezone: function get relative path, assumes starts from project root
var timezone = mustGetClickHouseDockerTimezone("./tests/infra/datasource/docker-compose.yaml")

// key - test case name
// value - table description
var tables = map[string]*test_utils.Table{
	"simple": {
		Name: "simple",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":   common.MakePrimitiveType(Ydb.Type_INT32),
				"col1": common.MakePrimitiveType(Ydb.Type_STRING),
				"col2": common.MakePrimitiveType(Ydb.Type_INT32),
			},
		},

		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id": []int32{1, 2, 3, 4, 5},
					"col1": [][]byte{
						[]byte("ch_a"),
						[]byte("ch_b"),
						[]byte("ch_c"),
						[]byte("ch_d"),
						[]byte("ch_e"),
					},
					"col2": []int32{10, 20, 30, 40, 50},
				},
			},
		},
	},

	"primitives": {
		Name: "primitives",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":                common.MakePrimitiveType(Ydb.Type_INT32),
				"col_01_boolean":    common.MakePrimitiveType(Ydb.Type_BOOL),
				"col_02_int8":       common.MakePrimitiveType(Ydb.Type_INT8),
				"col_03_uint8":      common.MakePrimitiveType(Ydb.Type_UINT8),
				"col_04_int16":      common.MakePrimitiveType(Ydb.Type_INT16),
				"col_05_uint16":     common.MakePrimitiveType(Ydb.Type_UINT16),
				"col_06_int32":      common.MakePrimitiveType(Ydb.Type_INT32),
				"col_07_uint32":     common.MakePrimitiveType(Ydb.Type_UINT32),
				"col_08_int64":      common.MakePrimitiveType(Ydb.Type_INT64),
				"col_09_uint64":     common.MakePrimitiveType(Ydb.Type_UINT64),
				"col_10_float32":    common.MakePrimitiveType(Ydb.Type_FLOAT),
				"col_11_float64":    common.MakePrimitiveType(Ydb.Type_DOUBLE),
				"col_12_string":     common.MakePrimitiveType(Ydb.Type_STRING),
				"col_13_string":     common.MakePrimitiveType(Ydb.Type_STRING),
				"col_14_date":       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				"col_15_date32":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				"col_16_datetime":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
				"col_17_datetime64": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
			},
		},

		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":             []int32{1, 2},
					"col_01_boolean": []uint8{0, 1}, // []bool{false, true}
					"col_02_int8":    []int8{2, -2},
					"col_03_uint8":   []uint8{3, 3},
					"col_04_int16":   []int16{4, -4},
					"col_05_uint16":  []uint16{5, 5},
					"col_06_int32":   []int32{6, -6},
					"col_07_uint32":  []uint32{7, 7},
					"col_08_int64":   []int64{8, -8},
					"col_09_uint64":  []uint64{9, 9},
					"col_10_float32": []float32{10.10, -10.10},
					"col_11_float64": []float64{11.11, -11.11},
					"col_12_string":  [][]byte{[]byte("az"), []byte("буки")},
					"col_13_string": [][]byte{
						append([]byte("az"), make([]byte, 11)...),
						append([]byte("буки"), make([]byte, 5)...),
					},
					"col_14_date": []*uint16{
						ptr.Uint16(
							common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 3, 0, 0, 0, timezone))),
						ptr.Uint16(
							common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 3, 0, 0, 0, timezone))),
					},
					"col_15_date32": []*uint16{
						ptr.Uint16(common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(1988, 11, 20, 3, 0, 0, 0, timezone))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](
							common.TimeToYDBDate, time.Date(2023, 03, 21, 3, 0, 0, 0, timezone))),
					},
					"col_16_datetime": []*uint32{
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, timezone))),
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(2023, 03, 21, 11, 21, 31, 0, timezone))),
					},
					"col_17_datetime64": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, timezone))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 456000000, timezone))),
					},
				},
			},
		},
	},

	"optionals": {
		Name: "optionals",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":                common.MakePrimitiveType(Ydb.Type_INT32),
				"col_01_boolean":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"col_02_int8":       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT8)),
				"col_03_uint8":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT8)),
				"col_04_int16":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				"col_05_uint16":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT16)),
				"col_06_int32":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_07_uint32":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT32)),
				"col_08_int64":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"col_09_uint64":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT64)),
				"col_10_float32":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				"col_11_float64":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"col_12_string":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_13_string":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_14_date":       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				"col_15_date32":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				"col_16_datetime":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
				"col_17_datetime64": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
			},
		},

		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":             []int32{1, 2, 3},
					"col_01_boolean": []*uint8{ptr.Uint8(0), ptr.Uint8(1), nil}, // []bool{false, true, nil}
					"col_02_int8":    []*int8{ptr.Int8(2), ptr.Int8(-2), nil},
					"col_03_uint8":   []*uint8{ptr.Uint8(3), ptr.Uint8(3), nil},
					"col_04_int16":   []*int16{ptr.Int16(4), ptr.Int16(-4), nil},
					"col_05_uint16":  []*uint16{ptr.Uint16(5), ptr.Uint16(5), nil},
					"col_06_int32":   []*int32{ptr.Int32(6), ptr.Int32(-6), nil},
					"col_07_uint32":  []*uint32{ptr.Uint32(7), ptr.Uint32(7), nil},
					"col_08_int64":   []*int64{ptr.Int64(8), ptr.Int64(-8), nil},
					"col_09_uint64":  []*uint64{ptr.Uint64(9), ptr.Uint64(9), nil},
					"col_10_float32": []*float32{ptr.Float32(10.10), ptr.Float32(-10.10), nil},
					"col_11_float64": []*float64{ptr.Float64(11.11), ptr.Float64(-11.11), nil},
					"col_12_string":  []*[]byte{ptr.T[[]byte]([]byte("az")), ptr.T[[]byte]([]byte("буки")), nil},
					"col_13_string": []*[]byte{
						ptr.T[[]byte](
							append([]byte("az"), make([]byte, 11)...),
						),
						ptr.T[[]byte](
							append([]byte("буки"), make([]byte, 5)...),
						),
						nil,
					},
					"col_14_date": []*uint16{
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 3, 0, 0, 0, timezone))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 3, 0, 0, 0, timezone))),
						nil,
					},
					"col_15_date32": []*uint16{
						ptr.Uint16(
							common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 3, 0, 0, 0, timezone))),
						ptr.Uint16(
							common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 3, 0, 0, 0, timezone))),
						nil,
					},
					"col_16_datetime": []*uint32{
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, timezone))),
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(2023, 03, 21, 11, 21, 31, 0, timezone))),
						nil,
					},
					"col_17_datetime64": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123000000, timezone))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 456000000, timezone))),
						nil,
					},
				},
			},
		},
	},

	"datetime_format_yql": {
		Name: "datetime",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":                common.MakePrimitiveType(Ydb.Type_INT32),
				"col_01_date":       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				"col_02_date32":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				"col_03_datetime":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
				"col_04_datetime64": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
			},
		},

		Records: []*test_utils.Record{
			{
				// In YQL mode, CH time values exceeding YQL date/datetime/timestamp type bounds
				// are handled in two ways:
				// 1. if value exceeds CH own type bounds, min or max time is returned
				// 2. if value exceeds only YQL type bounds, nil is returned
				Columns: map[string]any{
					"id": []int32{1, 2, 3},
					"col_01_date": []*uint16{
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1970, 01, 01, 3, 0, 0, 0, timezone))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 3, 0, 0, 0, timezone))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 3, 0, 0, 0, timezone))),
					},
					"col_02_date32": []*uint16{
						nil,
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(1988, 11, 20, 3, 0, 0, 0, timezone))),
						ptr.Uint16(common.MustTimeToYDBType[uint16](common.TimeToYDBDate, time.Date(2023, 03, 21, 3, 0, 0, 0, timezone))),
					},
					"col_03_datetime": []*uint32{
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1970, 01, 01, 3, 0, 0, 0, timezone))),
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, timezone))),
						ptr.Uint32(common.MustTimeToYDBType[uint32](
							common.TimeToYDBDatetime, time.Date(2023, 03, 21, 11, 21, 31, 0, timezone))),
					},
					"col_04_datetime64": []*uint64{
						nil,
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(1988, 11, 20, 12, 55, 28, 123456780, timezone))),
						ptr.Uint64(common.MustTimeToYDBType[uint64](
							common.TimeToYDBTimestamp, time.Date(2023, 03, 21, 11, 21, 31, 987654320, timezone))),
					},
				},
			},
		},
	},

	"datetime_format_string": {
		Name: "datetime",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":                common.MakePrimitiveType(Ydb.Type_INT32),
				"col_01_date":       common.MakePrimitiveType(Ydb.Type_UTF8),
				"col_02_date32":     common.MakePrimitiveType(Ydb.Type_UTF8),
				"col_03_datetime":   common.MakePrimitiveType(Ydb.Type_UTF8),
				"col_04_datetime64": common.MakePrimitiveType(Ydb.Type_UTF8),
			},
		},

		Records: []*test_utils.Record{
			{
				// In string mode, CH time values exceeding YQL date/datetime/timestamp type bounds
				// are saturated to the epoch start 1970.01.01 because Connector tries to imitate
				// ClickHouse behavior.
				Columns: map[string]any{
					"id":              []int32{1, 2, 3},
					"col_01_date":     []string{"1970-01-01", "1988-11-20", "2023-03-21"},
					"col_02_date32":   []string{"1950-05-27", "1988-11-20", "2023-03-21"},
					"col_03_datetime": []string{"1970-01-01T00:00:00Z", "1988-11-20T12:55:28Z", "2023-03-21T11:21:31Z"},
					"col_04_datetime64": []string{
						"1950-05-27T01:02:03.1111Z",
						"1988-11-20T12:55:28.12345678Z",
						"2023-03-21T11:21:31.98765432Z",
					},
				},
			},
		},
	},

	"pushdown_comparison_L": {
		Name:   "pushdown",
		Schema: pushdownSchema(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":            []int32{1},
					"col_01_int32":  []*int32{ptr.Int32(10)},
					"col_02_string": []*[]byte{ptr.T([]byte("a"))},
				},
			},
		},
	},
	"pushdown_comparison_LE": {
		Name:   "pushdown",
		Schema: pushdownSchema(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":            []int32{1, 2},
					"col_01_int32":  []*int32{ptr.Int32(10), ptr.Int32(20)},
					"col_02_string": []*[]byte{ptr.T([]byte("a")), ptr.T([]byte("b"))},
				},
			},
		},
	},
	"pushdown_comparison_EQ": {
		Name:   "pushdown",
		Schema: pushdownSchema(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":            []int32{2},
					"col_01_int32":  []*int32{ptr.Int32(20)},
					"col_02_string": []*[]byte{ptr.T([]byte("b"))},
				},
			},
		},
	},
	"pushdown_comparison_GE": {
		Name:   "pushdown",
		Schema: pushdownSchema(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":            []int32{2, 3},
					"col_01_int32":  []*int32{ptr.Int32(20), ptr.Int32(30)},
					"col_02_string": []*[]byte{ptr.T([]byte("b")), ptr.T([]byte("c"))},
				},
			},
		},
	},
	"pushdown_comparison_G": {
		Name:   "pushdown",
		Schema: pushdownSchema(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":            []int32{1, 2, 3},
					"col_01_int32":  []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30)},
					"col_02_string": []*[]byte{ptr.T([]byte("a")), ptr.T([]byte("b")), ptr.T([]byte("c"))},
				},
			},
		},
	},
	"pushdown_comparison_NE": {
		Name:   "pushdown",
		Schema: pushdownSchema(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":            []int32{2, 3, 4},
					"col_01_int32":  []*int32{ptr.Int32(20), ptr.Int32(30), nil},
					"col_02_string": []*[]byte{ptr.T([]byte("b")), ptr.T([]byte("c")), nil},
				},
			},
		},
	},
	"pushdown_comparison_NULL": {
		Name:   "pushdown",
		Schema: pushdownSchema(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":            []int32{4},
					"col_01_int32":  []*int32{nil},
					"col_02_string": []*[]byte{nil},
				},
			},
		},
	},
	"pushdown_comparison_NOT_NULL": {
		Name:   "pushdown",
		Schema: pushdownSchema(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":            []int32{1, 2, 3},
					"col_01_int32":  []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30)},
					"col_02_string": []*[]byte{ptr.T([]byte("a")), ptr.T([]byte("b")), ptr.T([]byte("c"))},
				},
			},
		},
	},
	"pushdown_conjunction": {
		Name:   "pushdown",
		Schema: pushdownSchema(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":            []int32{2, 3},
					"col_01_int32":  []*int32{ptr.Int32(20), ptr.Int32(30)},
					"col_02_string": []*[]byte{ptr.T([]byte("b")), ptr.T([]byte("c"))},
				},
			},
		},
	},
	"pushdown_disjunction": {
		Name:   "pushdown",
		Schema: pushdownSchema(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":            []int32{1, 2, 3},
					"col_01_int32":  []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30)},
					"col_02_string": []*[]byte{ptr.T([]byte("a")), ptr.T([]byte("b")), ptr.T([]byte("c"))},
				},
			},
		},
	},
	"pushdown_negation": {
		Name:   "pushdown",
		Schema: pushdownSchema(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":            []int32{4},
					"col_01_int32":  []*int32{nil},
					"col_02_string": []*[]byte{nil},
				},
			},
		},
	},
	"pushdown_between": {
		Name:   "pushdown",
		Schema: pushdownSchema(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":            []int32{2},
					"col_01_int32":  []*int32{ptr.Int32(20)},
					"col_02_string": []*[]byte{ptr.T([]byte("b"))},
				},
			},
		},
	},

	"array": {
		Name: "array",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":  common.MakePrimitiveType(Ydb.Type_INT32),
				"col": common.MakeListType(common.MakePrimitiveType(Ydb.Type_DATETIME)),
			},
		},
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id": []int32{1, 2, 3, 4},
					"col": [][]*uint32{
						{},
						{
							ptr.Uint32(common.MustTimeToYDBType[uint32](
								common.TimeToYDBDatetime, time.Date(1970, 01, 01, 3, 0, 0, 0, timezone))),
						},
						{
							ptr.Uint32(common.MustTimeToYDBType[uint32](
								common.TimeToYDBDatetime, time.Date(1970, 01, 01, 3, 0, 0, 0, timezone))),
							ptr.Uint32(common.MustTimeToYDBType[uint32](
								common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, timezone))),
						},
						{
							ptr.Uint32(common.MustTimeToYDBType[uint32](
								common.TimeToYDBDatetime, time.Date(1970, 01, 01, 3, 0, 0, 0, timezone))),
							ptr.Uint32(common.MustTimeToYDBType[uint32](
								common.TimeToYDBDatetime, time.Date(1988, 11, 20, 12, 55, 28, 0, timezone))),
							ptr.Uint32(common.MustTimeToYDBType[uint32](
								common.TimeToYDBDatetime, time.Date(2023, 03, 21, 11, 21, 31, 0, timezone))),
						},
					},
					// "col": [][]time.Time{{}, {time.Now()}, {time.Now(), time.Now()}, {time.Now(), time.Now(), time.Now()}},
				},
			},
		},
	},
}

func pushdownSchema() *test_utils.TableSchema {
	return &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			"id":            common.MakePrimitiveType(Ydb.Type_INT32),
			"col_01_int32":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			"col_02_string": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
		},
	}
}
