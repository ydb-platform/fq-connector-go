package mysql

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var tables = map[string]*test_utils.Table{
	"simple": {
		Name: "simple",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col1": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col2": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			},
		},
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id": []*int32{
						ptr.Int32(1),
						ptr.Int32(2),
						ptr.Int32(3),
					},
					"col1": []*string{
						ptr.String("mysql_a"),
						ptr.String("mysql_b"),
						ptr.String("mysql_c"),
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
		Name: "primitives",
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":                        common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_01_tinyint":            common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT8)),
				"col_02_tinyint_unsigned":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT8)),
				"col_03_smallint":           common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				"col_04_smallint_unsigned":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT16)),
				"col_05_mediumint":          common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_06_mediumint_unsigned": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT32)),
				"col_07_integer":            common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"col_08_integer_unsigned":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT32)),
				"col_09_bigint":             common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"col_10_bigint_unsigned":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT64)),
				"col_11_float":              common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				"col_12_double":             common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"col_13_date":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				"col_14_datetime":           common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"col_15_timestamp":          common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				"col_16_char":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_17_varchar":            common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_18_tinytext":           common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_19_text":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_20_mediumtext":         common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_21_longtext":           common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_22_binary":             common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_23_varbinary":          common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"col_24_tinyblob":           common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_25_blob":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_26_mediumblob":         common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_27_longblob":           common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"col_28_bool":               common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
			},
		},
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":                        []*int32{ptr.Int32(0), ptr.Int32(1), ptr.Int32(2)},
					"col_01_tinyint":            []*int8{ptr.Int8(1), nil, ptr.Int8(-10)},
					"col_02_tinyint_unsigned":   []*uint8{ptr.Uint8(2), nil, ptr.Uint8(20)},
					"col_03_smallint":           []*int16{ptr.Int16(3), nil, ptr.Int16(-30)},
					"col_04_smallint_unsigned":  []*uint16{ptr.Uint16(4), nil, ptr.Uint16(40)},
					"col_05_mediumint":          []*int32{ptr.Int32(5), nil, ptr.Int32(-50)},
					"col_06_mediumint_unsigned": []*uint32{ptr.Uint32(6), nil, ptr.Uint32(60)},
					"col_07_integer":            []*int32{ptr.Int32(7), nil, ptr.Int32(-70)},
					"col_08_integer_unsigned":   []*uint32{ptr.Uint32(8), nil, ptr.Uint32(80)},
					"col_09_bigint":             []*int64{ptr.Int64(9), nil, ptr.Int64(-90)},
					"col_10_bigint_unsigned":    []*uint64{ptr.Uint64(10), nil, ptr.Uint64(100)},
					"col_11_float":              []*float32{ptr.Float32(11.11), nil, ptr.Float32(-1111.1111)},
					"col_12_double":             []*float64{ptr.Float64(12.12), nil, ptr.Float64(-1212.1212)},
					"col_13_date": []*uint16{
						ptr.Uint16(common.MustTimeToYDBType(common.TimeToYDBDate, time.Date(1988, 11, 20, 0, 0, 0, 0, time.UTC))),
						nil,
						ptr.Uint16(common.MustTimeToYDBType(common.TimeToYDBDate, time.Date(2024, 07, 01, 0, 0, 0, 0, time.UTC))),
					},
					"col_14_datetime": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1988, 11, 20, 12, 34, 56, 777777000, time.UTC))),
						nil,
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(2024, 07, 01, 01, 02, 03, 444444000, time.UTC))),
					},
					"col_15_timestamp": []*uint64{
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(1988, 11, 20, 12, 34, 56, 777777000, time.UTC))),
						nil,
						ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(2024, 07, 01, 01, 02, 03, 444444000, time.UTC))),
					},
					"col_16_char":       []*string{ptr.String("az"), nil, ptr.String("буки")},
					"col_17_varchar":    []*string{ptr.String("az"), nil, ptr.String("буки")},
					"col_18_tinytext":   []*[]byte{ptr.T([]byte("az")), nil, ptr.T([]byte("буки"))},
					"col_19_text":       []*[]byte{ptr.T([]byte("az")), nil, ptr.T([]byte("буки"))},
					"col_20_mediumtext": []*[]byte{ptr.T([]byte("az")), nil, ptr.T([]byte("буки"))},
					"col_21_longtext":   []*[]byte{ptr.T([]byte("az")), nil, ptr.T([]byte("буки"))},
					"col_22_binary":     []*string{ptr.String("az\x00\x00\x00\x00\x00\x00"), nil, ptr.String("буки")},
					"col_23_varbinary":  []*string{ptr.String("az"), nil, ptr.String("буки")},
					"col_24_tinyblob":   []*[]byte{ptr.T([]byte("az")), nil, ptr.T([]byte("буки"))},
					"col_25_blob":       []*[]byte{ptr.T([]byte("az")), nil, ptr.T([]byte("буки"))},
					"col_26_mediumblob": []*[]byte{ptr.T([]byte("az")), nil, ptr.T([]byte("буки"))},
					"col_27_longblob":   []*[]byte{ptr.T([]byte("az")), nil, ptr.T([]byte("буки"))},
					"col_28_bool":       []*uint8{ptr.Uint8(1), nil, ptr.Uint8(0)},
				},
			},
		},
	},
	"pushdown_comparison_L": {
		Name:   "pushdown",
		Schema: pushdownSchemaYdb(),
		Records: []*test_utils.Record{
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
		Schema: pushdownSchemaYdb(),
		Records: []*test_utils.Record{
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
		Schema: pushdownSchemaYdb(),
		Records: []*test_utils.Record{
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
		Schema: pushdownSchemaYdb(),
		Records: []*test_utils.Record{
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
		Schema: pushdownSchemaYdb(),
		Records: []*test_utils.Record{
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
		Schema: pushdownSchemaYdb(),
		Records: []*test_utils.Record{
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
		Schema: pushdownSchemaYdb(),
		Records: []*test_utils.Record{
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
		Schema: pushdownSchemaYdb(),
		Records: []*test_utils.Record{
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
		Schema: pushdownSchemaYdb(),
		Records: []*test_utils.Record{
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
		Schema: pushdownSchemaYdb(),
		Records: []*test_utils.Record{
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
		Schema: pushdownSchemaYdb(),
		Records: []*test_utils.Record{
			{
				Columns: map[string]any{
					"id":             []*int32{ptr.Int32(4)},
					"int_column":     []*int32{nil},
					"varchar_column": []*string{nil},
				},
			},
		},
	},
}

func pushdownSchemaYdb() *test_utils.TableSchema {
	return &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			"id":             common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			"int_column":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			"varchar_column": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
		},
	}
}
