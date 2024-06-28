package mysql

import (
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
	// "simple": {
	// 	Name: "simple",
	// 	Schema: &test_utils.TableSchema{
	// 		Columns: map[string]*Ydb.Type{
	// 			"id":                  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
	// 			"tinyint_column":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT8)),
	// 			"smallint_column":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
	// 			"mediumint_column":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
	// 			"unsigned_int_column": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT32)),
	// 			"int_column":          common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
	// 			"varchar_column":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
	// 			"float_column":        common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
	// 			"double_column":       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
	// 			"bool_column":         common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
	// 		},
	// 	},
	// 	Records: []*test_utils.Record{
	// 		{
	// 			Columns: map[string]any{
	// 				"id": []*int32{
	// 					ptr.Int32(1),
	// 					ptr.Int32(2),
	// 					ptr.Int32(3),
	// 				},
	// 				"tinyint_column": []*int8{
	// 					ptr.Int8(-1),
	// 					ptr.Int8(-2),
	// 					ptr.Int8(-2),
	// 				},
	// 				"smallint_column": []*int16{
	// 					ptr.Int16(2),
	// 					nil,
	// 					ptr.Int16(3),
	// 				},
	// 				"mediumint_column": []*int32{
	// 					ptr.Int32(45),
	// 					ptr.Int32(21),
	// 					ptr.Int32(42),
	// 				},
	// 				"unsigned_int_column": []*uint32{
	// 					ptr.Uint32(234),
	// 					ptr.Uint32(532),
	// 					ptr.Uint32(532),
	// 				},
	// 				"int_column": []*int32{
	// 					ptr.Int32(-234),
	// 					ptr.Int32(234),
	// 					ptr.Int32(234),
	// 				},
	// 				"varchar_column": []*string{
	// 					ptr.String("hello"),
	// 					ptr.String("world"),
	// 					ptr.String("!!!"),
	// 				},
	// 				"float_column": []*float32{
	// 					ptr.Float32(4.24),
	// 					ptr.Float32(-4.24),
	// 					ptr.Float32(-1.23),
	// 				},
	// 				"double_column": []*float64{
	// 					nil,
	// 					ptr.Float64(-12.2),
	// 					ptr.Float64(42.1),
	// 				},
	// 				"bool_column": []*uint8{
	// 					ptr.Uint8(1),
	// 					ptr.Uint8(0),
	// 					ptr.Uint8(1),
	// 				},
	// 			},
	// 		},
	// 	},
	// },

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
