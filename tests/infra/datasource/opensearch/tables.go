package opensearch

import (
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var memPool memory.Allocator = memory.NewGoAllocator()

var testIdType = common.MakePrimitiveType(Ydb.Type_UTF8)

var tables = map[string]*test_utils.Table[string, *array.StringBuilder]{
	"simple": {
		Name:                  "simple",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":             testIdType,
				"bool_field":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"int32_field":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"int64_field":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"float_field":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				"double_field":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"string_field":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"timestamp_field": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id":          []string{"0", "1", "2"},
				"bool_field":   []*uint8{ptr.Uint8(1), ptr.Uint8(0), ptr.Uint8(1)},
				"int32_field":  []*int32{ptr.Int32(42), ptr.Int32(-100), ptr.Int32(0)},
				"int64_field":  []*int64{ptr.Int64(1234567890123), ptr.Int64(-987654321), ptr.Int64(0)},
				"float_field":  []*float32{ptr.Float32(1.5), ptr.Float32(-3.14), ptr.Float32(0.0)},
				"double_field": []*float64{ptr.Float64(2.71828), ptr.Float64(0.0), ptr.Float64(-1.2345)},
				"string_field": []*string{
					ptr.String("text_value1"),
					ptr.String("text_value2"),
					ptr.String("text_value3"),
				},
				"timestamp_field": []*uint64{
					ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
						time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))),
					ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
						time.Date(2023, 2, 15, 12, 0, 0, 0, time.UTC))),
					ptr.Uint64(common.MustTimeToYDBType(common.TimeToYDBTimestamp,
						time.Date(2023, 3, 20, 18, 30, 0, 0, time.UTC))),
				},
			},
		}},
	},
	"list": {
		Name:                  "list",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":  testIdType,
				"name": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"tags": common.MakeOptionalType(common.MakeListType(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)))),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{},
	},
	"nested": {
		Name:                  "nested",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":  testIdType,
				"name": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"nested": common.MakeOptionalType(common.MakeStructType([]*Ydb.StructMember{
					{Name: "binary_field", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING))},
					{Name: "bool_field", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL))},
					{Name: "double_field", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE))},
					{Name: "float_field", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT))},
					{Name: "int32_field", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32))},
					{Name: "int64_field", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64))},
					{Name: "string_field", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
					{Name: "timestamp_field", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP))},
				})),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id":  []string{"0", "1"},
				"name": []*string{ptr.String("Alice"), ptr.String("Bob")},
				"nested": []map[string]*any{
					{
						"bool_field":   ptr.T[any](uint8(1)),
						"int32_field":  ptr.T[any](int32(42)),
						"int64_field":  ptr.T[any](int64(1234567890123)),
						"float_field":  ptr.T[any](float32(3.14)),
						"double_field": ptr.T[any](3.1415926535912345678910101),
						"string_field": ptr.T[any]("value1"),
						"timestamp_field": ptr.T[any](common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(2023, 7, 20, 12, 0, 0, 0, time.UTC))),
						"binary_field": ptr.T[any]([]byte{
							0x53, 0x47, 0x56, 0x73, 0x62, 0x47, 0x38, 0x67,
							0x51, 0x57, 0x78, 0x70, 0x59, 0x32, 0x55, 0x3d,
						}),
					},
					{
						"bool_field":   ptr.T[any](uint8(0)),
						"int32_field":  ptr.T[any](int32(24)),
						"int64_field":  ptr.T[any](int64(9876543210987)),
						"float_field":  ptr.T[any](float32(2.71)),
						"double_field": ptr.T[any](2.7182818284512345678910101),
						"string_field": ptr.T[any]("value2"),
						"timestamp_field": ptr.T[any](common.MustTimeToYDBType(common.TimeToYDBTimestamp,
							time.Date(2023, 7, 21, 15, 30, 0, 0, time.UTC))),
						"binary_field": ptr.T[any]([]byte{
							0x53, 0x47, 0x56, 0x73, 0x62, 0x47,
							0x38, 0x67, 0x51, 0x6d, 0x39, 0x69,
						}),
					},
				},
			},
		}},
	},

	"nested_list": {
		Name:                  "nested_list",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":     testIdType,
				"company": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"employees": common.MakeOptionalType(common.MakeListType(
					common.MakeOptionalType(common.MakeStructType([]*Ydb.StructMember{
						{
							Name: "id",
							Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
						},
						{
							Name: "name",
							Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
						},
						{
							Name: "skills",
							Type: common.MakeOptionalType(common.MakeListType(
								common.MakeOptionalType(common.MakeStructType([]*Ydb.StructMember{
									{
										Name: "level",
										Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
									},
									{
										Name: "name",
										Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
									},
								})),
							)),
						},
					})),
				)),
			},
		},
	},
	"optional": {
		Name:                  "optional",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"b":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"c":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"d":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"1", "2", "3", "4"},
				"a": []*string{
					ptr.String("value1"),
					ptr.String("value2"),
					ptr.String("value3"),
					ptr.String("value4")},
				"b": []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30), nil},
				"c": []*string{nil, ptr.String("new_field"), nil, ptr.String("another_value")},
				"d": []*float32{nil, nil, ptr.Float32(3.14), ptr.Float32(2.71)},
			},
		}},
	},
	"pushdown_projection": {
		Name:                  "pushdown_projection",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":             testIdType,
				"bool_field":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"int32_field":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"int64_field":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"float_field":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				"double_field":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"string_field":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"timestamp_field": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id":         []string{"0", "1", "2"},
				"int32_field": []*int32{ptr.Int32(42), ptr.Int32(-100), ptr.Int32(0)},
			},
		}},
	},
	"pushdown_null": {
		Name:                  "pushdown_null_checks",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"int32":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"double":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"boolean":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"int64":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"string":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"objectid": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id":      []string{"1"},
				"int32":    []*int32{nil},
				"double":   []*float64{nil},
				"boolean":  []*uint8{nil},
				"int64":    []*int64{ptr.Int64(456)},
				"string":   []*string{nil},
				"objectid": []*string{nil},
			},
		}},
	},
	"pushdown_not_null": {
		Name:                  "pushdown_null_checks",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"int32":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"double":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"boolean":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"int64":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"string":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"objectid": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id":     []string{"0", "2"},
				"int32":   []*int32{ptr.Int32(42), ptr.Int32(24)},
				"double":  []*float64{ptr.Float64(1.1), ptr.Float64(2.2)},
				"boolean": []*uint8{ptr.Uint8(1), ptr.Uint8(0)},
				"int64":   []*int64{ptr.Int64(123), nil},
				"string":  []*string{ptr.String("exists"), ptr.String("exists2")},
				"objectid": []*string{
					ptr.String("507f1f77bcf86cd799439011"),
					ptr.String("507f1f77bcf86cd799439012"),
				},
			},
		}},
	},
	"pushdown_comparisons_eq": {
		Name:                  "pushdown_comparisons",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"ind":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"int32":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"int64":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"string":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"binary":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"double":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"boolean":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"objectid": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id":     []string{"0"},
				"ind":     []*int32{ptr.Int32(0)},
				"int32":   []*int32{ptr.Int32(64)},
				"int64":   []*int64{ptr.Int64(23423)},
				"string":  []*string{ptr.String("outer")},
				"binary":  []*[]byte{ptr.Bytes([]byte("q80="))},
				"double":  []*float64{ptr.Float64(1.1)},
				"boolean": []*uint8{ptr.Uint8(0)},
				"objectid": []*string{
					ptr.String("507f1f77bcf86cd799439011"),
				},
			},
		}},
	},
	"strcomp_0": {
		Name:                  "pushdown_string_comps",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"0"},
				"a":   []*string{ptr.String("abc def")},
			},
		}},
	},
	"strcomp_1": {
		Name:                  "pushdown_string_comps",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"1"},
				"a":   []*string{ptr.String("def abc")},
			},
		}},
	},
	"strcomp": {
		Name:                  "pushdown_string_comps",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"0", "1"},
				"a":   []*string{ptr.String("abc def"), ptr.String("def abc")},
			},
		}},
	},
	"columns_l": {
		Name:                  "pushdown_two_columns",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"ind": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"2"},
				"ind": []*int32{ptr.Int32(1)},
				"a":   []*int32{ptr.Int32(1)},
			},
		}},
	},
	"columns_le": {
		Name:                  "pushdown_two_columns",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"ind": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"1", "2"},
				"ind": []*int32{ptr.Int32(3), ptr.Int32(1)},
				"a":   []*int32{ptr.Int32(2), ptr.Int32(1)},
			},
		}},
	},
	"columns_g": {
		Name:                  "pushdown_two_columns",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"ind": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"0", "1"},
				"ind": []*int32{ptr.Int32(5), ptr.Int32(3)},
				"a":   []*int32{ptr.Int32(6), ptr.Int32(2)},
			},
		}},
	},
	"columns_ge": {
		Name:                  "pushdown_two_columns",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"ind": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"0", "1", "2"},
				"ind": []*int32{ptr.Int32(5), ptr.Int32(3), ptr.Int32(1)},
				"a":   []*int32{ptr.Int32(6), ptr.Int32(2), ptr.Int32(1)},
			},
		}},
	},
	"conj": {
		Name:                  "pushdown_logical_ops",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"b":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"ind": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"0"},
				"a":   []*int32{ptr.Int32(1)},
				"b":   []*string{ptr.String("hello")},
				"ind": []*int32{ptr.Int32(0)},
			},
		}},
	},
	"disj": {
		Name:                  "pushdown_logical_ops",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"b":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"ind": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"0", "1", "2"},
				"a":   []*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(1)},
				"b":   []*string{ptr.String("hello"), ptr.String("hi"), ptr.String("world")},
				"ind": []*int32{ptr.Int32(0), ptr.Int32(1), ptr.Int32(2)},
			},
		}},
	},
	"neg": {
		Name:                  "pushdown_null_checks",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"int32":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"double":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"boolean":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"int64":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"string":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"objectid": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id":     []string{"2"},
				"int32":   []*int32{ptr.Int32(24)},
				"double":  []*float64{ptr.Float64(2.2)},
				"boolean": []*uint8{ptr.Uint8(0)},
				"int64":   []*int64{nil},
				"string":  []*string{ptr.String("exists2")},
				"objectid": []*string{
					ptr.String("507f1f77bcf86cd799439012"),
				},
			},
		}},
	},
	"bool": {
		Name:                  "pushdown_comparisons",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"ind":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"int32":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"int64":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"string":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"binary":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"double":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"boolean":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"objectid": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id":     []string{"1"},
				"ind":     []*int32{ptr.Int32(1)},
				"int32":   []*int32{ptr.Int32(32)},
				"int64":   []*int64{ptr.Int64(12345)},
				"string":  []*string{ptr.String("inner")},
				"binary":  []*[]byte{ptr.Bytes([]byte("q81="))},
				"double":  []*float64{ptr.Float64(2.2)},
				"boolean": []*uint8{ptr.Uint8(1)},
				"objectid": []*string{
					ptr.String("507f1f77bcf86cd799439012"),
				},
			},
		}},
	},
	"neg_bool": {
		Name:                  "pushdown_comparisons",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"ind":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"int32":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"int64":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"string":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"binary":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				"double":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"boolean":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"objectid": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id":     []string{"0"},
				"ind":     []*int32{ptr.Int32(0)},
				"int32":   []*int32{ptr.Int32(64)},
				"int64":   []*int64{ptr.Int64(23423)},
				"string":  []*string{ptr.String("outer")},
				"binary":  []*[]byte{ptr.Bytes([]byte("q80="))},
				"double":  []*float64{ptr.Float64(1.1)},
				"boolean": []*uint8{ptr.Uint8(0)},
				"objectid": []*string{
					ptr.String("507f1f77bcf86cd799439011"),
				},
			},
		}},
	},
	"pushdown_between": {
		Name:                  "pushdown_between",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"ind": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"1", "2"},
				"ind": []*int32{ptr.Int32(3), ptr.Int32(4)},
			},
		}},
	},
	"pushdown_in": {
		Name:                  "pushdown_in",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"b":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"ind": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"1", "3", "4"},
				"b":   []*string{ptr.String("four"), ptr.String("two"), ptr.String("hi")},
				"ind": []*int32{ptr.Int32(4), ptr.Int32(2), ptr.Int32(5)},
			},
		}},
	},
	"pushdown_regex": {
		Name:                  "pushdown_regex",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{{
			Columns: map[string]any{
				"_id": []string{"0"},
				"a":   []*string{ptr.String("toast is great")},
			},
		}},
	},
}

func newStringIDArrayBuilder(pool memory.Allocator) func() *array.StringBuilder {
	return func() *array.StringBuilder {
		return array.NewStringBuilder(pool)
	}
}
