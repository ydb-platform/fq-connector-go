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

var testIdType = common.MakePrimitiveType(Ydb.Type_INT64)

var tables = map[string]*test_utils.Table[int64, *array.Int64Builder]{
	"simple": {
		Name:                  "simple",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
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
		Records: []*test_utils.Record[int64, *array.Int64Builder]{{
			Columns: map[string]any{
				"_id":          []int64{0, 1, 2},
				"bool_field":   []*uint8{ptr.Uint8(1), ptr.Uint8(0), ptr.Uint8(1)},
				"int32_field":  []*int32{ptr.Int32(42), ptr.Int32(-100), ptr.Int32(0)},
				"int64_field":  []*int64{ptr.Int64(1234567890123), ptr.Int64(-987654321), ptr.Int64(0)},
				"float_field":  []*float32{ptr.Float32(1.5), ptr.Float32(-3.14), ptr.Float32(0.0)},
				"double_field": []*float64{ptr.Float64(2.71828), ptr.Float64(0.0), ptr.Float64(-1.2345)},
				"string_field": []*string{ptr.String("text_value1"), ptr.String("text_value2"), ptr.String("text_value3")},
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
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":  testIdType,
				"name": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"tags": common.MakeOptionalType(common.MakeListType(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)))),
			},
		},
		Records: []*test_utils.Record[int64, *array.Int64Builder]{},
	},
	"nested": {
		Name:                  "nested",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":  testIdType,
				"name": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"address": common.MakeOptionalType(common.MakeStructType([]*Ydb.StructMember{
					{Name: "city", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
					{Name: "country", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
				})),
			},
		},
		Records: []*test_utils.Record[int64, *array.Int64Builder]{},
	},

	"nested_list": {
		Name:                  "nested_list",
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
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
		IDArrayBuilderFactory: newInt64IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"b":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"c":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"d":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
			},
		},
		Records: []*test_utils.Record[int64, *array.Int64Builder]{{
			Columns: map[string]any{
				"_id": []int64{1, 2, 3, 4},
				"a":   []*string{ptr.String("value1"), ptr.String("value2"), ptr.String("value3"), ptr.String("value4")},
				"b":   []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30), nil},
				"c":   []*string{nil, ptr.String("new_field"), nil, ptr.String("another_value")},
				"d":   []*float32{nil, nil, ptr.Float32(3.14), ptr.Float32(2.71)},
			},
		}},
	},
}

func newInt64IDArrayBuilder(pool memory.Allocator) func() *array.Int64Builder {
	return func() *array.Int64Builder {
		return array.NewInt64Builder(pool)
	}
}
