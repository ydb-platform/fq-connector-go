package opensearch

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var memPool memory.Allocator = memory.NewGoAllocator()

var testIdType = common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32))

var tables = map[string]*test_utils.Table[int32, *array.Int32Builder]{
	"simple": {
		Name:                  "simple",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id": testIdType,
				"a":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"b":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"c":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"id": []*int32{ptr.Int32(0), ptr.Int32(1), ptr.Int32(2)},
				"a":  []*string{ptr.String("jelly"), ptr.String("butter"), ptr.String("toast")},
				"b":  []*int32{ptr.Int32(2000), ptr.Int32(-20021), ptr.Int32(2076)},
				"c":  []*int64{ptr.Int64(13), ptr.Int64(0), ptr.Int64(2076)},
			},
		}},
	},
	"list": {
		Name:                  "list",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":   testIdType,
				"name": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"tags": common.MakeOptionalType(common.MakeListType(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)))),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{},
	},
	"nested": {
		Name:                  "nested",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":   testIdType,
				"name": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"address": common.MakeOptionalType(common.MakeStructType([]*Ydb.StructMember{
					{Name: "city", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
					{Name: "country", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
				})),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{},
	},

	"nested_list": {
		Name:                  "nested_list",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":      testIdType,
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
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id": testIdType,
				"a":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"b":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"c":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"d":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"id": []*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3), ptr.Int32(4)},
				"a":  []*string{ptr.String("value1"), ptr.String("value2"), ptr.String("value3"), ptr.String("value4")},
				"b":  []*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30), nil},
				"c":  []*string{nil, ptr.String("new_field"), nil, ptr.String("another_value")},
				"d":  []*float32{nil, nil, ptr.Float32(3.14), ptr.Float32(2.71)},
			},
		}},
	},
}

func newInt32IDArrayBuilder(pool memory.Allocator) func() *array.Int32Builder {
	return func() *array.Int32Builder {
		return array.NewInt32Builder(pool)
	}
}
