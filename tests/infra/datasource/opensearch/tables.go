package opensearch

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
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
		Records: []*test_utils.Record[int32, *array.Int32Builder]{},
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
				"id":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"name": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"address": common.MakeOptionalType(common.MakeStructType([]*Ydb.StructMember{
					{Name: "city", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
					{Name: "country", Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
				})),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{},
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
	},
}

func newInt32IDArrayBuilder(pool memory.Allocator) func() *array.Int32Builder {
	return func() *array.Int32Builder {
		return array.NewInt32Builder(pool)
	}
}
