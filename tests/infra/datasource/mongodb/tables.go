package mongodb

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var memPool memory.Allocator = memory.NewGoAllocator()

var tables = map[string]*test_utils.Table[string, *array.StringBuilder]{
	"simple": {
		Name:                  "simple",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": common.MakePrimitiveType(Ydb.Type_STRING),
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"b":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"c":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{},
	},
	"primitives": {
		Name:                  "primitives",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":     common.MakePrimitiveType(Ydb.Type_STRING),
				"int32":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"int64":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"string":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"double":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"boolean": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"binary":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{},
	},
	"missing": {
		Name:                  "missing",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":     common.MakePrimitiveType(Ydb.Type_STRING),
				"int32":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"int64":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				"string":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"double":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"boolean": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				"binary":  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{},
	},
	"uneven": {
		Name:                  "uneven",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": common.MakePrimitiveType(Ydb.Type_STRING),
				"a":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"b":   common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{},
	},
	"nested": {
		Name:                  "nested",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":    common.MakePrimitiveType(Ydb.Type_STRING),
				"arr":    common.MakeOptionalType(common.MakeListType(common.MakePrimitiveType(Ydb.Type_INT32))),
				"struct": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_JSON)),
				"nested": common.MakeOptionalType(common.MakeListType(common.MakePrimitiveType(Ydb.Type_JSON))),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{},
	},
	"datetime": {
		Name:                  "datetime",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":  common.MakePrimitiveType(Ydb.Type_STRING),
				"date": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INTERVAL)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{},
	},
	"unsupported": {
		Name:                  "unsupported",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":     common.MakePrimitiveType(Ydb.Type_STRING),
				"decimal": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{},
	},
	"simple_json": {
		Name:                  "simple",
		IDArrayBuilderFactory: newStringIDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":    common.MakePrimitiveType(Ydb.Type_STRING),
				"simple": common.MakePrimitiveType(Ydb.Type_JSON),
			},
		},
		Records: []*test_utils.Record[string, *array.StringBuilder]{},
	},
}

func newStringIDArrayBuilder(pool memory.Allocator) func() *array.StringBuilder {
	return func() *array.StringBuilder {
		return array.NewStringBuilder(pool)
	}
}
