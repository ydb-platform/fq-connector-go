package redis

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/datasource/nosql/redis"

	"github.com/ydb-platform/fq-connector-go/common"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var memPool memory.Allocator = memory.NewGoAllocator()

// Table for the case when only string keys are present in Redis.
// Expected schema: columns "key" and "string_values".
var stringOnlyTable = &test_utils.Table[int32, *array.Int32Builder]{
	Name:                  "stringOnly",
	IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
	Schema: &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			redis.KeyColumnName:    common.MakePrimitiveType(Ydb.Type_STRING),
			redis.StringColumnName: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
		},
	},
	Records: []*test_utils.Record[int32, *array.Int32Builder]{},
}

// Table for the case when only hash keys are present in Redis.
// Expected schema: columns "key" and "hash_values", where hash_values is an OptionalType wrapping a StructType
// with members being the union of all hash fields.
var hashOnlyTable = &test_utils.Table[int32, *array.Int32Builder]{
	Name:                  "hashOnly",
	IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
	Schema: &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			redis.KeyColumnName: common.MakePrimitiveType(Ydb.Type_STRING),
			redis.HashColumnName: common.MakeOptionalType(&Ydb.Type{
				Type: &Ydb.Type_StructType{
					StructType: &Ydb.StructType{
						Members: []*Ydb.StructMember{
							{
								Name: "field1",
								Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
							},
							{
								Name: "field2",
								Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
							},
							{
								Name: "field3",
								Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
							},
						},
					},
				},
			}),
		},
	},
	Records: []*test_utils.Record[int32, *array.Int32Builder]{},
}

// Table for the case when both string and hash keys are present in Redis.
// Expected schema: columns "key", "string_values" and "hash_values" (OptionalType wrapping a StructType).
var mixedTable = &test_utils.Table[int32, *array.Int32Builder]{
	Name:                  "mixed",
	IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
	Schema: &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			redis.KeyColumnName:    common.MakePrimitiveType(Ydb.Type_STRING),
			redis.StringColumnName: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
			redis.HashColumnName: common.MakeOptionalType(&Ydb.Type{
				Type: &Ydb.Type_StructType{
					StructType: &Ydb.StructType{
						Members: []*Ydb.StructMember{
							{
								Name: "hashField1",
								Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
							},
							{
								Name: "hashField2",
								Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
							},
						},
					},
				},
			}),
		},
	},
	Records: []*test_utils.Record[int32, *array.Int32Builder]{},
}

// Table for the case of an empty database – expected schema: no columns.
var emptyTable = &test_utils.Table[int32, *array.Int32Builder]{
	Name:                  "empty",
	IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
	Schema: &test_utils.TableSchema{
		Columns: make(map[string]*Ydb.Type, 0),
	},
	Records: make([]*test_utils.Record[int32, *array.Int32Builder], 0),
}

var tables = map[string]*test_utils.Table[int32, *array.Int32Builder]{
	"stringOnly": stringOnlyTable,
	"hashOnly":   hashOnlyTable,
	"mixed":      mixedTable,
	"empty":      emptyTable,
}

func newInt32IDArrayBuilder(pool memory.Allocator) func() *array.Int32Builder {
	return func() *array.Int32Builder {
		return array.NewInt32Builder(pool)
	}
}
