package redis

import (
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/datasource/nosql/redis"

	"github.com/ydb-platform/fq-connector-go/library/go/ptr"

	"github.com/ydb-platform/fq-connector-go/common"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var memPool memory.Allocator = memory.NewGoAllocator()

// Table for the case when only string keys are present in Redis.
// Expected schema: columns "key" and "string_values".
var stringOnlyTable = &test_utils.Table[[]byte, *array.BinaryBuilder]{
	Name:                  "stringOnly:*",
	IDArrayBuilderFactory: newBinaryIDArrayBuilder(memPool),
	Schema: &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			redis.KeyColumnName:    common.MakePrimitiveType(Ydb.Type_STRING),
			redis.StringColumnName: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
		},
	},
	Records: []*test_utils.Record[[]byte, *array.BinaryBuilder]{{
		Columns: map[string]any{
			redis.KeyColumnName:    [][]byte{[]byte("stringOnly:stringKey1"), []byte("stringOnly:stringKey2")},
			redis.StringColumnName: []*[]byte{ptr.Bytes([]byte("value1")), ptr.Bytes([]byte("value2"))},
		},
	}},
}

// Table for the case when only hash keys are present in Redis.
// Expected schema: columns "key" and "hash_values", where hash_values is an OptionalType wrapping a StructType
// with members being the union of all hash fields.
var hashOnlyTable = &test_utils.Table[[]byte, *array.BinaryBuilder]{
	Name:                  "hashOnly:*",
	IDArrayBuilderFactory: newBinaryIDArrayBuilder(memPool),
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
	Records: []*test_utils.Record[[]byte, *array.BinaryBuilder]{{
		Columns: map[string]any{
			redis.KeyColumnName: [][]byte{[]byte("hashOnly:hashKey1"), []byte("hashOnly:hashKey2")},
			redis.HashColumnName: []map[string]*[]byte{
				{
					"field1": ptr.Bytes([]byte("hashValue1")),
					"field2": ptr.Bytes([]byte("hashValue2")),
					"field3": nil,
				},
				{
					"field1": ptr.Bytes([]byte("hashValue3")),
					"field2": ptr.Bytes([]byte("hashValue4")),
					"field3": ptr.Bytes([]byte("hashValue5")),
				},
			},
		},
	}},
}

// Table for the case when both string and hash keys are present in Redis.
// Expected schema: columns "key", "string_values" and "hash_values" (OptionalType wrapping a StructType).
var mixedTable = &test_utils.Table[[]byte, *array.BinaryBuilder]{
	Name:                  "mixed:*",
	IDArrayBuilderFactory: newBinaryIDArrayBuilder(memPool),
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
	Records: []*test_utils.Record[[]byte, *array.BinaryBuilder]{{
		Columns: map[string]any{
			redis.KeyColumnName:    [][]byte{[]byte("mixed:hashKey2"), []byte("mixed:stringKey1")},
			redis.StringColumnName: []*[]byte{nil, ptr.Bytes([]byte("mixedString"))},
			redis.HashColumnName: []map[string]*[]byte{
				{
					"hashField1": ptr.Bytes([]byte("mixedHash1")),
					"hashField2": ptr.Bytes([]byte("mixedHash2")),
				},
				nil,
			},
		},
	}},
}

// Table for the case of an empty database – expected schema: no columns.
var emptyTable = &test_utils.Table[[]byte, *array.BinaryBuilder]{
	Name:                  "empty:*",
	IDArrayBuilderFactory: newBinaryIDArrayBuilder(memPool),
	Schema: &test_utils.TableSchema{
		Columns: make(map[string]*Ydb.Type, 0),
	},
	Records: make([]*test_utils.Record[[]byte, *array.BinaryBuilder], 0),
}

var tables = map[string]*test_utils.Table[[]byte, *array.BinaryBuilder]{
	"stringOnly": stringOnlyTable,
	"hashOnly":   hashOnlyTable,
	"mixed":      mixedTable,
	"empty":      emptyTable,
}

func newBinaryIDArrayBuilder(pool memory.Allocator) func() *array.BinaryBuilder {
	return func() *array.BinaryBuilder {
		return array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	}
}
