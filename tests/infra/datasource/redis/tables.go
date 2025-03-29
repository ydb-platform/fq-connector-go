package redis

import (
	"fmt"

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
			redis.KeyColumnName:    []string{"stringOnly:stringKey1", "stringOnly:stringKey2"},
			redis.StringColumnName: []*string{ptr.String("value1"), ptr.String("value2")},
		},
	}},
}

// Table for the case when only hash keys are present in Redis.
// Expected schema: columns "key" and "hash_values", where hash_values is an OptionalType wrapping a StructType
// with members being the union of all hash fields.
var hashOnlyTable = func() *test_utils.Table[[]byte, *array.BinaryBuilder] {
	field1Value1 := "hashValue1"
	field2Value1 := "hashValue2"
	var field3Value1 *string = nil

	field1Value2 := "hashValue3"
	field2Value2 := "hashValue4"
	field3Value2 := "hashValue5"

	hashRecord1 := map[string]*string{
		"field1": &field1Value1,
		"field2": &field2Value1,
		"field3": field3Value1,
	}

	hashRecord2 := map[string]*string{
		"field1": &field1Value2,
		"field2": &field2Value2,
		"field3": &field3Value2,
	}

	// Печатаем тип для дебага
	fmt.Printf("DEBUG hashRecord1 type: %T, value: %v\n", hashRecord1, hashRecord1)
	fmt.Printf("DEBUG []map[string]*string{hashRecord1, hashRecord2} type: %T\n", []map[string]*string{hashRecord1, hashRecord2})

	return &test_utils.Table[[]byte, *array.BinaryBuilder]{
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
				redis.KeyColumnName: []string{"hashOnly:hashKey1", "hashOnly:hashKey2"},
				redis.HashColumnName: []map[string]*string{
					hashRecord1,
					hashRecord2,
				},
			},
		}},
	}
}()

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
			redis.KeyColumnName:    []string{"mixed:stringKey1", "mixed:hashKey2"},
			redis.StringColumnName: []*string{ptr.String("mixedString"), nil},
			redis.HashColumnName: []map[string]*string{
				nil,
				{
					"hashField1": ptr.String("mixedHash1"),
					"hashField2": ptr.String("mixedHash2"),
				},
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
