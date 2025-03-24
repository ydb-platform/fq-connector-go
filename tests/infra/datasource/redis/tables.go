package redis

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/nosql/redis"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var memPool memory.Allocator = memory.NewGoAllocator()

// Таблица для кейса, когда в Redis присутствуют только строковые ключи.
// Ожидаемая схема: колонки "key_" и "string_values".
var stringOnlyTable = &test_utils.Table[int32, *array.Int32Builder]{
	Name:                  "stringOnly",
	IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
	Schema: &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			redis.KeyColumnName:    common.MakePrimitiveType(Ydb.Type_STRING),
			redis.StringColumnName: common.MakePrimitiveType(Ydb.Type_STRING),
		},
	},
	Records: []*test_utils.Record[int32, *array.Int32Builder]{},
}

// Таблица для кейса, когда в Redis присутствуют только hash-ключи.
// Ожидаемая схема: колонки "key_" и "hash_values" (StructType, где набор полей – объединение всех полей из hash).
var hashOnlyTable = &test_utils.Table[int32, *array.Int32Builder]{
	Name:                  "hashOnly",
	IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
	Schema: &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			redis.KeyColumnName: common.MakePrimitiveType(Ydb.Type_STRING),
			redis.HashColumnName: {
				Type: &Ydb.Type_StructType{
					StructType: &Ydb.StructType{
						Members: []*Ydb.StructMember{
							{
								Name: "field1",
								Type: common.MakePrimitiveType(Ydb.Type_STRING),
							},
							{
								Name: "field2",
								Type: common.MakePrimitiveType(Ydb.Type_STRING),
							},
							{
								Name: "field3",
								Type: common.MakePrimitiveType(Ydb.Type_STRING),
							},
						},
					},
				},
			},
		},
	},
	Records: []*test_utils.Record[int32, *array.Int32Builder]{},
}

// Таблица для кейса, когда в Redis присутствуют и строковые, и hash-значения.
// Ожидаемая схема: колонки "key_", "string_values" и "hash_values".
var mixedTable = &test_utils.Table[int32, *array.Int32Builder]{
	Name:                  "mixed",
	IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
	Schema: &test_utils.TableSchema{
		Columns: map[string]*Ydb.Type{
			redis.KeyColumnName:    common.MakePrimitiveType(Ydb.Type_STRING),
			redis.StringColumnName: common.MakePrimitiveType(Ydb.Type_STRING),
			redis.HashColumnName: {
				Type: &Ydb.Type_StructType{
					StructType: &Ydb.StructType{
						Members: []*Ydb.StructMember{
							{
								Name: "hashField1",
								Type: common.MakePrimitiveType(Ydb.Type_STRING),
							},
							{
								Name: "hashField2",
								Type: common.MakePrimitiveType(Ydb.Type_STRING),
							},
						},
					},
				},
			},
		},
	},
	Records: []*test_utils.Record[int32, *array.Int32Builder]{},
}

// Таблица для кейса пустой базы – ожидается пустая схема (без колонок).
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
