package mongodb

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

var memPool memory.Allocator = memory.NewGoAllocator()

var testIdType = Optional(Primitive(Ydb.Type_INT32))
var objectIdType = Primitive(Ydb.Type_STRING)

var tables = map[string]*test_utils.Table[int32, *array.Int32Builder]{
	"simple": {
		Name:                  "simple",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   Optional(Primitive(Ydb.Type_UTF8)),
				"b":   Optional(Primitive(Ydb.Type_INT32)),
				"c":   Optional(Primitive(Ydb.Type_INT64)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(0), ptr.Int32(1), ptr.Int32(2)},
				"a":   []*string{ptr.String("jelly"), ptr.String("butter"), ptr.String("toast")},
				"b":   []*int32{ptr.Int32(2000), ptr.Int32(-20021), ptr.Int32(2076)},
				"c":   []*int64{ptr.Int64(13), ptr.Int64(0), ptr.Int64(2076)},
			},
		}},
	},
	"simple_last": {
		Name:                  "simple",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   Optional(Primitive(Ydb.Type_UTF8)),
				"b":   Optional(Primitive(Ydb.Type_INT32)),
				"c":   Optional(Primitive(Ydb.Type_INT64)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(2)},
				"a":   []*string{ptr.String("toast")},
				"b":   []*int32{ptr.Int32(2076)},
				"c":   []*int64{ptr.Int64(2076)},
			},
		}},
	},
	"primitives": {
		Name:                  "primitives",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"int32":    Optional(Primitive(Ydb.Type_INT32)),
				"int64":    Optional(Primitive(Ydb.Type_INT64)),
				"string":   Optional(Primitive(Ydb.Type_UTF8)),
				"double":   Optional(Primitive(Ydb.Type_DOUBLE)),
				"boolean":  Optional(Primitive(Ydb.Type_BOOL)),
				"binary":   Optional(Primitive(Ydb.Type_STRING)),
				"objectid": Optional(objectIdType),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id":     []*int32{ptr.Int32(0), ptr.Int32(1), ptr.Int32(2)},
				"int32":   []*int32{ptr.Int32(42), ptr.Int32(13), ptr.Int32(15)},
				"int64":   []*int64{ptr.Int64(23423), ptr.Int64(13), ptr.Int64(15)},
				"string":  []*string{ptr.String("hello"), ptr.String("hi"), ptr.String("bye")},
				"double":  []*float64{ptr.Float64(1.22), ptr.Float64(1.23), ptr.Float64(1.24)},
				"boolean": []*uint8{ptr.Uint8(1), ptr.Uint8(0), ptr.Uint8(0)},
				"binary":  []*[]byte{ptr.T([]byte{0xaa, 0xaa}), ptr.T([]byte{0xab, 0xab}), ptr.T([]byte{0xac, 0xac})},
				"objectid": []*[]byte{
					ptr.T([]byte(string("171e75500ecde1c75c59139e"))),
					ptr.T([]byte(string("271e75500ecde1c75c59139e"))),
					ptr.T([]byte(string("371e75500ecde1c75c59139e"))),
				},
			},
		}},
	},
	"missing": {
		Name:                  "missing",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"int32":    Optional(Primitive(Ydb.Type_INT32)),
				"int64":    Optional(Primitive(Ydb.Type_INT64)),
				"string":   Optional(Primitive(Ydb.Type_UTF8)),
				"double":   Optional(Primitive(Ydb.Type_DOUBLE)),
				"boolean":  Optional(Primitive(Ydb.Type_BOOL)),
				"binary":   Optional(Primitive(Ydb.Type_STRING)),
				"objectid": Optional(objectIdType),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id":      []*int32{ptr.Int32(0), ptr.Int32(1), ptr.Int32(2)},
				"int32":    []*int32{ptr.Int32(64), ptr.Int32(32), nil},
				"int64":    []*int64{ptr.Int64(23423), nil, nil},
				"string":   []*string{ptr.String("outer"), nil, nil},
				"double":   []*float64{ptr.Float64(1.1), ptr.Float64(1.2), nil},
				"boolean":  []*uint8{ptr.Uint8(0), ptr.Uint8(1), nil},
				"binary":   []*[]byte{ptr.T([]byte{0xab, 0xcd}), nil, nil},
				"objectid": []*[]byte{ptr.T([]byte(string("171e75500ecde1c75c59139e"))), nil, nil},
			},
		}},
	},
	"uneven": {
		Name:                  "uneven",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   Optional(Primitive(Ydb.Type_UTF8)),
				"b":   Optional(Primitive(Ydb.Type_UTF8)),
				"c":   Optional(Primitive(Ydb.Type_UTF8)),
				"d":   Optional(Primitive(Ydb.Type_UTF8)),
				"e":   Optional(Primitive(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(0), ptr.Int32(1)},
				"a":   []*string{ptr.String("32"), ptr.String("42")},
				"b":   []*string{ptr.String("{foo: 32}"), ptr.String("b")},
				"c":   []*string{ptr.String("bye"), ptr.String("rKw=")},
				"d":   []*string{ptr.String("1.24"), ptr.String("371e75500ecde1c75c59139e")},
				"e":   []*string{ptr.String("false"), ptr.String("0")},
			},
		}},
	},
	"unsupported": {
		Name:                  "unsupported",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":     testIdType,
				"decimal": Optional(Primitive(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id":     []*int32{ptr.Int32(2202)},
				"decimal": []*string{ptr.String("9823.1297")},
			},
		}},
	},
	"primitives_int32": {
		Name:                  "primitives",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"int32":    Optional(Primitive(Ydb.Type_INT32)),
				"int64":    Optional(Primitive(Ydb.Type_INT64)),
				"string":   Optional(Primitive(Ydb.Type_UTF8)),
				"double":   Optional(Primitive(Ydb.Type_DOUBLE)),
				"boolean":  Optional(Primitive(Ydb.Type_BOOL)),
				"binary":   Optional(Primitive(Ydb.Type_STRING)),
				"objectid": Optional(objectIdType),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id":   []*int32{ptr.Int32(0), ptr.Int32(1), ptr.Int32(2)},
				"int32": []*int32{ptr.Int32(42), ptr.Int32(13), ptr.Int32(15)},
			},
		}},
	},
	"missing_0": {
		Name:                  "missing",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"int32":    Optional(Primitive(Ydb.Type_INT32)),
				"int64":    Optional(Primitive(Ydb.Type_INT64)),
				"string":   Optional(Primitive(Ydb.Type_UTF8)),
				"double":   Optional(Primitive(Ydb.Type_DOUBLE)),
				"boolean":  Optional(Primitive(Ydb.Type_BOOL)),
				"binary":   Optional(Primitive(Ydb.Type_STRING)),
				"objectid": Optional(objectIdType),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id":      []*int32{ptr.Int32(0)},
				"int32":    []*int32{ptr.Int32(64)},
				"int64":    []*int64{ptr.Int64(23423)},
				"string":   []*string{ptr.String("outer")},
				"double":   []*float64{ptr.Float64(1.1)},
				"boolean":  []*uint8{ptr.Uint8(0)},
				"binary":   []*[]byte{ptr.T([]byte{0xab, 0xcd})},
				"objectid": []*[]byte{ptr.T([]byte(string("171e75500ecde1c75c59139e")))},
			},
		}},
	},
	"missing_1": {
		Name:                  "missing",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"int32":    Optional(Primitive(Ydb.Type_INT32)),
				"int64":    Optional(Primitive(Ydb.Type_INT64)),
				"string":   Optional(Primitive(Ydb.Type_UTF8)),
				"double":   Optional(Primitive(Ydb.Type_DOUBLE)),
				"boolean":  Optional(Primitive(Ydb.Type_BOOL)),
				"binary":   Optional(Primitive(Ydb.Type_STRING)),
				"objectid": Optional(objectIdType),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id":      []*int32{ptr.Int32(1)},
				"int32":    []*int32{ptr.Int32(32)},
				"int64":    []*int64{nil},
				"string":   []*string{nil},
				"double":   []*float64{ptr.Float64(1.2)},
				"boolean":  []*uint8{ptr.Uint8(1)},
				"binary":   []*[]byte{nil},
				"objectid": []*[]byte{nil},
			},
		}},
	},
	"missing_2": {
		Name:                  "missing",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"int32":    Optional(Primitive(Ydb.Type_INT32)),
				"int64":    Optional(Primitive(Ydb.Type_INT64)),
				"string":   Optional(Primitive(Ydb.Type_UTF8)),
				"double":   Optional(Primitive(Ydb.Type_DOUBLE)),
				"boolean":  Optional(Primitive(Ydb.Type_BOOL)),
				"binary":   Optional(Primitive(Ydb.Type_STRING)),
				"objectid": Optional(objectIdType),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id":      []*int32{ptr.Int32(2)},
				"int32":    []*int32{nil},
				"int64":    []*int64{nil},
				"string":   []*string{nil},
				"double":   []*float64{nil},
				"boolean":  []*uint8{nil},
				"binary":   []*[]byte{nil},
				"objectid": []*[]byte{nil},
			},
		}},
	},
	"missing_12": {
		Name:                  "missing",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"int32":    Optional(Primitive(Ydb.Type_INT32)),
				"int64":    Optional(Primitive(Ydb.Type_INT64)),
				"string":   Optional(Primitive(Ydb.Type_UTF8)),
				"double":   Optional(Primitive(Ydb.Type_DOUBLE)),
				"boolean":  Optional(Primitive(Ydb.Type_BOOL)),
				"binary":   Optional(Primitive(Ydb.Type_STRING)),
				"objectid": Optional(objectIdType),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id":      []*int32{ptr.Int32(1), ptr.Int32(2)},
				"int32":    []*int32{ptr.Int32(32), nil},
				"int64":    []*int64{nil, nil},
				"string":   []*string{nil, nil},
				"double":   []*float64{ptr.Float64(1.2), nil},
				"boolean":  []*uint8{ptr.Uint8(1), nil},
				"binary":   []*[]byte{nil, nil},
				"objectid": []*[]byte{nil, nil},
			},
		}},
	},
	"similar_0": {
		Name:                  "similar",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   Optional(Primitive(Ydb.Type_INT32)),
				"b":   Optional(Primitive(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(0)},
				"a":   []*int32{ptr.Int32(1)},
				"b":   []*string{ptr.String("hello")},
			},
		}},
	},
	"similar_01": {
		Name:                  "similar",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   Optional(Primitive(Ydb.Type_INT32)),
				"b":   Optional(Primitive(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(0), ptr.Int32(1)},
				"a":   []*int32{ptr.Int32(1), ptr.Int32(1)},
				"b":   []*string{ptr.String("hello"), ptr.String("hi")},
			},
		}},
	},
	"similar_234": {
		Name:                  "similar",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   Optional(Primitive(Ydb.Type_INT32)),
				"b":   Optional(Primitive(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(2), ptr.Int32(3), ptr.Int32(4)},
				"a":   []*int32{ptr.Int32(2), ptr.Int32(2), ptr.Int32(2)},
				"b":   []*string{ptr.String("hello"), ptr.String("one"), ptr.String("two")},
			},
		}},
	},
	"similar_146": {
		Name:                  "similar",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   Optional(Primitive(Ydb.Type_INT32)),
				"b":   Optional(Primitive(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(1), ptr.Int32(4), ptr.Int32(6)},
				"a":   []*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(9)},
				"b":   []*string{ptr.String("hi"), ptr.String("two"), ptr.String("four")},
			},
		}},
	},
	"similar_056": {
		Name:                  "similar",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   Optional(Primitive(Ydb.Type_INT32)),
				"b":   Optional(Primitive(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(0), ptr.Int32(5), ptr.Int32(6)},
				"a":   []*int32{ptr.Int32(1), ptr.Int32(6), ptr.Int32(9)},
				"b":   []*string{ptr.String("hello"), ptr.String("three"), ptr.String("four")},
			},
		}},
	},
	"strcomp": {
		Name:                  "strcomp",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   Optional(Primitive(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(0), ptr.Int32(1), ptr.Int32(2)},
				"a":   []*string{ptr.String("abc__"), ptr.String("__abc"), ptr.String("__abc__")},
			},
		}},
	},
	"strcomp_0": {
		Name:                  "strcomp",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   Optional(Primitive(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(0)},
				"a":   []*string{ptr.String("abc__")},
			},
		}},
	},
	"strcomp_1": {
		Name:                  "strcomp",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id": testIdType,
				"a":   Optional(Primitive(Ydb.Type_UTF8)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(1)},
				"a":   []*string{ptr.String("__abc")},
			},
		}},
	},
	"tagged": {
		Name:                  "object_ids",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"objectid": common.MakeOptionalType(common.MakeTaggedType("ObjectId", objectIdType)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(0), ptr.Int32(1), ptr.Int32(2)},
				"objectid": []*[]byte{
					ptr.T([]byte(string("171e75500ecde1c75c59139e"))),
					ptr.T([]byte(string("271e75500ecde1c75c59139e"))),
					ptr.T([]byte(string("371e75500ecde1c75c59139e"))),
				},
			},
		}},
	},
	"tagged_0": {
		Name:                  "object_ids",
		IDArrayBuilderFactory: newInt32IDArrayBuilder(memPool),
		Schema: &test_utils.TableSchema{
			Columns: map[string]*Ydb.Type{
				"_id":      testIdType,
				"objectid": common.MakeOptionalType(common.MakeTaggedType("ObjectId", objectIdType)),
			},
		},
		Records: []*test_utils.Record[int32, *array.Int32Builder]{{
			Columns: map[string]any{
				"_id": []*int32{ptr.Int32(0)},
				"objectid": []*[]byte{
					ptr.T([]byte(string("171e75500ecde1c75c59139e"))),
				},
			},
		}},
	},
}

func newInt32IDArrayBuilder(pool memory.Allocator) func() *array.Int32Builder {
	return func() *array.Int32Builder {
		return array.NewInt32Builder(pool)
	}
}

func Optional(ydbType *Ydb.Type) *Ydb.Type {
	return common.MakeOptionalType(ydbType)
}

func Primitive(typeId Ydb.Type_PrimitiveTypeId) *Ydb.Type {
	return common.MakePrimitiveType(typeId)
}

func Tagged(tag string, ydbType *Ydb.Type) *Ydb.Type {
	return common.MakeTaggedType(tag, ydbType)
}
