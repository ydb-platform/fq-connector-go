package mysql

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
)

var tables = map[string]*datasource.Table{
	"simple": {
		Name: "simple",
		Schema: &datasource.TableSchema{
			Columns: map[string]*Ydb.Type{
				"id":                  common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"tinyint_column":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT8)),
				"smallint_column":     common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				"mediumint_column":    common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"unsigned_int_column": common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT32)),
				"int_column":          common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				"varchar_column":      common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				"float_column":        common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				"double_column":       common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				"bool_column":         common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
			},
		},
		Records: []*datasource.Record{
			{
				Columns: map[string]any{
					"id": []*int32{
						ptr.Int32(1),
						ptr.Int32(2),
						ptr.Int32(3),
					},
					"tinyint_column": []*int8{
						ptr.Int8(-1),
						ptr.Int8(-2),
						ptr.Int8(-2),
					},
					"smallint_column": []*int16{
						ptr.Int16(2),
						nil,
						ptr.Int16(3),
					},
					"mediumint_column": []*int32{
						ptr.Int32(45),
						ptr.Int32(21),
						ptr.Int32(42),
					},
					"unsigned_int_column": []*uint32{
						ptr.Uint32(234),
						ptr.Uint32(532),
						ptr.Uint32(532),
					},
					"int_column": []*int32{
						ptr.Int32(-234),
						ptr.Int32(234),
						ptr.Int32(234),
					},
					"varchar_column": []*string{
						ptr.String("hello"),
						ptr.String("world"),
						ptr.String("!!!"),
					},
					"float_column": []*float32{
						ptr.Float32(4.24),
						ptr.Float32(-4.24),
						ptr.Float32(-1.23),
					},
					"double_column": []*float64{
						nil,
						ptr.Float64(-12.2),
						ptr.Float64(42.1),
					},
					"bool_column": []*uint8{
						ptr.Uint8(1),
						ptr.Uint8(0),
						ptr.Uint8(1),
					},
				},
			},
		},
	},
}
