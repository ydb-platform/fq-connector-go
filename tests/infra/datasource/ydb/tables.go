package ydb

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
)

// key - test case name
// value - table description
var tables = map[string]*datasource.Table{
	"simple": {
		Name: "simple",
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakePrimitiveType(Ydb.Type_INT8),
				},
				{
					Name: "col1",
					Type: common.MakePrimitiveType(Ydb.Type_STRING),
				},
				{
					Name: "col2",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int8{1, 2, 3, 4, 5},
					[][]byte{
						[]byte("ydb_a"),
						[]byte("ydb_b"),
						[]byte("ydb_c"),
						[]byte("ydb_d"),
						[]byte("ydb_e"),
					},
					[]int32{10, 20, 30, 40, 50},
				},
			},
		},
	},
	"primitives": {
		Name: "primitives",
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakePrimitiveType(Ydb.Type_INT8),
				},
				{
					Name: "col_01_bool",
					Type: common.MakePrimitiveType(Ydb.Type_BOOL),
				},
				{
					Name: "col_02_int8",
					Type: common.MakePrimitiveType(Ydb.Type_INT8),
				},
				{
					Name: "col_03_int16",
					Type: common.MakePrimitiveType(Ydb.Type_INT16),
				},
				{
					Name: "col_04_int32",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col_05_int64",
					Type: common.MakePrimitiveType(Ydb.Type_INT64),
				},
				{
					Name: "col_06_uint8",
					Type: common.MakePrimitiveType(Ydb.Type_UINT8),
				},
				{
					Name: "col_07_uint16",
					Type: common.MakePrimitiveType(Ydb.Type_UINT16),
				},
				{
					Name: "col_08_uint32",
					Type: common.MakePrimitiveType(Ydb.Type_UINT32),
				},
				{
					Name: "col_09_uint64",
					Type: common.MakePrimitiveType(Ydb.Type_UINT64),
				},
				{
					Name: "col_10_float",
					Type: common.MakePrimitiveType(Ydb.Type_FLOAT),
				},
				{
					Name: "col_11_double",
					Type: common.MakePrimitiveType(Ydb.Type_DOUBLE),
				},
				{
					Name: "col_12_string",
					Type: common.MakePrimitiveType(Ydb.Type_STRING),
				},
				{
					Name: "col_13_utf8",
					Type: common.MakePrimitiveType(Ydb.Type_UTF8),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int8{1},
					[]uint8{0}, // []bool{false}
					[]int8{1},
					[]int16{-2},
					[]int32{3},
					[]int64{-4},
					[]uint8{5},
					[]uint16{6},
					[]uint32{7},
					[]uint64{8},
					[]float32{9.9},
					[]float64{-10.10},
					[][]byte{[]byte("ая")},
					[]string{"az"},
				},
			},
		},
	},
	"optionals": {
		Name: "optionals",
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakePrimitiveType(Ydb.Type_INT8),
				},
				{
					Name: "col_01_bool",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				},
				{
					Name: "col_02_int8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT8)),
				},
				{
					Name: "col_03_int16",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				},
				{
					Name: "col_04_int32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_05_int64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				},
				{
					Name: "col_06_uint8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT8)),
				},
				{
					Name: "col_07_uint16",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT16)),
				},
				{
					Name: "col_08_uint32",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT32)),
				},
				{
					Name: "col_09_uint64",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UINT64)),
				},
				{
					Name: "col_10_float",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				},
				{
					Name: "col_11_double",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				},
				{
					Name: "col_12_string",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				},
				{
					Name: "col_13_utf8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]int8{1, 2},
					[]*uint8{ptr.Uint8(1), nil}, // []*bool{true, nil}
					[]*int8{ptr.Int8(1), nil},
					[]*int16{ptr.Int16(-2), nil},
					[]*int32{ptr.Int32(3), nil},
					[]*int64{ptr.Int64(-4), nil},
					[]*uint8{ptr.Uint8(5), nil},
					[]*uint16{ptr.Uint16(6), nil},
					[]*uint32{ptr.Uint32(7), nil},
					[]*uint64{ptr.Uint64(8), nil},
					[]*float32{ptr.Float32(9.9), nil},
					[]*float64{ptr.Float64(-10.10), nil},
					[]*[]byte{ptr.T[[]byte]([]byte("ая")), nil},
					[]*string{ptr.String("az"), nil},
				},
			},
		},
	},
}
