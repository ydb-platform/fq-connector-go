package postgresql

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
)

var Tables = map[string]*datasource.Table{
	"simple": {
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "id",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col1",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
				{
					Name: "col2",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3), ptr.Int32(4), ptr.Int32(5)},
					[]*string{
						ptr.String("pg_a"),
						ptr.String("pg_b"),
						ptr.String("pg_c"),
						ptr.String("pg_d"),
						ptr.String("pg_e"),
					},
					[]*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30), ptr.Int32(40), ptr.Int32(50)},
				},
			},
		},
	},
	"primitives": {
		SchemaYdb: &api_service_protos.TSchema{
			Columns: []*Ydb.Column{
				{
					Name: "col_01_bool",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)),
				},
				{
					Name: "col_02_smallint",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				},
				{
					Name: "col_03_int2",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				},
				{
					Name: "col_04_smallserial",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				},
				{
					Name: "col_05_serial2",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT16)),
				},
				{
					Name: "col_06_integer",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_07_int",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_08_int4",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_09_serial",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_10_serial4",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
				},
				{
					Name: "col_11_bigint",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				},
				{
					Name: "col_12_int8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				},
				{
					Name: "col_13_bigserial",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				},
				{
					Name: "col_14_serial8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)),
				},
				{
					Name: "col_15_real",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				},
				{
					Name: "col_16_float4",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_FLOAT)),
				},
				{
					Name: "col_17_double_precision",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				},
				{
					Name: "col_18_float8",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)),
				},
				{
					Name: "col_19_bytea",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				},
				{
					Name: "col_20_character_n",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
				{
					Name: "col_21_character_varying_n",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)),
				},
				{
					Name: "col_22_text",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
				},
				{
					Name: "col_23_timestamp",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP)),
				},
				{
					Name: "col_24_date",
					Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DATE)),
				},
			},
		},
		Records: []*datasource.Record{
			{
				Columns: []any{
					[]*int32{ptr.Int32(1), ptr.Int32(2), ptr.Int32(3), ptr.Int32(4), ptr.Int32(5)},
					[]*string{
						ptr.String("pg_a"),
						ptr.String("pg_b"),
						ptr.String("pg_c"),
						ptr.String("pg_d"),
						ptr.String("pg_e"),
					},
					[]*int32{ptr.Int32(10), ptr.Int32(20), ptr.Int32(30), ptr.Int32(40), ptr.Int32(50)},
				},
			},
		},
	},
}
