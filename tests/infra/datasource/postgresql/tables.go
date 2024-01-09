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
}
