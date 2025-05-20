package logging

import (
	"context"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

var loggingVirtualSchema = &api_service_protos.TSchema{
	Columns: []*Ydb.Column{
		{Name: clusterColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
		{Name: hostnameColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
		{Name: jsonPayloadColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_JSON))},
		{Name: labelsColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_JSON))},
		{Name: levelColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
		{Name: messageColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
		{Name: metaColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_JSON))},
		{Name: projectColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
		{Name: serviceColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
		{Name: spanIDColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
		{Name: timestampColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP))},
		{Name: traceIDColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
	},
}

var _ rdbms_utils.SchemaProvider = (*schemaProviderImpl)(nil)

type schemaProviderImpl struct {
}

func (schemaProviderImpl) GetSchema(
	_ context.Context,
	_ *zap.Logger,
	_ rdbms_utils.Connection,
	_ *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TSchema, error) {
	return loggingVirtualSchema, nil
}

func NewSchemaProvider() rdbms_utils.SchemaProvider {
	return &schemaProviderImpl{}
}
