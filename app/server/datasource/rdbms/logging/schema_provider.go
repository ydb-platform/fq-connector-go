package logging

import (
	"context"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"
)

const (
	levelColumnName     = "level"
	timestampColumnName = "timestamp"
	messageColumnName   = "message"
	metaColumnName      = "meta"
)

var loggingVirtualSchema = &api_service_protos.TSchema{
	Columns: []*Ydb.Column{
		{Name: levelColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING))},
		{Name: timestampColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_TIMESTAMP))},
		{Name: messageColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8))},
		{Name: metaColumnName, Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_JSON))},
	},
}

var _ rdbms_utils.SchemaProvider = (*schemaProviderImpl)(nil)

type schemaProviderImpl struct {
}

func (s *schemaProviderImpl) GetSchema(
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
