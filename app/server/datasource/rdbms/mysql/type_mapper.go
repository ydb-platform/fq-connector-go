package mysql

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.TypeMapper = typeMapper{}

type typeMapper struct{}

func (typeMapper) SQLTypeToYDBColumn(columnName, _ string, _ *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {
	return &Ydb.Column{
		Name: columnName,
		Type: common.MakePrimitiveType(Ydb.Type_INT32),
	}, nil
}

func NewTypeMapper() datasource.TypeMapper { return typeMapper{} }
