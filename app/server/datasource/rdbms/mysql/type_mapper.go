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

// func transformerFromTypeNames(typeNames []string, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
// 	acceptors := make([]any, 0, len(typeNames))
// 	appenders := make([]func(acceptor any, builder array.Builder))
//
// 	for i, name := range typeNames {
// 		switch name {
// 		default:
// 			acceptors = append(acceptors, "int")
// 			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
// 				cast := acceptor.(*mysql.)
// 			})
// 		}
// 	}
// }
