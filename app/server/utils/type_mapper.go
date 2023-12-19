package utils

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type TypeMapper interface {
	SQLTypeToYDBColumn(columnName, typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error)
}
