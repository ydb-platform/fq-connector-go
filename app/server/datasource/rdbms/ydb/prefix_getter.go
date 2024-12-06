package ydb

import (
	"context"
	"path"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

type PrefixGetter interface {
	GetPrefix(
		ctx context.Context,
		logger *zap.Logger,
		driver *ydb.Driver,
		request *api_service_protos.TDescribeTableRequest) (string, error)
}

type prefixGetterImpl struct{}

func (prefixGetterImpl) GetPrefix(
	_ context.Context,
	_ *zap.Logger,
	db *ydb.Driver,
	request *api_service_protos.TDescribeTableRequest,
) (string, error) {
	return path.Join(db.Name(), request.Table), nil
}

func NewPrefixGetter() PrefixGetter {
	return prefixGetterImpl{}
}
