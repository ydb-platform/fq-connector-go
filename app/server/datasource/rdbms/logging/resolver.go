package logging

import (
	"context"
	"fmt"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"go.uber.org/zap"
)

type resolveParams struct {
	ctx          context.Context
	logger       *zap.Logger
	folderId     string
	logGroupName string
	iamToken     string // optional, used for authorization into external APIs
}

type ydbSource struct {
	endpoint     *api_common.TGenericEndpoint
	databaseName string
	tableName    string
}

func (r *ydbSource) ToZapFields() []zap.Field {
	return []zap.Field{
		zap.String("host", r.endpoint.Host),
		zap.Uint32("port", r.endpoint.Port),
		zap.String("database_name", r.databaseName),
		zap.String("table_name", r.tableName),
	}
}

type resolveResponse struct {
	sources []*ydbSource
}

type Resolver interface {
	resolve(request *resolveParams) (*resolveResponse, error)
	Close() error
}

func NewResolver(cfg *config.TLoggingConfig) (Resolver, error) {
	switch cfg.GetResolving().(type) {
	case *config.TLoggingConfig_Static:
		return newResolverStatic(cfg.GetStatic()), nil
	case *config.TLoggingConfig_Dynamic:
		return newResolverDynamic(cfg)
	default:
		return nil, fmt.Errorf("unsupported resolver type: %T", cfg.GetResolving())
	}
}
