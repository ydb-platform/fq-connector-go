package logging

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
)

type Resolver interface {
	resolve(request *resolveRequest) (*resolveResponse, error)
	Close() error
}
type resolveRequest struct {
	ctx          context.Context
	logger       *zap.Logger
	folderId     string
	logGroupName string
	// optional, used for authorization into external APIs
	credentials *api_common.TGenericCredentials
}

type ydbSource struct {
	endpoint     *api_common.TGenericEndpoint
	databaseName string
	tableName    string
	credentials  *api_common.TGenericCredentials
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
