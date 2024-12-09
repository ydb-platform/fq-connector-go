package logging

import (
	"context"
	"fmt"
	"math/rand"

	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
)

type resolveParams struct {
	ctx          context.Context
	logger       *zap.Logger
	folderId     string
	logGroupName string
}

type resolveResponse struct {
	endpoint     *api_common.TEndpoint
	databaseName string
	tableName    string
}

func (r *resolveResponse) ToZapFields() []zap.Field {
	return []zap.Field{
		zap.String("host", r.endpoint.Host),
		zap.Uint32("port", r.endpoint.Port),
		zap.String("database_name", r.databaseName),
		zap.String("table_name", r.tableName),
	}
}

type Resolver interface {
	resolve(request *resolveParams) (*resolveResponse, error)
}

type staticResolver struct {
	cfg *config.TLoggingConfig_TStaticResolving
}

func (r *staticResolver) resolve(
	request *resolveParams,
) (*resolveResponse, error) {
	if len(r.cfg.Databases) == 0 {
		return nil, fmt.Errorf("no YDB endpoints provided")
	}

	// get random YDB endpoint from provided list
	ix := rand.Intn(len(r.cfg.Databases))

	endpoint := r.cfg.Databases[ix].Endpoint
	databaseName := r.cfg.Databases[ix].Name

	// pick a preconfigured folder
	folder, exists := r.cfg.Folders[request.folderId]
	if !exists {
		return nil, fmt.Errorf("folder_id '%s' is missing", request.folderId)
	}

	// resolve log group name into log group id
	logGroupId, exists := folder.LogGroups[request.logGroupName]
	if !exists {
		return nil, fmt.Errorf("log group '%s' is missing", request.logGroupName)
	}

	tableName := fmt.Sprintf("logs/origin/yc.logs.cloud/%s/%s", request.folderId, logGroupId)

	return &resolveResponse{
		endpoint:     endpoint,
		tableName:    tableName,
		databaseName: databaseName,
	}, nil
}

func newStaticResolver(cfg *config.TLoggingConfig_TStaticResolving) Resolver {
	return &staticResolver{
		cfg: cfg,
	}
}

func NewResolver(cfg *config.TLoggingConfig) (Resolver, error) {
	if cfg.GetStatic() != nil {
		return newStaticResolver(cfg.GetStatic()), nil
	}

	return nil, fmt.Errorf("unsupported resolver type: %T", cfg.GetResolving())
}
