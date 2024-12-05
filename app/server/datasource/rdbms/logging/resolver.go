package logging

import (
	"context"
	"fmt"
	"math/rand"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"go.uber.org/zap"
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

type resolver interface {
	resolve(request *resolveParams) (*resolveResponse, error)
}

type staticResolver struct {
	cfg *config.TLoggingConfig_TStaticResolving
}

func (r *staticResolver) resolve(
	request *resolveParams,
) (*resolveResponse, error) {
	// get random YDB endpoint from provided list
	ix := rand.Intn(len(r.cfg.Databases))
	endpoint := r.cfg.Databases[ix].Endpoint
	databaseName := r.cfg.Databases[ix].Name

	// get log_group_id from provided map
	logGroupId, exists := r.cfg.LogGroups[request.logGroupName]
	if !exists {
		return nil, fmt.Errorf("log group %s not found", request.logGroupName)
	}

	tableName := fmt.Sprintf("logs/origin/yc.logs.cloud/%s/%s", request.folderId, logGroupId)

	return &resolveResponse{
		endpoint:     endpoint,
		tableName:    tableName,
		databaseName: databaseName,
	}, nil
}

func newStaticResolver(cfg *config.TLoggingConfig_TStaticResolving) resolver {
	return &staticResolver{
		cfg: cfg,
	}
}

func newResolver(cfg *config.TLoggingConfig) (resolver, error) {
	if cfg.GetStatic() != nil {
		return newStaticResolver(cfg.GetStatic()), nil
	}

	return nil, fmt.Errorf("unsupported resolver type: %T", cfg.GetResolving())
}
