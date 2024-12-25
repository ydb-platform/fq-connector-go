package logging

import (
	"context"
	"fmt"
	"math/rand"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_logging "github.com/ydb-platform/fq-connector-go/api/logging/v1"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

type resolveParams struct {
	ctx          context.Context
	logger       *zap.Logger
	folderId     string
	logGroupName string
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

type staticResolver struct {
	cfg *config.TLoggingConfig_TStaticResolving
}

func (r *staticResolver) resolve(request *resolveParams) (*resolveResponse, error) {
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

	// FIXME: hardcoded cloud name is a mistake
	tableName := fmt.Sprintf("logs/origin/yc.logs.cloud/%s/%s", request.folderId, logGroupId)

	return &resolveResponse{
		sources: []*ydbSource{
			{
				endpoint:     endpoint,
				tableName:    tableName,
				databaseName: databaseName,
			},
		},
	}, nil
}

func (r *staticResolver) Close() error { return nil }

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

type dynamicResolver struct {
	client api_logging.LogGroupServiceClient
	conn   *grpc.ClientConn
}

func (r *dynamicResolver) resolve(
	request *resolveParams,
) (*resolveResponse, error) {
	return nil, nil
}

func (r *dynamicResolver) Close() error {
	return r.conn.Close()
}

func NewGRPCResolver(cfg *config.TLoggingConfig) (Resolver, error) {
	endpoint := common.EndpointToString(cfg.GetDynamic().LoggingEndpoint)

	grpcConn, err := grpc.Dial(endpoint)
	if err != nil {
		return nil, fmt.Errorf("GRPC dial: %w", err)
	}

	return &dynamicResolver{
		client: api_logging.NewLogGroupServiceClient(grpcConn),
		conn:   grpcConn,
	}, nil
}
