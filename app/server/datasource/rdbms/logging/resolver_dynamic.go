package logging

import (
	"crypto/tls"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	api_logging "github.com/ydb-platform/fq-connector-go/api/logging/v1"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

type dynamicResolver struct {
	client api_logging.LogGroupServiceClient
	conn   *grpc.ClientConn
	cfg    *config.TLoggingConfig
}

func (r *dynamicResolver) resolve(
	request *resolveParams,
) (*resolveResponse, error) {
	if request.credentials.GetToken().GetValue() == "" {
		return nil, fmt.Errorf("IAM token is missing")
	}

	md := metadata.Pairs("authorization", fmt.Sprintf("Bearer %s", request.credentials.GetToken().GetValue()))
	ctx := metadata.NewOutgoingContext(request.ctx, md)

	response, err := r.client.GetReadingEndpoint(ctx, &api_logging.GetReadingEndpointRequest{
		FolderId:  request.folderId,
		GroupName: request.logGroupName,
	})

	if err != nil {
		return nil, fmt.Errorf("get reading endpoint: %w", err)
	}

	var sources []*ydbSource

LOOP:
	for _, table := range response.GetTables() {
		endpoint, err := common.StringToEndpoint(table.GetDbEndpoint())
		if err != nil {
			return nil, fmt.Errorf("string '%s' to endpoint: %w", table.GetDbEndpoint(), err)
		}

		// Use underlay network if necessary
		if r.cfg.Ydb.UseUnderlayNetworkForDedicatedDatabases {
			endpoint.Host = "u-" + endpoint.Host
		}

		// due to the troubles like KIKIMR-22852
		for _, blacklistedDbName := range r.cfg.GetDynamic().DatabaseBlacklist {
			if table.DbName == blacklistedDbName {
				request.logger.Warn("skipping blacklisted database", zap.String("database", table.DbName))

				continue LOOP
			}
		}

		sources = append(sources, &ydbSource{
			endpoint:     endpoint,
			databaseName: table.DbName,
			tableName:    table.TableName,
		})
	}

	return &resolveResponse{
		sources: sources,
	}, nil
}

func (r *dynamicResolver) Close() error {
	return r.conn.Close()
}

func newResolverDynamic(cfg *config.TLoggingConfig) (Resolver, error) {
	endpoint := common.EndpointToString(cfg.GetDynamic().LoggingEndpoint)

	tlsCfg := &tls.Config{}

	grpcConn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		return nil, fmt.Errorf("GRPC dial: %w", err)
	}

	return &dynamicResolver{
		client: api_logging.NewLogGroupServiceClient(grpcConn),
		conn:   grpcConn,
		cfg:    cfg,
	}, nil
}
