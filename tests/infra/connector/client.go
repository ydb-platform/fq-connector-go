package connector

import (
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
)

type clientImpl struct {
	api_service.ConnectorClient
	conn   *grpc.ClientConn
	logger *zap.Logger
}

func (cl *clientImpl) stop() {
	utils.LogCloserError(cl.logger, cl.conn, "client GRPC connection")
}

func newClient(logger *zap.Logger, cfg *config.TServerConfig) (*clientImpl, error) {
	conn, err := grpc.Dial(
		utils.EndpointToString(cfg.ConnectorServer.Endpoint),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}

	grpcClient := api_service.NewConnectorClient(conn)

	return &clientImpl{ConnectorClient: grpcClient, conn: conn, logger: logger}, nil
}
