package bench

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server"
	"github.com/ydb-platform/fq-connector-go/common"
)

func Run(logger *zap.Logger, cfg *config.TBenchmarkConfig) error {
	if cfg.GetServerRemote() != nil {
		return fmt.Errorf("not ready to work with remote connector")
	}

	if cfg.GetServerLocal() == nil {
		return fmt.Errorf("you must provide local configuration for connector")
	}

}

func RunTestCase(
	logger *zap.Logger,
	cfg *config.TBenchmarkConfig,
	testCase *config.TBenchmarkTestCase,
) error {
	srv, err := server.NewEmbedded(
		server.WithLoggingConfig(&config.TLoggerConfig{
			LogLevel: config.ELogLevel_ERROR,
		}),
		server.WithPagingConfig(testCase.ServerParams.Paging),
	)

	if err != nil {
		return fmt.Errorf("new server embedded: %w", err)
	}

	srv.Start()
	defer srv.Stop()

	client := srv.ClientStreaming()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	describeTableResponse, err := client.DescribeTable(ctx, cfg.DataSourceInstance, nil, cfg.Table)
	if err != nil {
		return fmt.Errorf("describe table: %w", err)
	}

	if !common.IsSuccess(describeTableResponse.Error) {
		return fmt.Errorf("describe table: %w", common.NewSTDErrorFromAPIError(describeTableResponse.Error))
	}

	schema := describeTableResponse.Schema

	var errGroup errgroup.Group
	errGroup.Go(func() error {
		request := &api_service_protos.TListSplitsRequest{}

		return nil
	})

	return nil
}
