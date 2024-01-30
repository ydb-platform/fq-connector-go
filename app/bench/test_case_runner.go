package bench

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server"
	"github.com/ydb-platform/fq-connector-go/common"
)

type testCaseRunner struct {
	cfg             *config.TBenchmarkConfig
	testCase        *config.TBenchmarkTestCase
	srv             *server.Embedded
	reportGenerator *reportGenerator
	ctx             context.Context
	cancel          context.CancelFunc
	logger          *zap.Logger
}

func newTestCaseRunner(
	logger *zap.Logger,
	cfg *config.TBenchmarkConfig,
	testCase *config.TBenchmarkTestCase,
) (*testCaseRunner, error) {
	srv, err := server.NewEmbedded(
		server.WithLoggingConfig(&config.TLoggerConfig{
			LogLevel: config.ELogLevel_ERROR,
		}),
		server.WithPagingConfig(testCase.ServerParams.Paging),
	)

	if err != nil {
		return nil, fmt.Errorf("new server embedded: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	tcr := &testCaseRunner{
		logger:          logger,
		cfg:             cfg,
		testCase:        testCase,
		srv:             srv,
		ctx:             ctx,
		cancel:          cancel,
		reportGenerator: newReportGenerator(logger),
	}

	return tcr, nil
}

func (tcr *testCaseRunner) run() error {
	tcr.srv.Start()
	tcr.reportGenerator.start()

	// get table schema
	describeTableResponse, err := tcr.srv.ClientStreaming().DescribeTable(
		tcr.ctx,
		tcr.cfg.DataSourceInstance,
		nil,
		tcr.cfg.Table,
	)

	if err != nil {
		return fmt.Errorf("describe table: %w", err)
	}

	if !common.IsSuccess(describeTableResponse.Error) {
		return fmt.Errorf("describe table: %w", common.NewSTDErrorFromAPIError(describeTableResponse.Error))
	}

	// launch split listing
	slct := &api_service_protos.TSelect{
		DataSourceInstance: tcr.cfg.DataSourceInstance,
		What:               common.SchemaToSelectWhatItems(describeTableResponse.Schema, nil),
		From: &api_service_protos.TSelect_TFrom{
			Table: tcr.cfg.Table,
		},
	}

	if err := tcr.listAndReadSplits(slct); err != nil {
		return fmt.Errorf("list and read splits: %w", common.NewSTDErrorFromAPIError(describeTableResponse.Error))
	}

	return nil
}

func (tcr *testCaseRunner) listAndReadSplits(slct *api_service_protos.TSelect) error {
	resultChan, err := tcr.srv.ClientStreaming().ListSplits(tcr.ctx, slct)
	if err != nil {
		return fmt.Errorf("list splits: %w", err)
	}

	for result := range resultChan {
		if result.Err != nil {
			return fmt.Errorf("list splits result: %w", result.Err)
		}

		if !common.IsSuccess(result.Response.Error) {
			return fmt.Errorf("list splits result: %w", common.NewSTDErrorFromAPIError(result.Response.Error))
		}

		// TODO: read data in the same thread now, but configure parallel reading later
		if err := tcr.readSplits(result.Response.Splits); err != nil {
			return fmt.Errorf("read splits: %w", err)
		}
	}

	return nil
}

func (tcr *testCaseRunner) readSplits(splits []*api_service_protos.TSplit) error {
	resultChan, err := tcr.srv.ClientStreaming().ReadSplits(tcr.ctx, splits)
	if err != nil {
		return fmt.Errorf("read splits: %w", err)
	}

	for result := range resultChan {
		if result.Err != nil {
			return fmt.Errorf("list splits result: %w", result.Err)
		}

		if !common.IsSuccess(result.Response.Error) {
			return fmt.Errorf("list splits result: %w", common.NewSTDErrorFromAPIError(result.Response.Error))
		}
	}

	return nil
}

func (tcr *testCaseRunner) finish() *report {
	tcr.cancel()                      // terminate requests
	tcr.srv.Stop()                    // terminate server
	return tcr.reportGenerator.stop() // obtain final report
}
