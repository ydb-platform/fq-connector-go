package bench

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/time/rate"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server"
	"github.com/ydb-platform/fq-connector-go/common"
)

type testCaseRunner struct {
	cfg             *config.TBenchmarkConfig
	testCase        *config.TBenchmarkTestCase
	srv             common.TestingServer
	reportGenerator *reportGenerator
	ctx             context.Context
	logger          *zap.Logger
}

func newTestCaseRunner(
	ctx context.Context,
	logger *zap.Logger,
	cfg *config.TBenchmarkConfig,
	testCase *config.TBenchmarkTestCase,
) (*testCaseRunner, error) {
	if testCase.ServerParams != nil && cfg.GetServerLocal() == nil {
		return nil, errors.New("you can specify server params in test case only if local server is deployed")
	}

	var (
		err           error
		testingServer common.TestingServer
	)

	if local := cfg.GetServerLocal(); local != nil {
		testingServer, err = newTestingServerLocal(testCase.ServerParams)
	} else if remote := cfg.GetServerRemote(); remote != nil {
		testingServer, err = newTestingServerRemote(logger, remote)
	}

	if err != nil {
		return nil, fmt.Errorf("new testing server: %w", err)
	}

	tcr := &testCaseRunner{
		logger:          logger,
		cfg:             cfg,
		testCase:        testCase,
		srv:             testingServer,
		ctx:             ctx,
		reportGenerator: newReportGenerator(logger, testCase),
	}

	return tcr, nil
}

func newTestingServerLocal(serverParams *config.TBenchmarkServerParams) (common.TestingServer, error) {
	return server.NewEmbedded(
		server.WithLoggerConfig(&config.TLoggerConfig{
			LogLevel: config.ELogLevel_ERROR,
		}),
		server.WithPagingConfig(serverParams.Paging),
		server.WithPprofServerConfig(&config.TPprofServerConfig{
			Endpoint: &api_common.TEndpoint{Host: "localhost", Port: 50052},
		}),
		server.WithConversionConfig(
			&config.TConversionConfig{
				UseUnsafeConverters: true,
			},
		),
	)
}

func newTestingServerRemote(logger *zap.Logger, clientCfg *config.TClientConfig) (common.TestingServer, error) {
	return common.NewTestingServerRemote(logger, clientCfg)
}

func (tcr *testCaseRunner) run() error {
	tcr.srv.Start()
	tcr.reportGenerator.start()

	if params := tcr.testCase.ClientParams; params == nil {
		// if we need to execute requests only once, just do it
		if err := tcr.executeScenario(); err != nil {
			return fmt.Errorf("execute scenario: %w", err)
		}
	} else {
		// if we need to repeate request in a loop, rate limiter is a good idea
		ctx, cancel := context.WithTimeout(context.Background(), common.MustDurationFromString(params.Duration))
		defer cancel()

		limiter := rate.NewLimiter(rate.Limit(params.QueriesPerSecond), 1)
		counter := 0

		tcr.logger.Info("load session started")

		for {
			if err := limiter.Wait(ctx); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					tcr.logger.Info("load session finished")
					return nil
				}

				return fmt.Errorf("limiter wait: %w", err)
			}

			counter++

			tcr.logger.Debug("scenario started", zap.Int("id", counter))

			if err := tcr.executeScenario(); err != nil {
				return fmt.Errorf("execute scenario: %w", err)
			}

			tcr.logger.Debug("scenario finished", zap.Int("id", counter))
		}
	}

	return nil
}

func (tcr *testCaseRunner) executeScenario() error {
	// get table schema
	describeTableResponse, err := tcr.srv.ClientStreaming().DescribeTable(
		tcr.ctx,
		tcr.cfg.DataSourceInstance,
		&api_service_protos.TTypeMappingSettings{
			DateTimeFormat: api_service_protos.EDateTimeFormat_STRING_FORMAT,
		},
		tcr.cfg.Table,
	)

	if err != nil {
		return fmt.Errorf("describe table: %w", err)
	}

	if !common.IsSuccess(describeTableResponse.Error) {
		return fmt.Errorf("describe table: %w", common.NewSTDErrorFromAPIError(describeTableResponse.Error))
	}

	// launch split listing and reading
	slct := &api_service_protos.TSelect{
		DataSourceInstance: tcr.cfg.DataSourceInstance,
		What:               common.SchemaToSelectWhatItems(describeTableResponse.Schema, tcr.makeColumnWhitelist()),
		From: &api_service_protos.TSelect_TFrom{
			Table: tcr.cfg.Table,
		},
	}

	if err := tcr.listAndReadSplits(slct); err != nil {
		return fmt.Errorf("list and read splits: %w", err)
	}

	return nil
}

func (tcr *testCaseRunner) makeColumnWhitelist() map[string]struct{} {
	// if list is empty, read all columns
	if len(tcr.testCase.Columns) == 0 {
		return nil
	}

	out := make(map[string]struct{}, len(tcr.testCase.Columns))
	for _, col := range tcr.testCase.Columns {
		out[col] = struct{}{}
	}

	return out
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

	for {
		result, ok := <-resultChan
		if !ok {
			break
		}

		if result.Err != nil {
			return fmt.Errorf("list splits result: %w", result.Err)
		}

		if !common.IsSuccess(result.Response.Error) {
			return fmt.Errorf("list splits result: %w", common.NewSTDErrorFromAPIError(result.Response.Error))
		}

		tcr.reportGenerator.registerResponse(result.Response)
	}

	return nil
}

func (tcr *testCaseRunner) finish() *report {
	tcr.srv.Stop()                    // terminate server
	return tcr.reportGenerator.stop() // obtain final report
}

func (tcr *testCaseRunner) name() string {
	switch tcr.cfg.Server.(type) {
	case *config.TBenchmarkConfig_ServerLocal:
		return fmt.Sprintf(
			"bytes_per_page_%d-prefetch_queue_capacity_%d-columns_%d",
			tcr.testCase.ServerParams.Paging.BytesPerPage,
			tcr.testCase.ServerParams.Paging.PrefetchQueueCapacity,
			len(tcr.testCase.Columns),
		)
	case *config.TBenchmarkConfig_ServerRemote:
		return "remote"
	default:
		panic("unexpected type")
	}
}
