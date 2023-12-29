package client

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/prototext"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
)

const (
	tableName    = "primitives"
	outputFormat = api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING
)

func newConfigFromPath(configPath string) (*config.TClientConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read file %v: %w", configPath, err)
	}

	var cfg config.TClientConfig

	if err := prototext.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("prototext unmarshal `%v`: %w", string(data), err)
	}

	return &cfg, nil
}

func runClient(_ *cobra.Command, args []string) error {
	configPath := args[0]

	cfg, err := newConfigFromPath(configPath)
	if err != nil {
		return fmt.Errorf("unknown instance: %w", err)
	}

	logger := common.NewDefaultLogger()

	if err := callServer(logger, cfg); err != nil {
		return fmt.Errorf("call server: %w", err)
	}

	return nil
}

func callServer(logger *zap.Logger, cfg *config.TClientConfig) error {
	cl, err := NewClientFromClientConfig(logger, cfg)
	if err != nil {
		return fmt.Errorf("grpc dial: %w", err)
	}

	defer cl.Close()

	var splits []*api_service_protos.TSplit

	switch cfg.DataSourceInstance.Kind {
	case api_common.EDataSourceKind_CLICKHOUSE, api_common.EDataSourceKind_POSTGRESQL, api_common.EDataSourceKind_YDB:
		splits, err = prepareSplits(cl, tableName, cfg.DataSourceInstance)

		if err != nil {
			return fmt.Errorf("prepare splits: %w", err)
		}
	default:
		return fmt.Errorf("unexpected data source kind %v", cfg.DataSourceInstance.Kind)
	}

	// ReadSplits
	if err := readSplits(logger, cl, splits); err != nil {
		return fmt.Errorf("read splits: %w", err)
	}

	return nil
}

func prepareSplits(
	cl Client,
	tableName string,
	dsi *api_common.TDataSourceInstance,
) ([]*api_service_protos.TSplit, error) {
	// DescribeTable
	describeTableResponse, err := cl.DescribeTable(context.TODO(), dsi, tableName)
	if err != nil {
		return nil, fmt.Errorf("describe table: %w", err)
	}

	if !common.IsSuccess(describeTableResponse.Error) {
		return nil, fmt.Errorf("describe table: %v", describeTableResponse.Error)
	}

	// ListSplits - we want to SELECT *
	slct := &api_service_protos.TSelect{
		DataSourceInstance: dsi,
		What:               common.SchemaToSelectWhatItems(describeTableResponse.Schema, nil),
		From: &api_service_protos.TSelect_TFrom{
			Table: tableName,
		},
	}

	listSplitsResponse, err := cl.ListSplits(context.TODO(), slct)
	if err != nil {
		return nil, fmt.Errorf("list splits: %w", err)
	}

	return common.ListSplitsResponsesToSplits(listSplitsResponse), nil
}

func readSplits(
	logger *zap.Logger,
	cl Client,
	splits []*api_service_protos.TSplit,
) error {
	readSplitsResponses, err := cl.ReadSplits(context.Background(), splits)
	if err != nil {
		return fmt.Errorf("read splits: %w", err)
	}

	if err := dumpReadResponses(logger, readSplitsResponses); err != nil {
		return fmt.Errorf("dump read responses: %w", err)
	}

	return nil
}

func dumpReadResponses(
	logger *zap.Logger,
	responses []*api_service_protos.TReadSplitsResponse,
) error {
	for _, resp := range responses {
		buf := bytes.NewBuffer(resp.GetArrowIpcStreaming())

		reader, err := ipc.NewReader(buf)
		if err != nil {
			return fmt.Errorf("new reader: %w", err)
		}

		for reader.Next() {
			record := reader.Record()
			logger.Debug("schema", zap.String("schema", record.Schema().String()))

			for i, column := range record.Columns() {
				logger.Debug("column", zap.Int("id", i), zap.String("data", column.String()))
			}
		}

		reader.Release()
	}

	return nil
}

var Cmd = &cobra.Command{
	Use:   "client",
	Short: "client for Connector testing and debugging purposes",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runClient(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}
