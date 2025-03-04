package utils

import (
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

type Preset struct {
	Logger    *zap.Logger
	Cfg       *config.TClientConfig
	TableName string
}

func (p *Preset) Close() {
	if err := p.Logger.Sync(); err != nil {
		fmt.Println("failed to sync logger", err)
	}
}

func MakePreset(cmd *cobra.Command) (*Preset, error) {
	configPath, err := cmd.Flags().GetString(ConfigFlag)
	if err != nil {
		return nil, fmt.Errorf("get config flag: %v", err)
	}

	tableName, err := cmd.Flags().GetString(TableFlag)
	if err != nil {
		return nil, fmt.Errorf("get table flag: %v", err)
	}

	var cfg config.TClientConfig

	if err = common.NewConfigFromPrototextFile[*config.TClientConfig](configPath, &cfg); err != nil {
		return nil, fmt.Errorf("unknown instance: %w", err)
	}

	// override credentials if IAM-token provided
	common.MaybeInjectTokenToDataSourceInstance(cfg.DataSourceInstance)

	logger := common.AnnotateLoggerWithDataSourceInstance(common.NewDefaultLogger(), cfg.DataSourceInstance)

	return &Preset{
		Logger:    logger,
		Cfg:       &cfg,
		TableName: tableName,
	}, nil
}
