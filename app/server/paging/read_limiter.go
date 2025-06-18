package paging

import (
	"fmt"

	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

// ReadLimiter helps to limitate amount of data returned by Connector server in every read request.
// This is generally should be avoided after https://st.yandex-team.ru/YQ-2057
type ReadLimiter interface {
	addRow() error
}

type readLimiterNoop struct {
}

func (readLimiterNoop) addRow() error { return nil }

type readLimiterRows struct {
	rowsRead  uint64
	rowsLimit uint64
}

func (rl *readLimiterRows) addRow() error {
	if rl.rowsRead >= rl.rowsLimit {
		return fmt.Errorf(
			"server can read only %d line(s) from the data source per single `ReadSplits` request "+
				"(this limitation may be disabled in future): %w",
			rl.rowsLimit,
			common.ErrReadLimitExceeded,
		)
	}

	rl.rowsRead++

	return nil
}

type ReadLimiterFactory struct {
	configs map[api_common.EGenericDataSourceKind]*config.TReadLimiterConfig
}

func (rlf *ReadLimiterFactory) MakeReadLimiter(logger *zap.Logger, kind api_common.EGenericDataSourceKind) ReadLimiter {
	if len(rlf.configs) == 0 {
		return readLimiterNoop{}
	}

	cfg, exists := rlf.configs[kind]
	if !exists {
		return readLimiterNoop{}
	}

	logger.Warn("the maximal number of rows read from the data source will be limited", zap.Uint64("rows", cfg.GetRows()))

	return &readLimiterRows{rowsRead: 0, rowsLimit: cfg.GetRows()}
}

func NewReadLimiterFactory(datasourcesCfg *config.TDatasourcesConfig) *ReadLimiterFactory {
	configs := make(map[api_common.EGenericDataSourceKind]*config.TReadLimiterConfig)

	// YQ-4362: enable limitations only for Logging
	if datasourcesCfg.GetLogging() != nil {
		configs[api_common.EGenericDataSourceKind_LOGGING] = datasourcesCfg.Logging.ReadLimiter
	}

	return &ReadLimiterFactory{configs: configs}
}
