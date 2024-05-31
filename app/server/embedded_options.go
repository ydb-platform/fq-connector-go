package server

import "github.com/ydb-platform/fq-connector-go/app/config"

// EmbeddedOption parametrizes initialization of Connector server embedded into tests
type EmbeddedOption interface {
	apply(cfg *config.TServerConfig)
}

type withPagingConfig struct {
	pagingConfig *config.TPagingConfig
}

func (o *withPagingConfig) apply(cfg *config.TServerConfig) {
	cfg.Paging = o.pagingConfig
}

func WithPagingConfig(pagingConfig *config.TPagingConfig) EmbeddedOption {
	return &withPagingConfig{pagingConfig: pagingConfig}
}

type withLoggerConfig struct {
	loggerConfig *config.TLoggerConfig
}

func (o *withLoggerConfig) apply(cfg *config.TServerConfig) {
	cfg.Logger = o.loggerConfig
}

func WithLoggerConfig(loggerConfig *config.TLoggerConfig) EmbeddedOption {
	return &withLoggerConfig{loggerConfig: loggerConfig}
}

type withPprofServerConfig struct {
	pprofServerConfig *config.TPprofServerConfig
}

func (o *withPprofServerConfig) apply(cfg *config.TServerConfig) {
	cfg.PprofServer = o.pprofServerConfig
}

func WithPprofServerConfig(pprofServerConfig *config.TPprofServerConfig) EmbeddedOption {
	return &withPprofServerConfig{pprofServerConfig: pprofServerConfig}
}

type withConversionConfig struct {
	conversionConfig *config.TConversionConfig
}

func (o *withConversionConfig) apply(cfg *config.TServerConfig) {
	cfg.Conversion = o.conversionConfig
}

func WithConversionConfig(conversionConfig *config.TConversionConfig) EmbeddedOption {
	return &withConversionConfig{conversionConfig: conversionConfig}
}

type withMetricsServerConfig struct {
	metricsServerConfig *config.TMetricsServerConfig
}

func (o *withMetricsServerConfig) apply(cfg *config.TServerConfig) {
	cfg.MetricsServer = o.metricsServerConfig
}

func WithMetricsServerConfig(metricsServerConfig *config.TMetricsServerConfig) EmbeddedOption {
	return &withMetricsServerConfig{metricsServerConfig: metricsServerConfig}
}
