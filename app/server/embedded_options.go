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

type withConnectionTimeouts struct {
	open string
	ping string
}

func (o *withConnectionTimeouts) apply(cfg *config.TServerConfig) {
	cfg.Datasources.Clickhouse.OpenConnectionTimeout = o.open
	cfg.Datasources.Clickhouse.PingConnectionTimeout = o.ping
	cfg.Datasources.Greenplum.OpenConnectionTimeout = o.open
	cfg.Datasources.Mysql.OpenConnectionTimeout = o.open
	cfg.Datasources.MsSqlServer.PingConnectionTimeout = o.ping
	cfg.Datasources.Oracle.OpenConnectionTimeout = o.open
	cfg.Datasources.Oracle.PingConnectionTimeout = o.ping
	cfg.Datasources.Postgresql.OpenConnectionTimeout = o.open
	cfg.Datasources.Ydb.OpenConnectionTimeout = o.open
	cfg.Datasources.Ydb.PingConnectionTimeout = o.ping
}

func WithConnectionTimeouts(open, ping string) EmbeddedOption {
	return &withConnectionTimeouts{
		open: open, ping: ping,
	}
}

type withYdbConnectorMode struct {
	mode config.TYdbConfig_Mode
}

func (o *withYdbConnectorMode) apply(cfg *config.TServerConfig) {
	cfg.Datasources.Ydb.Mode = o.mode
}

func WithYdbConnectorMode(mode config.TYdbConfig_Mode) EmbeddedOption {
	return &withYdbConnectorMode{mode: mode}
}
