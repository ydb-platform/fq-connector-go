package server

import (
	"reflect"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

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

type withObjectIdYqlType struct {
	yqlType config.TMongoDbConfig_EObjectIdYqlType
}

func (o *withObjectIdYqlType) apply(cfg *config.TServerConfig) {
	cfg.Datasources.Mongodb.ObjectIdYqlType = o.yqlType
}

func WithObjectIdYQLType(yqlType config.TMongoDbConfig_EObjectIdYqlType) EmbeddedOption {
	return &withObjectIdYqlType{yqlType: yqlType}
}

type withPushdownConfig struct {
	pushdownConfig *config.TPushdownConfig
}

func (o *withPushdownConfig) apply(cfg *config.TServerConfig) {
	val := reflect.ValueOf(cfg.Datasources).Elem()

	// Loop through the each datasource config and set the Pushdown field to the new value
	for i := 0; i < val.NumField(); i++ {
		outerValue := val.Field(i)

		if outerValue.Type().Kind() == reflect.Pointer && outerValue.Elem().Type().Kind() == reflect.Struct {
			pushdownField := reflect.Indirect(outerValue).FieldByName("Pushdown")
			if pushdownField.IsValid() && pushdownField.CanSet() {
				pushdownField.Set(reflect.ValueOf(o.pushdownConfig))
			}
		}
	}
}

func WithPushdownConfig(cfg *config.TPushdownConfig) EmbeddedOption {
	return &withPushdownConfig{pushdownConfig: cfg}
}

type withPostgreSQLSplitting struct {
	splitting *config.TPostgreSQLConfig_TSplitting
}

func (o *withPostgreSQLSplitting) apply(cfg *config.TServerConfig) {
	cfg.Datasources.Postgresql.Splitting = o.splitting
}

func WithPostgreSQLSplitting(tablePhysicalSizeThresholdBytes uint64) EmbeddedOption {
	return &withPostgreSQLSplitting{
		splitting: &config.TPostgreSQLConfig_TSplitting{
			Enabled:                         true,
			TablePhysicalSizeThresholdBytes: tablePhysicalSizeThresholdBytes,
		},
	}
}

type withYDBTableMetadataCache struct {
	TableMetadataCache *config.TYdbConfig_TTableMetadataCache
}

func (o *withYDBTableMetadataCache) apply(cfg *config.TServerConfig) {
	cfg.Datasources.Ydb.TableMetadataCache = o.TableMetadataCache
}

func WithYDBTableMetadataCache(tableMetadataCache *config.TYdbConfig_TTableMetadataCache) EmbeddedOption {
	return &withYDBTableMetadataCache{TableMetadataCache: tableMetadataCache}
}
