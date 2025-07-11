package config

import (
	"errors"
	"fmt"
	"math"
	"os"

	"google.golang.org/protobuf/encoding/prototext"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

func makeDefaultExponentialBackoffConfig() *config.TExponentialBackoffConfig {
	return &config.TExponentialBackoffConfig{
		InitialInterval:     "500ms",
		RandomizationFactor: 0.25,
		Multiplier:          1.5,
		MaxInterval:         "10s",
		MaxElapsedTime:      "1m",
	}
}

func makeDefaultPushdownConfig() *config.TPushdownConfig {
	return &config.TPushdownConfig{
		EnableTimestampPushdown: false,
	}
}

// TODO: use reflection to generalize datasource setting code
//
//nolint:gocyclo,funlen
func fillServerConfigDefaults(c *config.TServerConfig) {
	if c.ConnectorServer.MaxRecvMessageSize == 0 {
		c.ConnectorServer.MaxRecvMessageSize = math.MaxInt32
	}

	if c.Paging == nil {
		c.Paging = &config.TPagingConfig{
			BytesPerPage:          4 * 1024 * 1024,
			PrefetchQueueCapacity: 2,
		}
	}

	if c.Logger == nil {
		c.Logger = &config.TLoggerConfig{
			LogLevel:              config.ELogLevel_INFO,
			EnableSqlQueryLogging: false,
		}
	}

	if c.Conversion == nil {
		c.Conversion = &config.TConversionConfig{
			UseUnsafeConverters: false,
		}
	}

	if c.Datasources == nil {
		c.Datasources = &config.TDatasourcesConfig{}
	}

	// ClickHouse

	if c.Datasources.Clickhouse == nil {
		c.Datasources.Clickhouse = &config.TClickHouseConfig{
			OpenConnectionTimeout: "5s",
			PingConnectionTimeout: "5s",
		}
	}

	if c.Datasources.Clickhouse.ExponentialBackoff == nil {
		c.Datasources.Clickhouse.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
	}

	if c.Datasources.Clickhouse.Pushdown == nil {
		c.Datasources.Clickhouse.Pushdown = makeDefaultPushdownConfig()
	}

	// Greenplum

	if c.Datasources.Greenplum == nil {
		c.Datasources.Greenplum = &config.TGreenplumConfig{
			OpenConnectionTimeout: "5s",
		}
	}

	if c.Datasources.Greenplum.ExponentialBackoff == nil {
		c.Datasources.Greenplum.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
	}

	if c.Datasources.Greenplum.Pushdown == nil {
		c.Datasources.Greenplum.Pushdown = makeDefaultPushdownConfig()
	}

	// MS SQL Server

	if c.Datasources.MsSqlServer == nil {
		c.Datasources.MsSqlServer = &config.TMsSQLServerConfig{
			OpenConnectionTimeout: "5s",
			PingConnectionTimeout: "5s",
		}
	}

	if c.Datasources.MsSqlServer.ExponentialBackoff == nil {
		c.Datasources.MsSqlServer.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
	}

	if c.Datasources.MsSqlServer.Pushdown == nil {
		c.Datasources.MsSqlServer.Pushdown = makeDefaultPushdownConfig()
	}

	// MySQL

	if c.Datasources.Mysql == nil {
		c.Datasources.Mysql = &config.TMySQLConfig{
			ResultChanCapacity:    512,
			OpenConnectionTimeout: "5s",
		}
	}

	if c.Datasources.Mysql.ExponentialBackoff == nil {
		c.Datasources.Mysql.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
	}

	if c.Datasources.Mysql.Pushdown == nil {
		c.Datasources.Mysql.Pushdown = makeDefaultPushdownConfig()
	}

	// Oracle

	if c.Datasources.Oracle == nil {
		c.Datasources.Oracle = &config.TOracleConfig{
			OpenConnectionTimeout: "5s",
			PingConnectionTimeout: "5s",
		}
	}

	if c.Datasources.Oracle.ExponentialBackoff == nil {
		c.Datasources.Oracle.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
	}

	if c.Datasources.Oracle.Pushdown == nil {
		c.Datasources.Oracle.Pushdown = makeDefaultPushdownConfig()
	}

	// MongoDB

	if c.Datasources.Mongodb == nil {
		c.Datasources.Mongodb = &config.TMongoDbConfig{
			OpenConnectionTimeout:   "5s",
			PingConnectionTimeout:   "5s",
			CountDocsToDeduceSchema: 5,
			ObjectIdYqlType:         config.TMongoDbConfig_OBJECT_ID_AS_STRING,
		}
	}

	if c.Datasources.Mongodb.ExponentialBackoff == nil {
		c.Datasources.Mongodb.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
	}

	// Redis

	if c.Datasources.Redis == nil {
		c.Datasources.Redis = &config.TRedisConfig{
			PingConnectionTimeout:   "5s",
			CountDocsToDeduceSchema: 5,
		}
	}

	if c.Datasources.Redis.ExponentialBackoff == nil {
		c.Datasources.Redis.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
	}

	// OpenSearch

	if c.Datasources.Opensearch == nil {
		c.Datasources.Opensearch = &config.TOpenSearchConfig{
			DialTimeout:           "5s",
			ResponseHeaderTimeout: "5s",
			PingConnectionTimeout: "5s",
			ScrollTimeout:         "10s",
			BatchSize:             100,
		}
	}

	if c.Datasources.Opensearch.ExponentialBackoff == nil {
		c.Datasources.Opensearch.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
	}

	// Prometheus

	if c.Datasources.Prometheus == nil {
		c.Datasources.Prometheus = &config.TPrometheusConfig{
			OpenConnectionTimeout: "5s",
			ChunkedReadLimitBytes: 5e7,
		}
	}

	if c.Datasources.Prometheus.ExponentialBackoff == nil {
		c.Datasources.Prometheus.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
	}

	// PostgreSQL

	if c.Datasources.Postgresql == nil {
		c.Datasources.Postgresql = &config.TPostgreSQLConfig{
			OpenConnectionTimeout: "5s",
		}
	}

	if c.Datasources.Postgresql.ExponentialBackoff == nil {
		c.Datasources.Postgresql.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
	}

	if c.Datasources.Postgresql.Pushdown == nil {
		c.Datasources.Postgresql.Pushdown = makeDefaultPushdownConfig()
	}

	if c.Datasources.Postgresql.Splitting == nil {
		c.Datasources.Postgresql.Splitting = &config.TPostgreSQLConfig_TSplitting{
			Enabled: false,
		}
	}

	// YDB

	if c.Datasources.Ydb == nil {
		c.Datasources.Ydb = &config.TYdbConfig{}
	}

	fillYdbConfigDefaults(c.Datasources.Ydb)

	// Logging

	if c.Datasources.Logging == nil {
		c.Datasources.Logging = &config.TLoggingConfig{
			Ydb: &config.TYdbConfig{},
			Resolving: &config.TLoggingConfig_Static{
				Static: &config.TLoggingConfig_TStaticResolving{},
			},
		}
	}

	fillYdbConfigDefaults(c.Datasources.Logging.Ydb)
}

//nolint:gocyclo
func fillYdbConfigDefaults(c *config.TYdbConfig) {
	if c.OpenConnectionTimeout == "" {
		c.OpenConnectionTimeout = "5s"
	}

	if c.PingConnectionTimeout == "" {
		c.PingConnectionTimeout = "5s"
	}

	if c.Mode == config.TYdbConfig_MODE_UNSPECIFIED {
		c.Mode = config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE
	}

	if c.ExponentialBackoff == nil {
		c.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
	}

	if c.Pushdown == nil {
		c.Pushdown = makeDefaultPushdownConfig()
	}

	if c.ServiceAccountKeyFileCredentials != "" {
		if c.IamEndpoint == nil {
			c.IamEndpoint = &api_common.TGenericEndpoint{
				Host: "iam.api.cloud.yandex.net",
				Port: 443,
			}
		}
	}

	if c.Splitting == nil {
		c.Splitting = &config.TYdbConfig_TSplitting{
			EnabledOnColumnShards: false,
		}
	}

	if c.Splitting.QueryTabletIdsTimeout == "" {
		c.Splitting.QueryTabletIdsTimeout = "1m"
	}

	if c.Mode == config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE && c.ResourcePool == "" {
		c.ResourcePool = "default"
	}

	if c.TableMetadataCache != nil && c.TableMetadataCache.GetRistretto() != nil {
		ristretto := c.TableMetadataCache.GetRistretto()

		if ristretto.MaxKeys == 0 {
			ristretto.MaxKeys = 10000
		}

		if ristretto.MaxSizeBytes == 0 {
			ristretto.MaxSizeBytes = 64 * 1024 * 1024
		}
	}
}

func validateServerConfig(c *config.TServerConfig) error {
	if err := validateConnectorServerConfig(c.ConnectorServer); err != nil {
		return fmt.Errorf("validate `connector_server`: %w", err)
	}

	if err := validatePprofServerConfig(c.PprofServer); err != nil {
		return fmt.Errorf("validate `pprof_server`: %w", err)
	}

	if err := validatePagingConfig(c.Paging); err != nil {
		return fmt.Errorf("validate `paging`: %w", err)
	}

	if err := validateConversionConfig(c.Conversion); err != nil {
		return fmt.Errorf("validate `conversion`: %w", err)
	}

	if err := validateDatasourcesConfig(c.Datasources); err != nil {
		return fmt.Errorf("validate `datasources`: %w", err)
	}

	if err := validateObservationConfig(c.Observation); err != nil {
		return fmt.Errorf("validate `observation`: %w", err)
	}

	return nil
}

func validateConnectorServerConfig(c *config.TConnectorServerConfig) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	if err := validateEndpoint(c.Endpoint); err != nil {
		return fmt.Errorf("validate `endpoint`: %w", err)
	}

	if err := validateServerTLSConfig(c.Tls); err != nil {
		return fmt.Errorf("validate `tls`: %w", err)
	}

	return nil
}

func validateEndpoint(c *api_common.TGenericEndpoint) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	if c.Host == "" {
		return fmt.Errorf("invalid value of field `host`: %v", c.Host)
	}

	if c.Port == 0 || c.Port > math.MaxUint16 {
		return fmt.Errorf("invalid value of field `port`: %v", c.Port)
	}

	return nil
}

func validateServerTLSConfig(c *config.TServerTLSConfig) error {
	if c == nil {
		// It's OK not to have TLS config section
		return nil
	}

	if c.Key == "" {
		return fmt.Errorf("invalid value of field `key`: %v", c.Key)
	}

	if c.Cert == "" {
		return fmt.Errorf("invalid value of field `cert`: %v", c.Cert)
	}

	return nil
}

func validateReadLimiterConfig(c *config.TReadLimiterConfig) error {
	if c == nil {
		// It's OK not to have read request memory limitation
		return nil
	}

	// but if it's not nil, one must set limits explicitly
	if c.GetRows() == 0 {
		return fmt.Errorf("invalid value of field `rows`")
	}

	return nil
}

func validatePprofServerConfig(c *config.TPprofServerConfig) error {
	if c == nil {
		// It's OK to disable profiler
		return nil
	}

	if err := validateEndpoint(c.Endpoint); err != nil {
		return fmt.Errorf("validate `endpoint`: %w", err)
	}

	if err := validateServerTLSConfig(c.Tls); err != nil {
		return fmt.Errorf("validate `tls`: %w", err)
	}

	return nil
}

const maxInterconnectMessageSize = 50 * 1024 * 1024

func validatePagingConfig(c *config.TPagingConfig) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	limitIsSet := c.BytesPerPage != 0 || c.RowsPerPage != 0
	if !limitIsSet {
		return fmt.Errorf("you must set either `bytes_per_page` or `rows_per_page` or both of them")
	}

	if c.BytesPerPage > maxInterconnectMessageSize {
		return fmt.Errorf("`bytes_per_page` limit exceeds the limits of interconnect system used by YDB engine")
	}

	return nil
}

func validateConversionConfig(c *config.TConversionConfig) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	return nil
}

func validateDatasourcesConfig(c *config.TDatasourcesConfig) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	if err := validateRelationalDatasourceConfig(c.Clickhouse); err != nil {
		return fmt.Errorf("validate `clickhouse`: %w", err)
	}

	if err := validateRelationalDatasourceConfig(c.Greenplum); err != nil {
		return fmt.Errorf("validate `greenplum`: %w", err)
	}

	if err := validateLoggingConfig(c.Logging); err != nil {
		return fmt.Errorf("validate `logging`: %w", err)
	}

	if err := validateRelationalDatasourceConfig(c.MsSqlServer); err != nil {
		return fmt.Errorf("validate `ms_sql_server`: %w", err)
	}

	if err := validateRelationalDatasourceConfig(c.Mysql); err != nil {
		return fmt.Errorf("validate `mysql`: %w", err)
	}

	if err := validateRelationalDatasourceConfig(c.Oracle); err != nil {
		return fmt.Errorf("validate `oracle`: %w", err)
	}

	if err := validatePostgreSQLConfig(c.Postgresql); err != nil {
		return fmt.Errorf("validate `postgresql`: %w", err)
	}

	if err := validateYdbConfig(c.Ydb); err != nil {
		return fmt.Errorf("validate `ydb`: %w", err)
	}

	if err := validateMongoDBConfig(c.Mongodb); err != nil {
		return fmt.Errorf("validate `mongodb`: %w", err)
	}

	if err := validateOpenSearchConfig(c.Opensearch); err != nil {
		return fmt.Errorf("validate `opensearch`: %w", err)
	}

	if err := validateRedisConfig(c.Redis); err != nil {
		return fmt.Errorf("validate `redis`: %w", err)
	}

	return nil
}

type relationalDatasourceConfig interface {
	GetOpenConnectionTimeout() string
	GetExponentialBackoff() *config.TExponentialBackoffConfig
	GetPushdown() *config.TPushdownConfig
}

func validateRelationalDatasourceConfig(c relationalDatasourceConfig) error {
	if c == nil {
		return nil
	}

	if _, err := common.DurationFromString(c.GetOpenConnectionTimeout()); err != nil {
		return fmt.Errorf("validate `open_connection_timeout`: %v", err)
	}

	if c.GetExponentialBackoff() == nil {
		return errors.New("missing `exponential_backoff`")
	}

	if c.GetPushdown() == nil {
		return errors.New("missing `pushdown`")
	}

	return nil
}

func validatePostgreSQLConfig(c *config.TPostgreSQLConfig) error {
	if err := validateRelationalDatasourceConfig(c); err != nil {
		return fmt.Errorf("validate relational datasource config: %w", err)
	}

	if c.Splitting == nil {
		return errors.New("missing `splitting`")
	}

	return nil
}

//nolint:gocyclo
func validateYdbConfig(c *config.TYdbConfig) error {
	if c == nil {
		return nil
	}

	if _, err := common.DurationFromString(c.OpenConnectionTimeout); err != nil {
		return fmt.Errorf("validate `open_connection_timeout`: %v", err)
	}

	if _, err := common.DurationFromString(c.PingConnectionTimeout); err != nil {
		return fmt.Errorf("validate `ping_connection_timeout`: %v", err)
	}

	switch c.Mode {
	case config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE, config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE_ARROW:
		// if c.ResourcePool == "" {
		// 	return fmt.Errorf("you must set `resource_pool` if `mode` is `query_service_native` or `query_service_native_arrow`")
		// }
	case config.TYdbConfig_MODE_TABLE_SERVICE_STDLIB_SCAN_QUERIES:
		if c.ResourcePool != "" {
			return fmt.Errorf("you must not set `resource_pool` if `mode` is `table_service_stdlib_scan_queries`")
		}
	default:
		return fmt.Errorf("invalid `mode` value: %v", c.Mode)
	}

	if c.ServiceAccountKeyFileCredentials != "" {
		if c.IamEndpoint == nil {
			return fmt.Errorf("you must set `iam_endpoint` if `service_account_key_file_credentials` is set")
		}

		if c.IamEndpoint.Host == "" {
			return fmt.Errorf("invalid value of field `iam_endpoint.host`: %v", c.IamEndpoint.Host)
		}

		if c.IamEndpoint.Port == 0 {
			return fmt.Errorf("invalid value of field `iam_endpoint.port`: %v", c.IamEndpoint.Port)
		}
	}

	if c.Splitting == nil {
		return fmt.Errorf("you must set `splitting` section")
	}

	if _, err := common.DurationFromString(c.Splitting.QueryTabletIdsTimeout); err != nil {
		return fmt.Errorf("validate `query_tablet_ids_timeout`: %v", err)
	}

	// it's OK not to have this cache, but if it's provided, validate it
	if cacheCfg := c.TableMetadataCache; cacheCfg != nil {
		if _, err := common.DurationFromString(cacheCfg.Ttl); err != nil {
			return fmt.Errorf("validate `ttl`: %v", err)
		}

		switch storageCfg := cacheCfg.Storage.(type) {
		case *config.TYdbConfig_TTableMetadataCache_Ristretto:
			if storageCfg.Ristretto.MaxSizeBytes <= 0 {
				return fmt.Errorf("invalid `max_size_bytes` value: %v", storageCfg.Ristretto.MaxSizeBytes)
			}

			if storageCfg.Ristretto.MaxKeys <= 0 {
				return fmt.Errorf("invalid `max_keys` value: %v", storageCfg.Ristretto.MaxKeys)
			}
		default:
			return fmt.Errorf("unknown storage: %v", storageCfg)
		}
	}

	if err := validateExponentialBackoff(c.ExponentialBackoff); err != nil {
		return fmt.Errorf("validate `exponential_backoff`: %v", err)
	}

	return nil
}

func validateMongoDBConfig(c *config.TMongoDbConfig) error {
	if c == nil {
		return nil
	}

	if _, err := common.DurationFromString(c.OpenConnectionTimeout); err != nil {
		return fmt.Errorf("validate `open_connection_timeout`: %v", err)
	}

	if _, err := common.DurationFromString(c.PingConnectionTimeout); err != nil {
		return fmt.Errorf("validate `ping_connection_timeout`: %v", err)
	}

	if c.CountDocsToDeduceSchema == 0 {
		return fmt.Errorf("validate `count_docs_to_deduce_schema`: can't be zero")
	}

	if c.ObjectIdYqlType == config.TMongoDbConfig_OBJECT_ID_UNSPECIFIED {
		return fmt.Errorf("invalid `object_id_yql_type` value: %v", c.ObjectIdYqlType)
	}

	if err := validateExponentialBackoff(c.ExponentialBackoff); err != nil {
		return fmt.Errorf("validate `exponential_backoff`: %v", err)
	}

	return nil
}

func validateRedisConfig(c *config.TRedisConfig) error {
	if c == nil {
		return nil
	}

	if _, err := common.DurationFromString(c.PingConnectionTimeout); err != nil {
		return fmt.Errorf("validate `ping_connection_timeout`: %v", err)
	}

	if c.CountDocsToDeduceSchema == 0 {
		return fmt.Errorf("validate `count_docs_to_deduce_schema`: can't be zero")
	}

	if err := validateExponentialBackoff(c.ExponentialBackoff); err != nil {
		return fmt.Errorf("validate `exponential_backoff`: %v", err)
	}

	return nil
}

func validateLoggingConfig(c *config.TLoggingConfig) error {
	if c == nil {
		return nil
	}

	if err := validateYdbConfig(c.Ydb); err != nil {
		return fmt.Errorf("validate `ydb`: %w", err)
	}

	if c.GetStatic() == nil && c.GetDynamic() == nil {
		return fmt.Errorf("you should set either `static` or `dynamic` section")
	}

	if c.GetStatic() != nil && c.GetDynamic() != nil {
		return fmt.Errorf("you should set either `static` or `dynamic` section, not both of them")
	}

	if err := validateLoggingResolvingStaticConfig(c.GetStatic()); err != nil {
		return fmt.Errorf("validate `static`: %w", err)
	}

	if err := validateLoggingResolvingDynamicConfig(c.GetDynamic()); err != nil {
		return fmt.Errorf("validate `dynamic`: %w", err)
	}

	if err := validateReadLimiterConfig(c.ReadLimiter); err != nil {
		return fmt.Errorf("validate `read_limit`: %w", err)
	}

	return nil
}

func validateLoggingResolvingStaticConfig(c *config.TLoggingConfig_TStaticResolving) error {
	if c == nil {
		return nil
	}

	if len(c.Databases) == 0 {
		// it's kind of OK to have empty list of databases
		return nil
	}

	if len(c.Folders) == 0 {
		// it's kind of OK to have empty list of folders
		return nil
	}

	for i, database := range c.Databases {
		if database.Name == "" {
			return fmt.Errorf("missing `name` for database #%d", i)
		}

		if database.Endpoint.Host == "" {
			return fmt.Errorf("missing `endpoint.host` for database #%d", i)
		}

		if database.Endpoint.Port == 0 {
			return fmt.Errorf("missing `endpoint.port` for database #%d", i)
		}
	}

	for folderId, folder := range c.Folders {
		if len(folder.LogGroups) == 0 {
			return fmt.Errorf("missing `log_groups` for folder %s", folderId)
		}
	}

	return nil
}

func validateLoggingResolvingDynamicConfig(c *config.TLoggingConfig_TDynamicResolving) error {
	if c == nil {
		return nil
	}

	if c.LoggingEndpoint.Host == "" {
		return fmt.Errorf("missing `logging_endpoint.host`")
	}

	if c.LoggingEndpoint.Port == 0 {
		return fmt.Errorf("missing `logging_endpoint.port`")
	}

	return nil
}

func validateExponentialBackoff(c *config.TExponentialBackoffConfig) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	if _, err := common.DurationFromString(c.InitialInterval); err != nil {
		return fmt.Errorf("validate `initial_interval`: %v", err)
	}

	if _, err := common.DurationFromString(c.MaxInterval); err != nil {
		return fmt.Errorf("validate `max_interval`: %v", err)
	}

	if c.Multiplier == 0 {
		return errors.New("zero value for `multiplier`")
	}

	if _, err := common.DurationFromString(c.MaxElapsedTime); err != nil {
		return fmt.Errorf("validate `max_elapsed_time`: %v", err)
	}

	return nil
}

func validateOpenSearchConfig(c *config.TOpenSearchConfig) error {
	if c == nil {
		return nil
	}

	if _, err := common.DurationFromString(c.DialTimeout); err != nil {
		return fmt.Errorf("validate `dial_timeout`: %v", err)
	}

	if _, err := common.DurationFromString(c.ResponseHeaderTimeout); err != nil {
		return fmt.Errorf("validate `response_header_timeout`: %v", err)
	}

	if _, err := common.DurationFromString(c.PingConnectionTimeout); err != nil {
		return fmt.Errorf("validate `ping_connection_timeout`: %v", err)
	}

	if _, err := common.DurationFromString(c.ScrollTimeout); err != nil {
		return fmt.Errorf("validate `scroll_timeout`: %v", err)
	}

	if c.BatchSize == 0 {
		return fmt.Errorf("validate `batch_size`, must be greater than zero")
	}

	if err := validateExponentialBackoff(c.ExponentialBackoff); err != nil {
		return fmt.Errorf("validate `exponential_backoff`: %v", err)
	}

	return nil
}

func validateObservationConfig(c *config.TObservationConfig) error {
	if c == nil {
		return nil
	}

	if err := validateObservationServerConfig(c.Server); err != nil {
		return fmt.Errorf("validate `server`: %w", err)
	}

	if err := validateObservationStorageConfig(c.Storage); err != nil {
		return fmt.Errorf("validate `storage`: %w", err)
	}

	return nil
}

func validateObservationServerConfig(c *config.TObservationConfig_TServer) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	if err := validateEndpoint(c.Endpoint); err != nil {
		return fmt.Errorf("validate `endpoint`: %w", err)
	}

	return nil
}

func validateObservationStorageConfig(c *config.TObservationConfig_TStorage) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	if storage := c.GetSqlite(); storage != nil {
		if storage.Path == "" {
			return fmt.Errorf("empty `sqlite.path`")
		}

		if _, err := common.DurationFromString(storage.GcPeriod); err != nil {
			return fmt.Errorf("validate `gc_period`: %v", err)
		}

		if _, err := common.DurationFromString(storage.RequestTtl); err != nil {
			return fmt.Errorf("validate `request_ttl`: %v", err)
		}
	}

	return nil
}

func newConfigFromPrototextFile(configPath string) (*config.TServerConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read file %v: %w", configPath, err)
	}

	var cfg config.TServerConfig

	unmarshaller := prototext.UnmarshalOptions{
		// Do not emit an error if config contains outdated or too fresh fields
		DiscardUnknown: true,
	}

	if err := unmarshaller.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("prototext unmarshal `%v`: %w", string(data), err)
	}

	return &cfg, nil
}

func newConfigFromYAMLFile(configPath string) (*config.TServerConfig, error) {
	var cfg config.TServerConfig

	if err := common.NewConfigFromYAMLFile(configPath, &cfg); err != nil {
		return nil, fmt.Errorf("new config from YAML file '%s': %w", configPath, err)
	}

	return &cfg, nil
}

func NewConfigFromFile(configPath string) (*config.TServerConfig, error) {
	var parsers = map[string]func(string) (*config.TServerConfig, error){
		"yaml":      newConfigFromYAMLFile,
		"prototext": newConfigFromPrototextFile,
	}

	var (
		err  error
		errs []error
		cfg  *config.TServerConfig
	)

	// Hopefully at least one of parser will succeed
	for key, parser := range parsers {
		cfg, err = parser(configPath)
		if err == nil {
			break
		}

		errs = append(errs, fmt.Errorf("config parser '%s': %w", key, err))
	}

	if cfg == nil {
		err := errors.Join(errs...)
		return nil, err
	}

	fillServerConfigDefaults(cfg)

	if err := validateServerConfig(cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

const (
	defaultPortConnector = 2130
	defaultPortPprof     = 6060
	defaultHost          = "localhost"
)

func NewDefaultConfig() *config.TServerConfig {
	cfg := &config.TServerConfig{
		ConnectorServer: &config.TConnectorServerConfig{
			Endpoint: &api_common.TGenericEndpoint{
				Host: defaultHost,
				Port: defaultPortConnector,
			},
		},
		PprofServer: &config.TPprofServerConfig{
			Endpoint: &api_common.TGenericEndpoint{
				Host: defaultHost,
				Port: defaultPortPprof,
			},
		},
	}

	fillServerConfigDefaults(cfg)

	return cfg
}
