package config

import (
	"errors"
	"fmt"
	"math"
	"os"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"sigs.k8s.io/yaml"

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

// TODO: use reflection to generalize datasource setting code
//
//nolint:gocyclo
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

	// Greenplum

	if c.Datasources.Greenplum == nil {
		c.Datasources.Greenplum = &config.TGreenplumConfig{
			OpenConnectionTimeout: "5s",
		}
	}

	if c.Datasources.Greenplum.ExponentialBackoff == nil {
		c.Datasources.Greenplum.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
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

	// PostgreSQL

	if c.Datasources.Postgresql == nil {
		c.Datasources.Postgresql = &config.TPostgreSQLConfig{
			OpenConnectionTimeout: "5s",
		}
	}

	if c.Datasources.Postgresql.ExponentialBackoff == nil {
		c.Datasources.Postgresql.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
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

func fillYdbConfigDefaults(c *config.TYdbConfig) {
	if c.OpenConnectionTimeout == "" {
		c.OpenConnectionTimeout = "5s"
	}

	if c.PingConnectionTimeout == "" {
		c.PingConnectionTimeout = "5s"
	}

	if c.Mode == config.TYdbConfig_MODE_UNSPECIFIED {
		c.Mode = config.TYdbConfig_MODE_TABLE_SERVICE_STDLIB_SCAN_QUERIES
	}

	if c.ExponentialBackoff == nil {
		c.ExponentialBackoff = makeDefaultExponentialBackoffConfig()
	}

	if c.ServiceAccountKeyFileCredentials != "" {
		if c.IamEndpoint == nil {
			c.IamEndpoint = &api_common.TGenericEndpoint{
				Host: "iam.api.cloud.yandex.net",
				Port: 443,
			}
		}
	}
}

func validateServerConfig(c *config.TServerConfig) error {
	if err := validateConnectorServerConfig(c.ConnectorServer); err != nil {
		return fmt.Errorf("validate `connector_server`: %w", err)
	}

	if err := validateServerReadLimit(c.ReadLimit); err != nil {
		return fmt.Errorf("validate `read_limit`: %w", err)
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

	if err := fileMustExist(c.Key); err != nil {
		return fmt.Errorf("invalid value of field `key`: %w", err)
	}

	if err := fileMustExist(c.Cert); err != nil {
		return fmt.Errorf("invalid value of field `cert`: %w", err)
	}

	return nil
}

func validateServerReadLimit(c *config.TServerReadLimit) error {
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

	if err := validateYdbConfig(c.Ydb); err != nil {
		return fmt.Errorf("validate `ydb`: %w", err)
	}

	if err := validateLoggingConfig(c.Logging); err != nil {
		return fmt.Errorf("validate `logging`: %w", err)
	}

	return nil
}

func validateYdbConfig(c *config.TYdbConfig) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	if _, err := common.DurationFromString(c.OpenConnectionTimeout); err != nil {
		return fmt.Errorf("validate `open_connection_timeout`: %v", err)
	}

	if _, err := common.DurationFromString(c.PingConnectionTimeout); err != nil {
		return fmt.Errorf("validate `ping_connection_timeout`: %v", err)
	}

	if c.Mode == config.TYdbConfig_MODE_UNSPECIFIED {
		return fmt.Errorf("invalid `mode` value: %v", c.Mode)
	}

	if c.ServiceAccountKeyFileCredentials != "" {
		if err := fileMustExist(c.ServiceAccountKeyFileCredentials); err != nil {
			return fmt.Errorf("invalid value of field `service_account_key_file_credentials`: %w", err)
		}

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

	if err := validateExponentialBackoff(c.ExponentialBackoff); err != nil {
		return fmt.Errorf("validate `exponential_backoff`: %v", err)
	}

	return nil
}

func validateLoggingConfig(c *config.TLoggingConfig) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	if err := validateYdbConfig(c.Ydb); err != nil {
		return fmt.Errorf("validate `ydb`: %w", err)
	}

	if staticConfig := c.GetStatic(); staticConfig != nil {
		if err := validateLoggingResolvingStaticConfig(staticConfig); err != nil {
			return fmt.Errorf("validate `static`: %w", err)
		}
	} else {
		return fmt.Errorf("missing `static` section")
	}

	return nil
}

func validateLoggingResolvingStaticConfig(c *config.TLoggingConfig_TStaticResolving) error {
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

func fileMustExist(path string) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return fmt.Errorf("path '%s' does not exist", path)
	}

	if info.IsDir() {
		return fmt.Errorf("path '%s' is a directory", path)
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
	dataYAML, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read file %v: %w", configPath, err)
	}

	// convert YAML to JSON

	dataJSON, err := yaml.YAMLToJSON(dataYAML)
	if err != nil {
		return nil, fmt.Errorf("convert YAML to JSON: %w", err)
	}

	var cfg config.TServerConfig

	// than parse JSON

	unmarshaller := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}

	if err := unmarshaller.Unmarshal(dataJSON, &cfg); err != nil {
		return nil, fmt.Errorf("protojson unmarshal `%v`: %w", string(dataJSON), err)
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
