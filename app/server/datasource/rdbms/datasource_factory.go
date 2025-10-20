package rdbms

import (
	"fmt"

	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/clickhouse"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/logging"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ms_sql_server"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/mysql"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/oracle"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/postgresql"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb/table_metadata_cache"
	"github.com/ydb-platform/fq-connector-go/app/server/observation"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.Factory[any] = (*dataSourceFactory)(nil)

type dataSourceFactory struct {
	clickhouse  Preset
	postgresql  Preset
	ydb         Preset
	msSQLServer Preset
	mysql       Preset
	greenplum   Preset
	oracle      Preset
	logging     Preset

	observationStorage  observation.Storage
	loggingResolver     logging.Resolver
	converterCollection conversion.Collection
}

func (dsf *dataSourceFactory) Make(
	logger *zap.Logger,
	dataSourceType api_common.EGenericDataSourceKind,
) (datasource.DataSource[any], error) {
	switch dataSourceType {
	case api_common.EGenericDataSourceKind_CLICKHOUSE:
		return NewDataSource(logger, &dsf.clickhouse, dsf.converterCollection, dsf.observationStorage), nil
	case api_common.EGenericDataSourceKind_POSTGRESQL:
		return NewDataSource(logger, &dsf.postgresql, dsf.converterCollection, dsf.observationStorage), nil
	case api_common.EGenericDataSourceKind_YDB:
		return NewDataSource(logger, &dsf.ydb, dsf.converterCollection, dsf.observationStorage), nil
	case api_common.EGenericDataSourceKind_MS_SQL_SERVER:
		return NewDataSource(logger, &dsf.msSQLServer, dsf.converterCollection, dsf.observationStorage), nil
	case api_common.EGenericDataSourceKind_MYSQL:
		return NewDataSource(logger, &dsf.mysql, dsf.converterCollection, dsf.observationStorage), nil
	case api_common.EGenericDataSourceKind_GREENPLUM:
		return NewDataSource(logger, &dsf.greenplum, dsf.converterCollection, dsf.observationStorage), nil
	case api_common.EGenericDataSourceKind_ORACLE:
		return NewDataSource(logger, &dsf.oracle, dsf.converterCollection, dsf.observationStorage), nil
	case api_common.EGenericDataSourceKind_LOGGING:
		return NewDataSource(logger, &dsf.logging, dsf.converterCollection, dsf.observationStorage), nil
	default:
		return nil, fmt.Errorf("pick handler for data source type '%v': %w", dataSourceType, common.ErrDataSourceNotSupported)
	}
}

func (dsf *dataSourceFactory) Close() error {
	if err := dsf.loggingResolver.Close(); err != nil {
		return fmt.Errorf("close logging resolver: %w", err)
	}

	return nil
}

func NewDataSourceFactory(
	cfg *config.TDatasourcesConfig,
	qlf common.QueryLoggerFactory,
	converterCollection conversion.Collection,
	observationStorage observation.Storage,
	ydbTableMetadataCache table_metadata_cache.Cache,
) (datasource.Factory[any], error) {
	connManagerBase := rdbms_utils.ConnectionManagerBase{
		QueryLoggerFactory: qlf,
	}

	postgresqlTypeMapper := postgresql.NewTypeMapper()
	clickhouseTypeMapper := clickhouse.NewTypeMapper()
	ydbTypeMapper := ydb.NewTypeMapper()
	msSQLServerTypeMapper := ms_sql_server.NewTypeMapper()
	mysqlTypeMapper := mysql.NewTypeMapper()
	oracleTypeMapper := oracle.NewTypeMapper()

	// for PostgreSQL-like systems
	schemaGetters := map[api_common.EGenericDataSourceKind]func(dsi *api_common.TGenericDataSourceInstance) string{
		api_common.EGenericDataSourceKind_POSTGRESQL: func(dsi *api_common.TGenericDataSourceInstance) string {
			return dsi.GetPgOptions().Schema
		},
		api_common.EGenericDataSourceKind_GREENPLUM: func(dsi *api_common.TGenericDataSourceInstance) string {
			return dsi.GetGpOptions().Schema
		},
	}

	dsf := &dataSourceFactory{
		clickhouse: Preset{
			SQLFormatter:      clickhouse.NewSQLFormatter(cfg.Clickhouse.Pushdown),
			ConnectionManager: clickhouse.NewConnectionManager(cfg.Clickhouse, connManagerBase),
			TypeMapper:        clickhouseTypeMapper,
			SchemaProvider:    rdbms_utils.NewDefaultSchemaProvider(clickhouseTypeMapper, clickhouse.TableMetadataQuery),
			SplitProvider:     rdbms_utils.NewDefaultSplitProvider(),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.Clickhouse.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
				Query:          retry.NewRetrierFromConfig(cfg.Clickhouse.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
		},
		postgresql: Preset{
			SQLFormatter: postgresql.NewSQLFormatter(cfg.Postgresql.Pushdown),
			ConnectionManager: postgresql.NewConnectionManager(
				cfg.Postgresql, connManagerBase, schemaGetters[api_common.EGenericDataSourceKind_POSTGRESQL]),
			TypeMapper: postgresqlTypeMapper,
			SchemaProvider: rdbms_utils.NewDefaultSchemaProvider(
				postgresqlTypeMapper,
				func(request *api_service_protos.TDescribeTableRequest) (string, *rdbms_utils.QueryArgs) {
					return postgresql.TableMetadataQuery(
						request,
						schemaGetters[api_common.EGenericDataSourceKind_POSTGRESQL](request.DataSourceInstance))
				}),
			SplitProvider: postgresql.NewSplitProvider(cfg.Postgresql.Splitting),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.Postgresql.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
				Query:          retry.NewRetrierFromConfig(cfg.Postgresql.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
		},
		ydb: Preset{
			SQLFormatter:      ydb.NewSQLFormatter(cfg.Ydb.Mode, cfg.Ydb.Pushdown),
			ConnectionManager: ydb.NewConnectionManager(cfg.Ydb, connManagerBase),
			TypeMapper:        ydbTypeMapper,
			SchemaProvider:    ydb.NewSchemaProvider(ydbTypeMapper, ydbTableMetadataCache),
			SplitProvider:     ydb.NewSplitProvider(cfg.Ydb.Splitting, ydbTableMetadataCache),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.Ydb.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
				Query:          retry.NewRetrierFromConfig(cfg.Ydb.ExponentialBackoff, ydb.ErrorCheckerQuery),
			},
		},
		msSQLServer: Preset{
			SQLFormatter:      ms_sql_server.NewSQLFormatter(cfg.MsSqlServer.Pushdown),
			ConnectionManager: ms_sql_server.NewConnectionManager(cfg.MsSqlServer, connManagerBase),
			TypeMapper:        msSQLServerTypeMapper,
			SchemaProvider:    rdbms_utils.NewDefaultSchemaProvider(msSQLServerTypeMapper, ms_sql_server.TableMetadataQuery),
			SplitProvider:     rdbms_utils.NewDefaultSplitProvider(),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.MsSqlServer.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
				Query:          retry.NewRetrierFromConfig(cfg.MsSqlServer.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
		},
		mysql: Preset{
			SQLFormatter:      mysql.NewSQLFormatter(cfg.Mysql.Pushdown),
			ConnectionManager: mysql.NewConnectionManager(cfg.Mysql, connManagerBase),
			TypeMapper:        mysqlTypeMapper,
			SchemaProvider:    rdbms_utils.NewDefaultSchemaProvider(mysqlTypeMapper, mysql.TableMetadataQuery),
			SplitProvider:     rdbms_utils.NewDefaultSplitProvider(),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.Mysql.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
				Query:          retry.NewRetrierFromConfig(cfg.Mysql.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
		},
		greenplum: Preset{
			SQLFormatter: postgresql.NewSQLFormatter(cfg.Greenplum.Pushdown),
			ConnectionManager: postgresql.NewConnectionManager(
				cfg.Greenplum, connManagerBase, schemaGetters[api_common.EGenericDataSourceKind_GREENPLUM]),
			TypeMapper: postgresqlTypeMapper,
			SchemaProvider: rdbms_utils.NewDefaultSchemaProvider(
				postgresqlTypeMapper,
				func(request *api_service_protos.TDescribeTableRequest) (string, *rdbms_utils.QueryArgs) {
					return postgresql.TableMetadataQuery(
						request,
						schemaGetters[api_common.EGenericDataSourceKind_GREENPLUM](request.DataSourceInstance))
				}),
			SplitProvider: rdbms_utils.NewDefaultSplitProvider(),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.Greenplum.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
				Query:          retry.NewRetrierFromConfig(cfg.Greenplum.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
		},
		oracle: Preset{
			SQLFormatter:      oracle.NewSQLFormatter(cfg.Oracle.Pushdown),
			ConnectionManager: oracle.NewConnectionManager(cfg.Oracle, connManagerBase),
			TypeMapper:        oracleTypeMapper,
			SchemaProvider:    rdbms_utils.NewDefaultSchemaProvider(oracleTypeMapper, oracle.TableMetadataQuery),
			SplitProvider:     rdbms_utils.NewDefaultSplitProvider(),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.Oracle.ExponentialBackoff, oracle.ErrorCheckerMakeConnection),
				Query:          retry.NewRetrierFromConfig(cfg.Oracle.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
		},
		converterCollection: converterCollection,
	}

	var err error

	dsf.loggingResolver, err = logging.NewResolver(cfg.Logging)
	if err != nil {
		return nil, fmt.Errorf("logging resolver: %w", err)
	}

	dsf.logging = Preset{
		SQLFormatter:      logging.NewSQLFormatter(ydb.NewSQLFormatter(cfg.Logging.Ydb.Mode, cfg.Logging.Ydb.Pushdown)),
		ConnectionManager: logging.NewConnectionManager(cfg.Logging, connManagerBase, dsf.loggingResolver),
		TypeMapper:        nil,
		SchemaProvider:    logging.NewSchemaProvider(),
		SplitProvider:     logging.NewSplitProvider(dsf.loggingResolver, ydb.NewSplitProvider(cfg.Logging.Ydb.Splitting, ydbTableMetadataCache)),
		RetrierSet: &retry.RetrierSet{
			MakeConnection: retry.NewRetrierFromConfig(cfg.Ydb.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
			Query:          retry.NewRetrierFromConfig(cfg.Ydb.ExponentialBackoff, ydb.ErrorCheckerQuery),
		},
	}

	dsf.observationStorage = observationStorage

	return dsf, nil
}
