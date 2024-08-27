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
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ms_sql_server"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/mysql"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/oracle"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/postgresql"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.Factory[any] = (*dataSourceFactory)(nil)

type dataSourceFactory struct {
	clickhouse          Preset
	postgresql          Preset
	ydb                 Preset
	msSQLServer         Preset
	mysql               Preset
	greenplum           Preset
	oracle              Preset
	converterCollection conversion.Collection
}

func (dsf *dataSourceFactory) Make(
	logger *zap.Logger,
	dataSourceType api_common.EDataSourceKind,
) (datasource.DataSource[any], error) {
	switch dataSourceType {
	case api_common.EDataSourceKind_CLICKHOUSE:
		return NewDataSource(logger, &dsf.clickhouse, dsf.converterCollection), nil
	case api_common.EDataSourceKind_POSTGRESQL:
		return NewDataSource(logger, &dsf.postgresql, dsf.converterCollection), nil
	case api_common.EDataSourceKind_YDB:
		return NewDataSource(logger, &dsf.ydb, dsf.converterCollection), nil
	case api_common.EDataSourceKind_MS_SQL_SERVER:
		return NewDataSource(logger, &dsf.msSQLServer, dsf.converterCollection), nil
	case api_common.EDataSourceKind_MYSQL:
		return NewDataSource(logger, &dsf.mysql, dsf.converterCollection), nil
	case api_common.EDataSourceKind_GREENPLUM:
		return NewDataSource(logger, &dsf.greenplum, dsf.converterCollection), nil
	case api_common.EDataSourceKind_ORACLE:
		return NewDataSource(logger, &dsf.oracle, dsf.converterCollection), nil
	default:
		return nil, fmt.Errorf("pick handler for data source type '%v': %w", dataSourceType, common.ErrDataSourceNotSupported)
	}
}
func NewDataSourceFactory(
	cfg *config.TDatasourcesConfig,
	qlf common.QueryLoggerFactory,
	converterCollection conversion.Collection,
) datasource.Factory[any] {
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
	schemaGetters := map[api_common.EDataSourceKind]func(dsi *api_common.TDataSourceInstance) string{
		api_common.EDataSourceKind_POSTGRESQL: func(dsi *api_common.TDataSourceInstance) string { return dsi.GetPgOptions().Schema },
		api_common.EDataSourceKind_GREENPLUM:  func(dsi *api_common.TDataSourceInstance) string { return dsi.GetGpOptions().Schema },
	}

	return &dataSourceFactory{
		clickhouse: Preset{
			SQLFormatter:      clickhouse.NewSQLFormatter(),
			ConnectionManager: clickhouse.NewConnectionManager(cfg.Clickhouse, connManagerBase),
			TypeMapper:        clickhouseTypeMapper,
			SchemaProvider:    rdbms_utils.NewDefaultSchemaProvider(clickhouseTypeMapper, clickhouse.TableMetadataQuery),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.Clickhouse.ExponentialBackoff, clickhouse.ErrorCheckerMakeConnection),
				Query:          retry.NewRetrierFromConfig(cfg.Clickhouse.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
		},
		postgresql: Preset{
			SQLFormatter: postgresql.NewSQLFormatter(),
			ConnectionManager: postgresql.NewConnectionManager(
				cfg.Postgresql, connManagerBase, schemaGetters[api_common.EDataSourceKind_POSTGRESQL]),
			TypeMapper: postgresqlTypeMapper,
			SchemaProvider: rdbms_utils.NewDefaultSchemaProvider(
				postgresqlTypeMapper,
				func(request *api_service_protos.TDescribeTableRequest) (string, []any) {
					return postgresql.TableMetadataQuery(
						request,
						schemaGetters[api_common.EDataSourceKind_POSTGRESQL](request.DataSourceInstance))
				}),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.Postgresql.ExponentialBackoff, postgresql.ErrorCheckerMakeConnection),
				Query:          retry.NewRetrierFromConfig(cfg.Postgresql.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
		},
		ydb: Preset{
			SQLFormatter:      ydb.NewSQLFormatter(),
			ConnectionManager: ydb.NewConnectionManager(cfg.Ydb, connManagerBase),
			TypeMapper:        ydbTypeMapper,
			SchemaProvider:    ydb.NewSchemaProvider(ydbTypeMapper),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.Ydb.ExponentialBackoff, ydb.ErrorCheckerMakeConnection),
				Query:          retry.NewRetrierFromConfig(cfg.Ydb.ExponentialBackoff, ydb.ErrorCheckerQuery),
			},
		},
		msSQLServer: Preset{
			SQLFormatter:      ms_sql_server.NewSQLFormatter(),
			ConnectionManager: ms_sql_server.NewConnectionManager(cfg.MsSqlServer, connManagerBase),
			TypeMapper:        msSQLServerTypeMapper,
			SchemaProvider:    rdbms_utils.NewDefaultSchemaProvider(msSQLServerTypeMapper, ms_sql_server.TableMetadataQuery),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.MsSqlServer.ExponentialBackoff, ms_sql_server.ErrorCheckerMakeConnection),
				Query:          retry.NewRetrierFromConfig(cfg.MsSqlServer.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
		},
		mysql: Preset{
			SQLFormatter:      mysql.NewSQLFormatter(),
			ConnectionManager: mysql.NewConnectionManager(cfg.Mysql, connManagerBase),
			TypeMapper:        mysqlTypeMapper,
			SchemaProvider:    rdbms_utils.NewDefaultSchemaProvider(mysqlTypeMapper, mysql.TableMetadataQuery),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.Mysql.ExponentialBackoff, retry.ErrorCheckerMakeConnectionCommon),
				Query:          retry.NewRetrierFromConfig(cfg.Mysql.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
		},
		greenplum: Preset{
			SQLFormatter: postgresql.NewSQLFormatter(),
			ConnectionManager: postgresql.NewConnectionManager(
				cfg.Greenplum, connManagerBase, schemaGetters[api_common.EDataSourceKind_GREENPLUM]),
			TypeMapper: postgresqlTypeMapper,
			SchemaProvider: rdbms_utils.NewDefaultSchemaProvider(
				postgresqlTypeMapper,
				func(request *api_service_protos.TDescribeTableRequest) (string, []any) {
					return postgresql.TableMetadataQuery(
						request,
						schemaGetters[api_common.EDataSourceKind_GREENPLUM](request.DataSourceInstance))
				}),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.Greenplum.ExponentialBackoff, postgresql.ErrorCheckerMakeConnection),
				Query:          retry.NewRetrierFromConfig(cfg.Greenplum.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
		},
		oracle: Preset{
			SQLFormatter:      oracle.NewSQLFormatter(),
			ConnectionManager: oracle.NewConnectionManager(cfg.Oracle, connManagerBase),
			TypeMapper:        oracleTypeMapper,
			SchemaProvider:    rdbms_utils.NewDefaultSchemaProvider(oracleTypeMapper, oracle.TableMetadataQuery),
			RetrierSet: &retry.RetrierSet{
				MakeConnection: retry.NewRetrierFromConfig(cfg.Oracle.ExponentialBackoff, oracle.ErrorCheckerMakeConnection),
				Query:          retry.NewRetrierFromConfig(cfg.Oracle.ExponentialBackoff, retry.ErrorCheckerNoop),
			},
		},
		converterCollection: converterCollection,
	}
}
