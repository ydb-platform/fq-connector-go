package rdbms

import (
	"fmt"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/clickhouse"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/postgresql"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"go.uber.org/zap"
)

var _ datasource.DataSourceFactory[any] = (*dataSourceFactory)(nil)

type dataSourceFactory struct {
	clickhouse Preset
	postgresql Preset
	ydb        Preset
}

func (dsf *dataSourceFactory) Make(
	logger *zap.Logger,
	dataSourceType api_common.EDataSourceKind,
) (datasource.DataSource[any], error) {
	switch dataSourceType {
	case api_common.EDataSourceKind_CLICKHOUSE:
		return NewDataSource(logger, &dsf.clickhouse), nil
	case api_common.EDataSourceKind_POSTGRESQL:
		return NewDataSource(logger, &dsf.postgresql), nil
	case api_common.EDataSourceKind_YDB:
		return NewDataSource(logger, &dsf.ydb), nil
	default:
		return nil, fmt.Errorf("pick handler for data source type '%v': %w", dataSourceType, utils.ErrDataSourceNotSupported)
	}
}

func NewDataSourceFactory(qlf utils.QueryLoggerFactory) datasource.DataSourceFactory[any] {
	connManagerCfg := rdbms_utils.ConnectionManagerBase{
		QueryLoggerFactory: qlf,
	}

	postgresqlTypeMapper := postgresql.NewTypeMapper()
	clickhouseTypeMapper := clickhouse.NewTypeMapper()
	ydbTypeMapper := ydb.NewTypeMapper()

	return &dataSourceFactory{
		clickhouse: Preset{
			SQLFormatter:      clickhouse.NewSQLFormatter(),
			ConnectionManager: clickhouse.NewConnectionManager(connManagerCfg),
			TypeMapper:        clickhouseTypeMapper,
			SchemaProvider:    rdbms_utils.NewDefaultSchemaProvider(clickhouseTypeMapper, clickhouse.GetQueryAndArgs),
		},
		postgresql: Preset{
			SQLFormatter:      postgresql.NewSQLFormatter(),
			ConnectionManager: postgresql.NewConnectionManager(connManagerCfg),
			TypeMapper:        postgresqlTypeMapper,
			SchemaProvider:    rdbms_utils.NewDefaultSchemaProvider(postgresqlTypeMapper, postgresql.GetQueryAndArgs),
		},
		ydb: Preset{
			SQLFormatter:      ydb.NewSQLFormatter(),
			ConnectionManager: ydb.NewConnectionManager(connManagerCfg),
			TypeMapper:        ydbTypeMapper,
			SchemaProvider:    ydb.NewSchemaProvider(ydbTypeMapper),
		},
	}
}
