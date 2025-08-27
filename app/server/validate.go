package server

import (
	"fmt"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

func ValidateDescribeTableRequest(request *api_service_protos.TDescribeTableRequest) error {
	if err := validateDataSourceInstance(request.GetDataSourceInstance()); err != nil {
		return fmt.Errorf("validate data source instance: %w", err)
	}

	if request.GetTable() == "" {
		return fmt.Errorf("empty table: %w", common.ErrInvalidRequest)
	}

	return nil
}

func ValidateListSplitsRequest(request *api_service_protos.TListSplitsRequest) error {
	if len(request.Selects) == 0 {
		return fmt.Errorf("empty select list: %w", common.ErrInvalidRequest)
	}

	for i, slct := range request.Selects {
		if err := validateSelect(slct); err != nil {
			return fmt.Errorf("validate select %d: %w", i, err)
		}
	}

	if request.MaxSplitCount != 0 {
		for _, slct := range request.Selects {
			switch slct.DataSourceInstance.Kind {
			case api_common.EGenericDataSourceKind_LOGGING:
				if request.MaxSplitCount != 3 {
					return fmt.Errorf("invalid max split count: %d", request.MaxSplitCount)
				}
			case api_common.EGenericDataSourceKind_YDB:
			default:
				return fmt.Errorf("unsupported data source kind: %s", slct.DataSourceInstance.Kind)
			}
		}
	}

	if request.SplitNumberLimit != 0 {
		return fmt.Errorf("split number limit is currently unsupported: %w", common.ErrInvalidRequest)
	}

	if request.SplitSize != 0 {
		return fmt.Errorf("split size is currently unsupported: %w", common.ErrInvalidRequest)
	}

	return nil
}

func ValidateReadSplitsRequest(request *api_service_protos.TReadSplitsRequest) error {
	if len(request.Splits) == 0 {
		return fmt.Errorf("splits are empty: %w", common.ErrInvalidRequest)
	}

	for i, split := range request.Splits {
		if err := validateSplit(split); err != nil {
			return fmt.Errorf("validate split #%d: %w", i, err)
		}
	}

	return nil
}

func validateSplit(split *api_service_protos.TSplit) error {
	if err := validateSelect(split.Select); err != nil {
		return fmt.Errorf("validate select: %w", err)
	}

	return nil
}

func validateSelect(slct *api_service_protos.TSelect) error {
	if slct == nil {
		return fmt.Errorf("select is empty: %w", common.ErrInvalidRequest)
	}

	if err := validateDataSourceInstance(slct.GetDataSourceInstance()); err != nil {
		return fmt.Errorf("validate data source instance: %w", err)
	}

	return nil
}

type dataSourceInstancesValidator func(dsi *api_common.TGenericDataSourceInstance) error

func validateDataSourceInstance(dsi *api_common.TGenericDataSourceInstance) error {
	if dsi == nil {
		return fmt.Errorf("empty data source instance: %w", common.ErrInvalidRequest)
	}

	var validators []dataSourceInstancesValidator

	switch dsi.Kind {
	case api_common.EGenericDataSourceKind_DATA_SOURCE_KIND_UNSPECIFIED:
		return fmt.Errorf("empty kind: %w", common.ErrInvalidRequest)
	case api_common.EGenericDataSourceKind_LOGGING:
	case api_common.EGenericDataSourceKind_ORACLE:
		validators = append(validators, validateEndpoint)
	default:
		validators = append(validators, validateEndpoint, validateDatabase)
	}

	validators = append(validators, validateDataSourceOptions)

	for _, v := range validators {
		if err := v(dsi); err != nil {
			return fmt.Errorf("validate data source instance: %w", err)
		}
	}

	return nil
}

func validateDataSourceOptions(dsi *api_common.TGenericDataSourceInstance) error {
	switch dsi.GetKind() {
	case api_common.EGenericDataSourceKind_POSTGRESQL:
		if dsi.GetPgOptions().GetSchema() == "" {
			return fmt.Errorf("schema field is empty: %w", common.ErrInvalidRequest)
		}
	case api_common.EGenericDataSourceKind_ORACLE:
		if dsi.GetOracleOptions().GetServiceName() == "" {
			return fmt.Errorf("service_name field is empty: %w", common.ErrInvalidRequest)
		}
	case api_common.EGenericDataSourceKind_MS_SQL_SERVER:
		// TODO: check schema
		return nil

	case api_common.EGenericDataSourceKind_GREENPLUM:
		return nil
	case api_common.EGenericDataSourceKind_LOGGING:
		if dsi.GetLoggingOptions().GetFolderId() == "" {
			return fmt.Errorf("folder_id field is empty: %w", common.ErrInvalidRequest)
		}
	case api_common.EGenericDataSourceKind_CLICKHOUSE,
		api_common.EGenericDataSourceKind_S3,
		api_common.EGenericDataSourceKind_YDB,
		api_common.EGenericDataSourceKind_MYSQL,
		api_common.EGenericDataSourceKind_MONGO_DB,
		api_common.EGenericDataSourceKind_REDIS,
		api_common.EGenericDataSourceKind_OPENSEARCH:
	default:
		return fmt.Errorf("unsupported data source %s: %w", dsi.GetKind().String(), common.ErrInvalidRequest)
	}

	return nil
}

func validateEndpoint(dsi *api_common.TGenericDataSourceInstance) error {
	endpoint := dsi.GetEndpoint()

	if endpoint == nil {
		return fmt.Errorf("endpoint is empty: %w", common.ErrInvalidRequest)
	}

	if endpoint.Host == "" {
		return fmt.Errorf("endpoint.host is empty: %w", common.ErrInvalidRequest)
	}

	if endpoint.Port == 0 {
		return fmt.Errorf("endpoint.port is empty: %w", common.ErrInvalidRequest)
	}

	return nil
}

func validateDatabase(dsi *api_common.TGenericDataSourceInstance) error {
	if dsi.Database == "" {
		return fmt.Errorf("database is empty: %w", common.ErrInvalidRequest)
	}

	return nil
}
