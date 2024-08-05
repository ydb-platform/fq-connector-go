package server

import (
	"fmt"

	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

func ValidateDescribeTableRequest(logger *zap.Logger, request *api_service_protos.TDescribeTableRequest) error {
	if err := validateDataSourceInstance(logger, request.GetDataSourceInstance()); err != nil {
		return fmt.Errorf("validate data source instance: %w", err)
	}

	if request.GetTable() == "" {
		return fmt.Errorf("empty table: %w", common.ErrInvalidRequest)
	}

	return nil
}

func ValidateListSplitsRequest(logger *zap.Logger, request *api_service_protos.TListSplitsRequest) error {
	if len(request.Selects) == 0 {
		return fmt.Errorf("empty select list: %w", common.ErrInvalidRequest)
	}

	for i, slct := range request.Selects {
		if err := validateSelect(logger, slct); err != nil {
			return fmt.Errorf("validate select %d: %w", i, err)
		}
	}

	return nil
}

func ValidateReadSplitsRequest(logger *zap.Logger, request *api_service_protos.TReadSplitsRequest) error {
	if len(request.Splits) == 0 {
		return fmt.Errorf("splits are empty: %w", common.ErrInvalidRequest)
	}

	for i, split := range request.Splits {
		if err := validateSplit(logger, split); err != nil {
			return fmt.Errorf("validate split #%d: %w", i, err)
		}
	}

	return nil
}

func validateSplit(logger *zap.Logger, split *api_service_protos.TSplit) error {
	if err := validateSelect(logger, split.Select); err != nil {
		return fmt.Errorf("validate select: %w", err)
	}

	return nil
}

func validateSelect(logger *zap.Logger, slct *api_service_protos.TSelect) error {
	if slct == nil {
		return fmt.Errorf("select is empty: %w", common.ErrInvalidRequest)
	}

	if err := validateDataSourceInstance(logger, slct.GetDataSourceInstance()); err != nil {
		return fmt.Errorf("validate data source instance: %w", err)
	}

	return nil
}

func validateDataSourceInstance(logger *zap.Logger, dsi *api_common.TDataSourceInstance) error {
	if dsi == nil {
		return fmt.Errorf("empty data source instance: %w", common.ErrInvalidRequest)
	}

	if dsi.GetKind() == api_common.EDataSourceKind_DATA_SOURCE_KIND_UNSPECIFIED {
		return fmt.Errorf("empty kind: %w", common.ErrInvalidRequest)
	}

	if dsi.Endpoint == nil {
		return fmt.Errorf("endpoint is empty: %w", common.ErrInvalidRequest)
	}

	if dsi.Endpoint.Host == "" {
		return fmt.Errorf("endpoint.host is empty: %w", common.ErrInvalidRequest)
	}

	if dsi.Endpoint.Port == 0 {
		return fmt.Errorf("endpoint.port is empty: %w", common.ErrInvalidRequest)
	}

	if dsi.Database == "" {
		return fmt.Errorf("database field is empty: %w", common.ErrInvalidRequest)
	}

	if dsi.UseTls {
		logger.Info("connector will use secure connection to access data source")
	} else {
		logger.Warn("connector will use insecure connection to access data source")
	}

	switch dsi.GetKind() {
	case api_common.EDataSourceKind_POSTGRESQL:
		if dsi.GetPgOptions().GetSchema() == "" {
			return fmt.Errorf("schema field is empty: %w", common.ErrInvalidRequest)
		}
	case api_common.EDataSourceKind_ORACLE:
		if dsi.GetOracleOptions().GetServiceName() == "" {
			return fmt.Errorf("service_name field is empty: %w", common.ErrInvalidRequest)
		}
	case api_common.EDataSourceKind_MS_SQL_SERVER:
		// TODO: check schema
		return nil

	case api_common.EDataSourceKind_GREENPLUM:
		return nil
	case api_common.EDataSourceKind_CLICKHOUSE, api_common.EDataSourceKind_S3, api_common.EDataSourceKind_YDB,
		api_common.EDataSourceKind_MYSQL:
	default:
		return fmt.Errorf("unsupported data source: %w", common.ErrInvalidRequest)
	}

	return nil
}
