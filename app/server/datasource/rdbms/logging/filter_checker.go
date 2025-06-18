package logging

import (
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

var _ rdbms_utils.FilterChecker = (*filterCheckerImpl)(nil)

type filterCheckerImpl struct{}

func (filterCheckerImpl) CheckFilter(_ *api_service_protos.TSelect_TWhere) error {
	return nil
}
