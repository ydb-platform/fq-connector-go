package utils

import api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"

type filterCheckerNoop struct{}

func (filterCheckerNoop) CheckFilter(where *api_service_protos.TSelect_TWhere) error {
	return nil
}

func NewFilterCheckerNoop() FilterChecker {
	return filterCheckerNoop{}
}
