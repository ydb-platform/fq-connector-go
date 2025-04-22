package observation

import (
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

var _ Storage = (*storageDummyImpl)(nil)

type storageDummyImpl struct {
}

func (s storageDummyImpl) CreateQuery(dsi *api_common.TGenericDataSourceInstance) (QueryID, error) {
	return 0, nil
}

func (s storageDummyImpl) SetQueryDetails(id QueryID, queryText string, queryArgs string) error {
	return nil
}

func (s storageDummyImpl) GetQuery(id QueryID) (*Query, error) {
	return nil, nil
}

func (s storageDummyImpl) ListQueries(state *QueryState, limit int, offset int) ([]*Query, error) {
	return nil, nil
}

func (s storageDummyImpl) ListRunningQueries() ([]*Query, error) {
	return nil, nil
}

func (s storageDummyImpl) ListSimilarQueriesWithDifferentStats() ([][]*Query, error) {
	return nil, nil
}

func (s storageDummyImpl) FinishQuery(id QueryID, stats *api_service_protos.TReadSplitsResponse_TStats) error {
	return nil
}

func (s storageDummyImpl) CancelQuery(id QueryID, errorMsg string, stats *api_service_protos.TReadSplitsResponse_TStats) error {
	return nil
}

func (s storageDummyImpl) DeleteQuery(id QueryID) error {
	return nil
}

func (s storageDummyImpl) close() error {
	return nil
}
