package observation

import api_common "github.com/ydb-platform/fq-connector-go/api/common"

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

func (s storageDummyImpl) UpdateQueryProgress(id QueryID, rowsRead int64, bytesRead int64) error {
	return nil
}

func (s storageDummyImpl) FinishQuery(id QueryID, rowsRead int64, bytesRead int64) error {
	return nil
}

func (s storageDummyImpl) CancelQuery(id QueryID, errorMsg string, rowsRead int64, bytesRead int64) error {
	return nil
}

func (s storageDummyImpl) DeleteQuery(id QueryID) error {
	return nil
}

func (s storageDummyImpl) GetRunningQueries() ([]*Query, error) {
	return nil, nil
}

func (s storageDummyImpl) FindSimilarQueriesWithDifferentUsage() ([][]*Query, error) {
	return nil, nil
}

func (s storageDummyImpl) Close() error {
	return nil
}
