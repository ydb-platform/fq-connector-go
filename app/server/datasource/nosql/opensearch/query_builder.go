package opensearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

type queryBuilder struct {
	logger *zap.Logger
}

func newQueryBuilder(logger *zap.Logger) *QueryBuilder {
	return &QueryBuilder{logger: logger}
}

func (qb *QueryBuilder) BuildSearchQuery(
	split *api_service_protos.TSplit,
	filtering api_service_protos.TReadSplitsRequest_EFiltering,
	batchSize uint64,
	scrollTimeout time.Duration,
) (io.Reader, *opensearchapi.SearchParams, error) {
	params := &opensearchapi.SearchParams{
		Scroll: scrollTimeout,
	}

	what := split.Select.GetWhat()
	if what == nil {
		return nil, nil, fmt.Errorf("not specified columns to query in Select.What")
	}

	var projection []string
	for _, item := range what.GetItems() {
		projection = append(projection, item.GetColumn().Name)
	}

func (*QueryBuilder) BuildInitialSearchQuery(batchSize uint64) (io.Reader, error) {
	query := map[string]any{
		"size": batchSize,
		"query": map[string]any{
			"match_all": make(map[string]any),
		},
	}

	jsonBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("marshal query: %w", err)
	}

	return bytes.NewReader(jsonBytes), nil
}
