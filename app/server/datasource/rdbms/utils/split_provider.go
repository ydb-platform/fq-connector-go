package utils

import (
	"context"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"go.uber.org/zap"
)

var _ SplitProvider = (*defaultSplitProvider)(nil)

type defaultSplitProvider struct{}

func (p *defaultSplitProvider) ListSplits(
	ctx context.Context,
	logger *zap.Logger,
	conn Connection,
	request *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
) (<-chan *datasource.ListSplitResult, error) {
	resultChan := make(chan *datasource.ListSplitResult, 1)

	// By default we deny table splitting
	resultChan <- &datasource.ListSplitResult{
		Slct:        slct,
		Description: []byte{},
		Error:       nil,
	}

	close(resultChan)

	return resultChan, nil
}

func NewDefaultSplitProvider() SplitProvider {
	return &defaultSplitProvider{}
}
