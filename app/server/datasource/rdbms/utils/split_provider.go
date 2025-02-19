package utils

import (
	"context"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
)

var _ SplitProvider = (*defaultSplitProvider)(nil)

type defaultSplitProvider struct{}

func (defaultSplitProvider) ListSplits(
	_ context.Context,
	_ *zap.Logger,
	_ Connection,
	_ *api_service_protos.TListSplitsRequest,
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
