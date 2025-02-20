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
	ctx context.Context,
	_ *zap.Logger,
	_ Connection,
	_ *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult) error {

	// By default we deny table splitting
	select {
	case resultChan <- &datasource.ListSplitResult{Slct: slct, Description: []byte{}}:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func NewDefaultSplitProvider() SplitProvider {
	return &defaultSplitProvider{}
}
