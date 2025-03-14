package utils

import (
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
)

var _ SplitProvider = (*defaultSplitProvider)(nil)

type defaultSplitProvider struct{}

func (defaultSplitProvider) ListSplits(
	params *ListSplitsParams,
) error {
	// By default we deny table splitting
	select {
	case params.ResultChan <- &datasource.ListSplitResult{Slct: params.Select, Description: nil}:
	case <-params.Ctx.Done():
		return params.Ctx.Err()
	}

	return nil
}

func NewDefaultSplitProvider() SplitProvider {
	return &defaultSplitProvider{}
}
