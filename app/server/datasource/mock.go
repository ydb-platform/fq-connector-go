package datasource

import (
	"context"

	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
)

var _ DataSource[any] = (*DataSourceMock[any])(nil)

//nolint:revive
type DataSourceMock[T paging.Acceptor] struct {
	mock.Mock
}

func (*DataSourceMock[T]) DescribeTable(
	_ context.Context,
	_ *zap.Logger,
	_ *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (*DataSourceMock[T]) ListSplits(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
) (<-chan *ListSplitResult, error) {
	panic("not implemented") // TODO: Implement
}

func (m *DataSourceMock[T]) ReadSplit(
	_ context.Context,
	_ *zap.Logger,
	_ *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sinkFactory paging.SinkFactory[T],
) error {
	return m.Called(split, sinkFactory).Error(0)
}
