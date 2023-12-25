package datasource

import (
	"context"

	"github.com/stretchr/testify/mock"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"go.uber.org/zap"
)

var _ DataSource[any] = (*DataSourceMock[any])(nil)

type DataSourceMock[T utils.Acceptor] struct {
	mock.Mock
}

func (m *DataSourceMock[T]) DescribeTable(
	_ context.Context,
	_ *zap.Logger,
	_ *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *DataSourceMock[T]) ReadSplit(
	_ context.Context,
	_ *zap.Logger,
	split *api_service_protos.TSplit,
	pagingWriter paging.Sink[T],
) {
	m.Called(split, pagingWriter)
}

func (m *DataSourceMock[T]) TypeMapper() utils.TypeMapper {
	panic("not implemented") // TODO: Implement
}
