package paging

import (
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

var _ Sink[any] = (*SinkMock)(nil)

type SinkMock struct {
	mock.Mock
}

func (m *SinkMock) AddRow(transformer RowTransformer[any]) error {
	args := m.Called(transformer)

	return args.Error(0)
}

func (m *SinkMock) AddArrowRecord(record arrow.Record) error {
	args := m.Called(record)

	return args.Error(0)
}

func (m *SinkMock) AddError(err error) {
	m.Called(err)
}

func (m *SinkMock) Finish() {
	m.Called()
}

func (m *SinkMock) ResultQueue() <-chan *ReadResult[any] {
	return m.Called().Get(0).(chan *ReadResult[any])
}

func (m *SinkMock) Logger() *zap.Logger {
	return m.Called().Get(0).(*zap.Logger)
}

var _ SinkFactory[any] = (*SinkFactoryMock)(nil)

type SinkFactoryMock struct {
	mock.Mock
}

func (m *SinkFactoryMock) MakeSinks(params []*SinkParams) ([]Sink[any], error) {
	args := m.Called(params)

	return args.Get(0).([]Sink[any]), args.Error(1)
}

func (m *SinkFactoryMock) ResultQueue() <-chan *ReadResult[any] {
	return m.Called().Get(0).(chan *ReadResult[any])
}

func (m *SinkFactoryMock) FinalStats() *api_service_protos.TReadSplitsResponse_TStats {
	return m.Called().Get(0).(*api_service_protos.TReadSplitsResponse_TStats)
}

var _ ColumnarBuffer[any] = (*ColumnarBufferMock)(nil)

type ColumnarBufferMock struct {
	mock.Mock
}

//nolint:unused
func (*ColumnarBufferMock) addRow(_ RowTransformer[any]) error {
	panic("not implemented") // TODO: Implement
}

func (*ColumnarBufferMock) addArrowRecord(_ arrow.Record) error {
	panic("not implemented") // TODO: Implement
}

func (m *ColumnarBufferMock) ToResponse() (*api_service_protos.TReadSplitsResponse, error) {
	args := m.Called()

	return args.Get(0).(*api_service_protos.TReadSplitsResponse), args.Error(1)
}

func (m *ColumnarBufferMock) Release() {
	m.Called()
}

func (*ColumnarBufferMock) TotalRows() int {
	panic("not implemented") // TODO: Implement
}
