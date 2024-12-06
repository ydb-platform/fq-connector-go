package utils

import (
	"context"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
)

var _ Connection = (*ConnectionMock)(nil)

type ConnectionMock struct {
	mock.Mock
}

func (m *ConnectionMock) Query(params *QueryParams) (Rows, error) {
	called := []any{params.QueryText}
	called = append(called, params.QueryArgs.Values()...)
	args := m.Called(called...)

	return args.Get(0).(Rows), args.Error(1)
}

func (m *ConnectionMock) Close() error {
	return m.Called().Error(0)
}

type ConnectionManagerMock struct {
	mock.Mock
}

func (m *ConnectionManagerMock) Make(
	params *ConnectionParamsMakeParams,
) (Connection, error) {
	args := m.Called(params.DataSourceInstance)

	return args.Get(0).(Connection), args.Error(1)
}

func (m *ConnectionManagerMock) Release(_ context.Context, _ *zap.Logger, conn Connection) {
	m.Called(conn)
}

var _ Rows = (*RowsMock)(nil)

type RowsMock struct {
	mock.Mock
	PredefinedData [][]any
	scanCalls      int
}

func (m *RowsMock) Close() error {
	return m.Called().Error(0)
}

func (m *RowsMock) Err() error {
	return m.Called().Error(0)
}

func (m *RowsMock) Next() bool {
	return m.Called().Bool(0)
}

func (m *RowsMock) NextResultSet() bool {
	return m.Called().Bool(0)
}

func (m *RowsMock) Scan(dest ...any) error {
	args := m.Called(dest...)

	// mutate acceptors by reference
	if m.scanCalls < len(m.PredefinedData) {
		row := m.PredefinedData[m.scanCalls]

		for i, d := range dest {
			switch t := d.(type) {
			case **int32:
				**t = row[i].(int32)
			case **string:
				**t = row[i].(string)
			}
		}

		m.scanCalls++
	}

	return args.Error(0)
}

func (m *RowsMock) MakeTransformer(ydbType []*Ydb.Type, _ conversion.Collection) (paging.RowTransformer[any], error) {
	args := m.Called(ydbType)

	return args.Get(0).(*RowTransformerMock), args.Error(1)
}

var _ paging.RowTransformer[any] = (*RowTransformerMock)(nil)

type RowTransformerMock struct {
	mock.Mock
	Acceptors []any
}

func (t *RowTransformerMock) GetAcceptors() []any { return t.Acceptors }

func (*RowTransformerMock) SetAcceptors([]any) {
	panic("not implemented")
}

func (t *RowTransformerMock) AppendToArrowBuilders(builder []array.Builder) error {
	builder[0].(*array.Int32Builder).Append(**t.Acceptors[0].(**int32))

	cast := **t.Acceptors[1].(**string)
	builder[1].(*array.BinaryBuilder).Append([]byte(cast))

	return nil
}
