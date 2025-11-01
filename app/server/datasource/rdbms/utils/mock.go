package utils //nolint:revive

import (
	"context"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
)

var _ Connection = (*ConnectionMock)(nil)

type ConnectionMock struct {
	mock.Mock
}

func (m *ConnectionMock) Query(params *QueryParams) (*QueryResult, error) {
	called := []any{params.QueryText}

	called = append(called, params.QueryArgs.Values()...)

	args := m.Called(called...)

	rows := args.Get(0)
	if rows == nil {
		return nil, args.Error(1)
	}

	return &QueryResult{
		Rows: rows.(Rows),
	}, args.Error(1)
}

func (m *ConnectionMock) Close() error {
	return m.Called().Error(0)
}

// DataSourceInstance comprehensively describing the target of the connection
func (m *ConnectionMock) DataSourceInstance() *api_common.TGenericDataSourceInstance {
	return m.Called().Get(0).(*api_common.TGenericDataSourceInstance)
}

// The name of a table that will be read via this connection.
func (m *ConnectionMock) TableName() string {
	return m.Called().String(0)
}

func (m *ConnectionMock) Logger() *zap.Logger {
	return m.Called().Get(0).(*zap.Logger)
}

type ConnectionManagerMock struct {
	mock.Mock
}

func (m *ConnectionManagerMock) Make(
	params *ConnectionParams,
) ([]Connection, error) {
	args := m.Called(params.DataSourceInstance)

	return args.Get(0).([]Connection), args.Error(1)
}

func (m *ConnectionManagerMock) Release(_ context.Context, _ *zap.Logger, cs []Connection) {
	m.Called(cs)
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

func (m *RowsMock) MakeTransformer(columns []*Ydb.Column, _ conversion.Collection) (paging.RowTransformer[any], error) {
	args := m.Called(columns)

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

func (t *RowTransformerMock) AppendToArrowBuilders(_ *arrow.Schema, builder []array.Builder) error {
	builder[0].(*array.Int32Builder).Append(**t.Acceptors[0].(**int32))

	cast := **t.Acceptors[1].(**string)
	builder[1].(*array.BinaryBuilder).Append([]byte(cast))

	return nil
}
