package streaming

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service "github.com/ydb-platform/fq-connector-go/api/service"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/clickhouse"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/observation"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ api_service.Connector_ReadSplitsServer = (*streamMock)(nil)

type streamMock struct {
	mock.Mock
	api_service.Connector_ReadSplitsServer
	logger *zap.Logger
}

func (m *streamMock) Context() context.Context {
	args := m.Called()

	return args.Get(0).(context.Context)
}

func (m *streamMock) Send(response *api_service_protos.TReadSplitsResponse) error {
	args := m.Called(response)

	return args.Error(0)
}

func (*streamMock) makeSendMatcher(
	t *testing.T,
	split *api_service_protos.TSplit,
	expectedColumnarBlock [][]any,
	expectedRowCount int,
) func(response *api_service_protos.TReadSplitsResponse) bool {
	return func(response *api_service_protos.TReadSplitsResponse) bool {
		// Check values in data blocks
		buf := bytes.NewBuffer(response.GetArrowIpcStreaming())

		reader, err := ipc.NewReader(buf)
		require.NoError(t, err)

		for reader.Next() {
			record := reader.Record()

			require.Equal(t, len(split.Select.What.Items), len(record.Columns()))

			if record.NumRows() != int64(expectedRowCount) {
				return false
			}

			col0 := record.Column(0).(*array.Int32)
			require.Equal(t, &arrow.Int32Type{}, col0.DataType())

			for i := 0; i < len(expectedColumnarBlock[0]); i++ {
				if expectedColumnarBlock[0][i] != col0.Value(i) {
					return false
				}
			}

			col1 := record.Column(1).(*array.Binary)
			require.Equal(t, &arrow.BinaryType{}, col1.DataType())

			for i := 0; i < len(expectedColumnarBlock[1]); i++ {
				if !bytes.Equal([]byte(expectedColumnarBlock[1][i].(string)), col1.Value(i)) {
					return false
				}
			}
		}

		reader.Release()

		// Check stats
		require.NotNil(t, response.Stats)
		require.Equal(t, uint64(len(expectedColumnarBlock[0])), response.Stats.Rows)

		// TODO: come up with more elegant way of expected data size computing
		var expectedBytes int

		expectedBytes += len(expectedColumnarBlock[0]) * 4 // int32 -> 4 bytes
		for _, val := range expectedColumnarBlock[1] {
			expectedBytes += len(val.(string))
		}

		require.Equal(t, uint64(expectedBytes), response.Stats.Bytes)

		return true
	}
}

type testCaseStreaming struct {
	src                 [][]any
	rowsPerPage         int
	bufferQueueCapacity int
	scanErr             error
	sendErr             error
}

func (tc testCaseStreaming) name() string {
	return fmt.Sprintf(
		"totalRows_%d_rowsPerBlock_%d_bufferQueueCapacity_%d_scanErr_%v_sendErr_%v",
		len(tc.src), tc.rowsPerPage, tc.bufferQueueCapacity, tc.scanErr != nil, tc.sendErr != nil)
}

func (tc testCaseStreaming) messageParams() (sentMessages, rowsInLastMessage int) {
	modulo := len(tc.src) % tc.rowsPerPage

	if modulo == 0 {
		sentMessages = len(tc.src) / tc.rowsPerPage
		rowsInLastMessage = tc.rowsPerPage
	} else {
		sentMessages = len(tc.src)/tc.rowsPerPage + 1
		rowsInLastMessage = modulo
	}

	if tc.scanErr != nil {
		sentMessages--

		rowsInLastMessage = tc.rowsPerPage
	}

	return
}

//nolint:funlen
func (tc testCaseStreaming) execute(t *testing.T) {
	logger := common.NewTestLogger(t)
	split := rdbms_utils.MakeTestSplit()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := &streamMock{logger: logger}

	stream.On("Context").Return(ctx)

	connection := &rdbms_utils.ConnectionMock{}
	connection.On("Logger").Return(logger)
	connection.On("TableName").Return("example_1").Once()
	connection.On("DataSourceInstance").Return(&api_common.TGenericDataSourceInstance{}).Once()

	connectionManager := &rdbms_utils.ConnectionManagerMock{}
	connectionManager.On("Make", split.Select.DataSourceInstance).Return([]rdbms_utils.Connection{connection}, nil).Once()
	connectionManager.On("Release", []rdbms_utils.Connection{connection}).Return().Once()

	rows := &rdbms_utils.RowsMock{
		PredefinedData: tc.src,
	}
	connection.On("Query", `SELECT "col0", "col1" FROM "example_1"`).Return(rows, nil).Once()

	col0Acceptor := new(*int32)
	*col0Acceptor = new(int32)
	col1Acceptor := new(*string)
	*col1Acceptor = new(string)

	transformer := &rdbms_utils.RowTransformerMock{
		Acceptors: []any{
			col0Acceptor,
			col1Acceptor,
		},
	}

	if tc.scanErr == nil {
		rows.On(
			"MakeTransformer",
			[]*Ydb.Column{
				{
					Name: "col0",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col1",
					Type: common.MakePrimitiveType(Ydb.Type_STRING),
				},
			}).Return(transformer, nil).Once()
		rows.On("Next").Return(true).Times(len(rows.PredefinedData))
		rows.On("Next").Return(false).Once()
		rows.On("Scan", transformer.GetAcceptors()...).Return(nil).Times(len(rows.PredefinedData))
		rows.On("Err").Return(nil).Once()
		rows.On("Close").Return(nil).Once()
		rows.On("NextResultSet").Return(false).Once()
	} else {
		rows.On("MakeTransformer",
			[]*Ydb.Column{
				{
					Name: "col0",
					Type: common.MakePrimitiveType(Ydb.Type_INT32),
				},
				{
					Name: "col1",
					Type: common.MakePrimitiveType(Ydb.Type_STRING),
				},
			}).Return(transformer, nil).Once()
		rows.On("Next").Return(true).Times(len(rows.PredefinedData) + 1)
		rows.On("Scan", transformer.GetAcceptors()...).Return(nil).Times(len(rows.PredefinedData))
		// instead of the last message, an error occurs
		rows.On("Scan", transformer.GetAcceptors()...).Return(tc.scanErr).Once()
		rows.On("Err").Return(nil).Once()
		rows.On("Close").Return(nil).Once()
		rows.On("NextResultSet").Return(false).Once()
	}

	totalMessages, rowsInLastMessage := tc.messageParams()

	expectedColumnarBlocks := rdbms_utils.DataConverter{}.RowsToColumnBlocks(rows.PredefinedData, tc.rowsPerPage)

	if tc.sendErr == nil {
		for sendCallID := 0; sendCallID < totalMessages; sendCallID++ {
			expectedColumnarBlock := expectedColumnarBlocks[sendCallID]

			rowsInMessage := tc.rowsPerPage
			if sendCallID == totalMessages-1 {
				rowsInMessage = rowsInLastMessage
			}

			matcher := stream.makeSendMatcher(t, split, expectedColumnarBlock, rowsInMessage)

			stream.On("Send", mock.MatchedBy(matcher)).Return(nil).Once()
		}
	} else {
		// the first attempt to send response is failed
		stream.On("Send", mock.MatchedBy(func(_ *api_service_protos.TReadSplitsResponse) bool {
			cancel() // emulate real behavior of GRPC

			return true
		})).Return(tc.sendErr).Once()
	}

	typeMapper := clickhouse.NewTypeMapper()

	dataSourcePreset := &rdbms.Preset{
		SQLFormatter:      clickhouse.NewSQLFormatter(nil),
		ConnectionManager: connectionManager,
		TypeMapper:        typeMapper,
		RetrierSet:        retry.NewRetrierSetNoop(),
	}

	converterCollection := conversion.NewCollection(&config.TConversionConfig{UseUnsafeConverters: true})

	// TODO: mock
	observationStorage, err := observation.NewStorage(logger, nil)
	require.NoError(t, err)

	dataSource := rdbms.NewDataSource(logger, dataSourcePreset, converterCollection, observationStorage)

	columnarBufferFactory, err := paging.NewColumnarBufferFactory[any](
		logger,
		memory.NewGoAllocator(),
		api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING,
		split.Select.What)
	require.NoError(t, err)

	pagingCfg := &config.TPagingConfig{RowsPerPage: uint64(tc.rowsPerPage)}
	readLimiterFactory := paging.NewReadLimiterFactory(nil)
	readLimiter := readLimiterFactory.MakeReadLimiter(logger)

	sinkFactory := paging.NewSinkFactory(ctx, logger, pagingCfg, columnarBufferFactory, readLimiter)

	request := &api_service_protos.TReadSplitsRequest{}
	streamer := NewReadSplitsStreamer(logger, "test-query-id", stream, request, split, sinkFactory, dataSource)

	err = streamer.Run()

	switch {
	case tc.scanErr != nil:
		require.True(t, errors.Is(err, tc.scanErr))
	case tc.sendErr != nil:
		require.True(t, errors.Is(err, tc.sendErr))
	default:
		require.NoError(t, err)
	}

	mocks := []any{stream, connectionManager, connection, transformer}

	mock.AssertExpectationsForObjects(t, mocks...)
}

func TestStreaming(t *testing.T) {
	srcValues := [][][]any{
		{
			{int32(1), "a"},
			{int32(2), "b"},
			{int32(3), "c"},
			{int32(4), "d"},
		},
		{
			{int32(1), "a"},
			{int32(2), "b"},
			{int32(3), "c"},
			{int32(4), "d"},
			{int32(5), "e"},
		},
	}
	rowsPerBlockValues := []int{2}
	bufferQueueCapacityValues := []int{
		0,
		1,
		10,
	}

	var testCases []testCaseStreaming

	for _, src := range srcValues {
		for _, rowsPerBlock := range rowsPerBlockValues {
			for _, bufferQueueCapacity := range bufferQueueCapacityValues {
				tc := testCaseStreaming{
					src:                 src,
					rowsPerPage:         rowsPerBlock,
					bufferQueueCapacity: bufferQueueCapacity,
				}

				testCases = append(testCases, tc)
			}
		}
	}

	t.Run("positive", func(t *testing.T) {
		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name(), func(t *testing.T) {
				tc.execute(t)
			})
		}
	})

	t.Run("scan error", func(t *testing.T) {
		scanErr := fmt.Errorf("scan error")

		for _, tc := range testCases {
			tc := tc
			tc.scanErr = scanErr
			t.Run(tc.name(), func(t *testing.T) {
				tc.execute(t)
			})
		}
	})

	t.Run("send error", func(t *testing.T) {
		sendErr := fmt.Errorf("stream send error")

		for _, tc := range testCases {
			tc := tc
			tc.sendErr = sendErr
			t.Run(tc.name(), func(t *testing.T) {
				tc.execute(t)
			})
		}
	})
}
