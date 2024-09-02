package rdbms

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/postgresql"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

func TestReadSplit(t *testing.T) {
	ctx := context.Background()
	split := &api_service_protos.TSplit{
		Select: &api_service_protos.TSelect{
			DataSourceInstance: &api_common.TDataSourceInstance{},
			What: &api_service_protos.TSelect_TWhat{
				Items: []*api_service_protos.TSelect_TWhat_TItem{
					{
						Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
							Column: &Ydb.Column{
								Name: "col1",
								Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
							},
						},
					},
					{
						Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
							Column: &Ydb.Column{
								Name: "col2",
								Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
							},
						},
					},
				},
			},
			From: &api_service_protos.TSelect_TFrom{
				Table: "example_1",
			},
		},
	}
	converterCollection := conversion.NewCollection(&config.TConversionConfig{UseUnsafeConverters: true})

	t.Run("positive", func(t *testing.T) {
		logger := common.NewTestLogger(t)

		connectionManager := &rdbms_utils.ConnectionManagerMock{}

		preset := &Preset{
			ConnectionManager: connectionManager,
			SQLFormatter:      postgresql.NewSQLFormatter(), // TODO: parametrize
			RetrierSet:        retry.NewRetrierSetNoop(),
		}

		connection := &rdbms_utils.ConnectionMock{}
		connectionManager.On("Make", split.Select.DataSourceInstance).Return(connection, nil).Once()
		connectionManager.On("Release", connection).Return().Once()

		rows := &rdbms_utils.RowsMock{
			PredefinedData: [][]any{
				{int32(1), "a"},
				{int32(2), "b"},
			},
		}
		connection.On("Query", `SELECT "col1", "col2" FROM "example_1"`).Return(rows, nil).Once()

		transformer := &rdbms_utils.RowTransformerMock{
			Acceptors: []any{
				new(int32),
				new(string),
			},
		}

		rows.On("MakeTransformer",
			[]*Ydb.Type{common.MakePrimitiveType(Ydb.Type_INT32), common.MakePrimitiveType(Ydb.Type_UTF8)},
		).Return(transformer, nil).Once()
		rows.On("Next").Return(true).Times(2)
		rows.On("Next").Return(false).Once()
		rows.On("Scan", transformer.GetAcceptors()...).Return(nil).Times(2)
		rows.On("Err").Return(nil).Once()
		rows.On("NextResultSet").Return(false).Once()
		rows.On("Close").Return(nil).Once()

		sink := &paging.SinkMock{}
		sink.On("AddRow", transformer).Return(nil).Times(2)
		sink.On("Finish").Return().Once()

		dataSource := NewDataSource(logger, preset, converterCollection)
		dataSource.ReadSplit(ctx, logger, split, sink)

		mock.AssertExpectationsForObjects(t, connectionManager, connection, rows, sink)
	})

	t.Run("scan error", func(t *testing.T) {
		logger := common.NewTestLogger(t)
		connectionManager := &rdbms_utils.ConnectionManagerMock{}

		preset := &Preset{
			ConnectionManager: connectionManager,
			SQLFormatter:      postgresql.NewSQLFormatter(), // TODO: parametrize
			RetrierSet:        retry.NewRetrierSetNoop(),
		}

		connection := &rdbms_utils.ConnectionMock{}
		connectionManager.On("Make", split.Select.DataSourceInstance).Return(connection, nil).Once()
		connectionManager.On("Release", connection).Return().Once()

		rows := &rdbms_utils.RowsMock{
			PredefinedData: [][]any{
				{int32(1), "a"},
				{int32(2), "b"},
			},
		}
		connection.On("Query", `SELECT "col1", "col2" FROM "example_1"`).Return(rows, nil).Once()

		transformer := &rdbms_utils.RowTransformerMock{
			Acceptors: []any{
				new(int32),
				new(string),
			},
		}

		scanErr := fmt.Errorf("scan failed")

		rows.On("MakeTransformer",
			[]*Ydb.Type{
				common.MakePrimitiveType(Ydb.Type_INT32),
				common.MakePrimitiveType(Ydb.Type_UTF8),
			},
		).Return(transformer, nil).Once()
		rows.On("Next").Return(true).Times(2)
		rows.On("Scan", transformer.GetAcceptors()...).Return(nil).Once()
		rows.On("Scan", transformer.GetAcceptors()...).Return(scanErr).Once()
		rows.On("Close").Return(nil).Once()

		sink := &paging.SinkMock{}
		sink.On("AddRow", transformer).Return(nil).Once()
		sink.On("AddError", mock.MatchedBy(func(err error) bool {
			return errors.Is(err, scanErr)
		})).Return().Once()
		sink.On("Finish").Return().Once()

		datasource := NewDataSource(logger, preset, converterCollection)
		datasource.ReadSplit(ctx, logger, split, sink)

		mock.AssertExpectationsForObjects(t, connectionManager, connection, rows, sink)
	})
}
