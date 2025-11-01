package ydb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	ydb "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

//nolint:lll
func TestMakeSelectQuery(t *testing.T) {
	type testCase struct {
		testName       string
		selectReq      *api_service_protos.TSelect
		outputQuery    string
		outputArgs     []any
		outputYdbTypes []*ydb.Type
		err            error
	}

	logger := common.NewTestLogger(t)
	formatter := NewSQLFormatter(
		config.TYdbConfig_MODE_TABLE_SERVICE_STDLIB_SCAN_QUERIES,
		&config.TPushdownConfig{
			EnableTimestampPushdown: true,
		},
	)

	tcs := []testCase{
		{
			testName: "empty_table_name",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "",
				},
				What: &api_service_protos.TSelect_TWhat{},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "",
			outputArgs:     nil,
			outputYdbTypes: nil,
			err:            common.ErrEmptyTableName,
		},
		{
			testName: "empty_no columns",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: &api_service_protos.TSelect_TWhat{},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT 0 FROM `tab`",
			outputArgs:     []any{},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT64)}, // special case for empty select
			err:            nil,
		},
		{
			testName: "select_col",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: &api_service_protos.TSelect_TWhat{
					Items: []*api_service_protos.TSelect_TWhat_TItem{
						{
							Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
								Column: &ydb.Column{
									Name: "col",
									Type: common.MakePrimitiveType(ydb.Type_INT32),
								},
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT `col` FROM `tab`",
			outputArgs:     []any{},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32)},
			err:            nil,
		},
		{
			testName: "is_null",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: rdbms_utils.NewDefaultWhat(),
				Where: &api_service_protos.TSelect_TWhere{
					FilterTyped: &api_service_protos.TPredicate{
						Payload: &api_service_protos.TPredicate_IsNull{
							IsNull: &api_service_protos.TPredicate_TIsNull{
								Value: rdbms_utils.NewColumnExpression("col1"),
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT `col0`, `col1` FROM `tab` WHERE (`col1` IS NULL)",
			outputArgs:     []any{},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:            nil,
		},
		{
			testName: "is_not_null",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: rdbms_utils.NewDefaultWhat(),
				Where: &api_service_protos.TSelect_TWhere{
					FilterTyped: &api_service_protos.TPredicate{
						Payload: &api_service_protos.TPredicate_IsNotNull{
							IsNotNull: &api_service_protos.TPredicate_TIsNotNull{
								Value: rdbms_utils.NewColumnExpression("col2"),
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT `col0`, `col1` FROM `tab` WHERE (`col2` IS NOT NULL)",
			outputArgs:     []any{},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:            nil,
		},
		{
			testName: "bool_column",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: rdbms_utils.NewDefaultWhat(),
				Where: &api_service_protos.TSelect_TWhere{
					FilterTyped: &api_service_protos.TPredicate{
						Payload: &api_service_protos.TPredicate_BoolExpression{
							BoolExpression: &api_service_protos.TPredicate_TBoolExpression{
								Value: rdbms_utils.NewColumnExpression("col2"),
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT `col0`, `col1` FROM `tab` WHERE `col2`",
			outputArgs:     []any{},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:            nil,
		},
		{
			testName: "complex_filter",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: rdbms_utils.NewDefaultWhat(),
				Where: &api_service_protos.TSelect_TWhere{
					FilterTyped: &api_service_protos.TPredicate{
						Payload: &api_service_protos.TPredicate_Disjunction{
							Disjunction: &api_service_protos.TPredicate_TDisjunction{
								Operands: []*api_service_protos.TPredicate{
									{
										Payload: &api_service_protos.TPredicate_Negation{
											Negation: &api_service_protos.TPredicate_TNegation{
												Operand: &api_service_protos.TPredicate{
													Payload: &api_service_protos.TPredicate_Comparison{
														Comparison: &api_service_protos.TPredicate_TComparison{
															Operation:  api_service_protos.TPredicate_TComparison_LE,
															LeftValue:  rdbms_utils.NewColumnExpression("col2"),
															RightValue: rdbms_utils.NewInt32ValueExpression(42),
														},
													},
												},
											},
										},
									},
									{
										Payload: &api_service_protos.TPredicate_Conjunction{
											Conjunction: &api_service_protos.TPredicate_TConjunction{
												Operands: []*api_service_protos.TPredicate{
													{
														Payload: &api_service_protos.TPredicate_Comparison{
															Comparison: &api_service_protos.TPredicate_TComparison{
																Operation:  api_service_protos.TPredicate_TComparison_NE,
																LeftValue:  rdbms_utils.NewColumnExpression("col1"),
																RightValue: rdbms_utils.NewInt64ValueExpression(0),
															},
														},
													},
													{
														Payload: &api_service_protos.TPredicate_IsNull{
															IsNull: &api_service_protos.TPredicate_TIsNull{
																Value: rdbms_utils.NewColumnExpression("col3"),
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT `col0`, `col1` FROM `tab` WHERE ((NOT (`col2` <= ?)) OR ((`col1` <> ?) AND (`col3` IS NULL)))",
			outputArgs:     []any{int32(42), int64(0)},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:            nil,
		},
		{
			testName: "unsupported_predicate",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: rdbms_utils.NewDefaultWhat(),
				Where: &api_service_protos.TSelect_TWhere{
					FilterTyped: &api_service_protos.TPredicate{
						Payload: &api_service_protos.TPredicate_Between{
							Between: &api_service_protos.TPredicate_TBetween{
								Value:    rdbms_utils.NewColumnExpression("col2"),
								Least:    rdbms_utils.NewColumnExpression("col1"),
								Greatest: rdbms_utils.NewColumnExpression("col3"),
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT `col0`, `col1` FROM `tab`",
			outputArgs:     []any{},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:            nil,
		},
		{
			testName: "unsupported_type",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: rdbms_utils.NewDefaultWhat(),
				Where: &api_service_protos.TSelect_TWhere{
					FilterTyped: &api_service_protos.TPredicate{
						Payload: &api_service_protos.TPredicate_Comparison{
							Comparison: &api_service_protos.TPredicate_TComparison{
								Operation: api_service_protos.TPredicate_TComparison_EQ,
								LeftValue: rdbms_utils.NewColumnExpression("col2"),
								// I don't know what is this, but it's unsupported
								RightValue: rdbms_utils.NewNestedValueExpression("text"),
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT `col0`, `col1` FROM `tab`",
			outputArgs:     []any{},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:            nil,
		},
		{
			testName: "partial_filter_removes_and",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: rdbms_utils.NewDefaultWhat(),
				Where: &api_service_protos.TSelect_TWhere{
					FilterTyped: &api_service_protos.TPredicate{
						Payload: &api_service_protos.TPredicate_Conjunction{
							Conjunction: &api_service_protos.TPredicate_TConjunction{
								Operands: []*api_service_protos.TPredicate{
									{
										Payload: &api_service_protos.TPredicate_Comparison{
											Comparison: &api_service_protos.TPredicate_TComparison{
												Operation:  api_service_protos.TPredicate_TComparison_EQ,
												LeftValue:  rdbms_utils.NewColumnExpression("col1"),
												RightValue: rdbms_utils.NewInt32ValueExpression(32),
											},
										},
									},
									{
										// Not supported
										Payload: &api_service_protos.TPredicate_Comparison{
											Comparison: &api_service_protos.TPredicate_TComparison{
												Operation: api_service_protos.TPredicate_TComparison_EQ,
												LeftValue: rdbms_utils.NewColumnExpression("col2"),
												// I don't know what is this, but it's unsupported
												RightValue: rdbms_utils.NewNestedValueExpression("value"),
											},
										},
									},
								},
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT `col0`, `col1` FROM `tab` WHERE (`col1` = ?)",
			outputArgs:     []any{int32(32)},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:            nil,
		},
		{
			testName: "partial_filter",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: rdbms_utils.NewDefaultWhat(),
				Where: &api_service_protos.TSelect_TWhere{
					FilterTyped: &api_service_protos.TPredicate{
						Payload: &api_service_protos.TPredicate_Conjunction{
							Conjunction: &api_service_protos.TPredicate_TConjunction{
								Operands: []*api_service_protos.TPredicate{
									{
										Payload: &api_service_protos.TPredicate_Comparison{
											Comparison: &api_service_protos.TPredicate_TComparison{
												Operation:  api_service_protos.TPredicate_TComparison_EQ,
												LeftValue:  rdbms_utils.NewColumnExpression("col1"),
												RightValue: rdbms_utils.NewInt32ValueExpression(32),
											},
										},
									},
									{
										// Not supported
										Payload: &api_service_protos.TPredicate_Comparison{
											Comparison: &api_service_protos.TPredicate_TComparison{
												Operation: api_service_protos.TPredicate_TComparison_EQ,
												LeftValue: rdbms_utils.NewColumnExpression("col2"),
												// I don't know what is this, but it's unsupported
												RightValue: rdbms_utils.NewNestedValueExpression("text"),
											},
										},
									},
									{
										Payload: &api_service_protos.TPredicate_IsNull{
											IsNull: &api_service_protos.TPredicate_TIsNull{
												Value: rdbms_utils.NewColumnExpression("col3"),
											},
										},
									},
									{
										Payload: &api_service_protos.TPredicate_IsNotNull{
											IsNotNull: &api_service_protos.TPredicate_TIsNotNull{
												Value: rdbms_utils.NewColumnExpression("col4"),
											},
										},
									},
								},
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT `col0`, `col1` FROM `tab` WHERE ((`col1` = ?) AND (`col3` IS NULL) AND (`col4` IS NOT NULL))",
			outputArgs:     []any{int32(32)},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:            nil,
		},
		{
			testName: "negative_sql_injection_by_table",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: `information_schema.columns; DROP TABLE information_schema.columns`,
				},
				What: &api_service_protos.TSelect_TWhat{},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT 0 FROM `information_schema.columns; DROP TABLE information_schema.columns`",
			outputArgs:     []any{},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT64)},
			err:            nil,
		},
		{
			testName: "negative_sql_injection_by_col",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: &api_service_protos.TSelect_TWhat{
					Items: []*api_service_protos.TSelect_TWhat_TItem{
						{
							Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
								Column: &ydb.Column{
									Name: `0; DROP TABLE information_schema.columns`,
									Type: common.MakePrimitiveType(ydb.Type_INT32),
								},
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT `0; DROP TABLE information_schema.columns` FROM `tab`",
			outputArgs:     []any{},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32)},
			err:            nil,
		},
		{
			testName: "negative_sql_injection_fake_quotes",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: &api_service_protos.TSelect_TWhat{
					Items: []*api_service_protos.TSelect_TWhat_TItem{
						{
							Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
								Column: &ydb.Column{
									Name: "0`; DROP TABLE information_schema.columns;",
									Type: common.MakePrimitiveType(ydb.Type_INT32),
								},
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_YDB,
				},
			},
			outputQuery:    "SELECT `0`; DROP TABLE information_schema.columns;` FROM `tab`",
			outputArgs:     []any{},
			outputYdbTypes: []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32)},
			err:            nil,
		},
		//nolint:revive
		{
			testName: "pushdown_coalesce",
			selectReq: rdbms_utils.MustTSelectFromLoggerOutput(
				"{\"table\":\"pushdown_coalesce\",\"object_key\":\"\"}",
				"{\"items\":[{\"column\":{\"name\":\"col_01_timestamp\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"TIMESTAMP\"}}}}},{\"column\":{\"name\":\"id\",\"type\":{\"type_id\":\"INT32\"}}}]}",
				"{\"filter_typed\":{\"conjunction\":{\"operands\":[{\"coalesce\":{\"operands\":[{\"comparison\":{\"operation\":\"GE\",\"left_value\":{\"column\":\"col_01_timestamp\"},\"right_value\":{\"typed_value\":{\"type\":{\"type_id\":\"TIMESTAMP\"},\"value\":{\"int64_value\":\"1609459200000000\",\"items\":[],\"pairs\":[],\"variant_index\":0,\"high_128\":\"0\"}}}}},{\"bool_expression\":{\"value\":{\"typed_value\":{\"type\":{\"type_id\":\"BOOL\"},\"value\":{\"bool_value\":false,\"items\":[],\"pairs\":[],\"variant_index\":0,\"high_128\":\"0\"}}}}}]}},{\"coalesce\":{\"operands\":[{\"comparison\":{\"operation\":\"LE\",\"left_value\":{\"column\":\"col_01_timestamp\"},\"right_value\":{\"typed_value\":{\"type\":{\"type_id\":\"TIMESTAMP\"},\"value\":{\"int64_value\":\"1704067200000000\",\"items\":[],\"pairs\":[],\"variant_index\":0,\"high_128\":\"0\"}}}}},{\"bool_expression\":{\"value\":{\"typed_value\":{\"type\":{\"type_id\":\"BOOL\"},\"value\":{\"bool_value\":false,\"items\":[],\"pairs\":[],\"variant_index\":0,\"high_128\":\"0\"}}}}}]}}]}}}",
				api_common.EGenericDataSourceKind_YDB,
			),
			outputQuery: "SELECT `col_01_timestamp`, `id` FROM `pushdown_coalesce` WHERE (COALESCE((`col_01_timestamp` >= ?), ?) AND COALESCE((`col_01_timestamp` <= ?), ?))",
			outputArgs:  []any{time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), false, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), false},
			outputYdbTypes: []*ydb.Type{
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_TIMESTAMP)),
				common.MakePrimitiveType(ydb.Type_INT32),
			},
			err: nil,
		},
		//nolint:revive
		{
			testName: "YQ-4194",
			selectReq: rdbms_utils.MustTSelectFromLoggerOutput(
				"{\"table\":\"db\", \"object_key\":\"\"}",
				"{\"items\":[{\"column\":{\"name\":\"a\", \"type\":{\"type_id\":\"INT32\"}}}, {\"column\":{\"name\":\"b\", \"type\":{\"type_id\":\"STRING\"}}}, {\"column\":{\"name\":\"c\", \"type\":{\"optional_type\":{\"item\":{\"type_id\":\"INT32\"}}}}}, {\"column\":{\"name\":\"d\", \"type\":{\"optional_type\":{\"item\":{\"type_id\":\"INT32\"}}}}}, {\"column\":{\"name\":\"e\", \"type\":{\"optional_type\":{\"item\":{\"type_id\":\"INT32\"}}}}}, {\"column\":{\"name\":\"f\", \"type\":{\"optional_type\":{\"item\":{\"type_id\":\"INT32\"}}}}}]}",
				"{\"filter_typed\":{\"disjunction\":{\"operands\":[{\"conjunction\":{\"operands\":[{\"comparison\":{\"operation\":\"EQ\", \"left_value\":{\"column\":\"a\"}, \"right_value\":{\"typed_value\":{\"type\":{\"type_id\":\"INT32\"}, \"value\":{\"int32_value\":7, \"items\":[], \"pairs\":[], \"variant_index\":0, \"high_128\":\"0\"}}}}}, {\"comparison\":{\"operation\":\"EQ\", \"left_value\":{\"column\":\"b\"}, \"right_value\":{\"typed_value\":{\"type\":{\"type_id\":\"STRING\"}, \"value\":{\"bytes_value\":\"OA==\", \"items\":[], \"pairs\":[], \"variant_index\":0, \"high_128\":\"0\"}}}}}]}}, {\"conjunction\":{\"operands\":[{\"comparison\":{\"operation\":\"EQ\", \"left_value\":{\"column\":\"a\"}, \"right_value\":{\"typed_value\":{\"type\":{\"type_id\":\"INT32\"}, \"value\":{\"int32_value\":1, \"items\":[], \"pairs\":[], \"variant_index\":0, \"high_128\":\"0\"}}}}}, {\"comparison\":{\"operation\":\"EQ\", \"left_value\":{\"column\":\"b\"}, \"right_value\":{\"typed_value\":{\"type\":{\"type_id\":\"STRING\"}, \"value\":{\"bytes_value\":\"Mg==\", \"items\":[], \"pairs\":[], \"variant_index\":0, \"high_128\":\"0\"}}}}}]}}, {\"conjunction\":{\"operands\":[{\"comparison\":{\"operation\":\"EQ\", \"left_value\":{\"column\":\"a\"}, \"right_value\":{\"typed_value\":{\"type\":{\"type_id\":\"INT32\"}, \"value\":{\"int32_value\":2, \"items\":[], \"pairs\":[], \"variant_index\":0, \"high_128\":\"0\"}}}}}, {\"comparison\":{\"operation\":\"EQ\", \"left_value\":{\"column\":\"b\"}, \"right_value\":{\"typed_value\":{\"type\":{\"type_id\":\"STRING\"}, \"value\":{\"bytes_value\":\"MQ==\", \"items\":[], \"pairs\":[], \"variant_index\":0, \"high_128\":\"0\"}}}}}]}}]}}}",
				api_common.EGenericDataSourceKind_YDB,
			),
			outputQuery: "SELECT `a`, `b`, `c`, `d`, `e`, `f` FROM `db` WHERE (((`a` = ?) AND (`b` = ?)) OR ((`a` = ?) AND (`b` = ?)) OR ((`a` = ?) AND (`b` = ?)))",
			outputArgs:  []any{int32(7), []byte("8"), int32(1), []byte("2"), int32(2), []byte("1")},
			outputYdbTypes: []*ydb.Type{
				common.MakePrimitiveType(ydb.Type_INT32),
				common.MakePrimitiveType(ydb.Type_STRING),
			},
			err: nil,
		},
	}

	for _, tc := range tcs {
		tc := tc

		t.Run(tc.testName, func(t *testing.T) {
			splitDescription := &TSplitDescription{
				Payload: &TSplitDescription_DataShard{
					DataShard: &TSplitDescription_TDataShard{},
				},
			}

			splitDescriptionBytes, err := protojson.Marshal(splitDescription)
			require.NoError(t, err)

			readSplitsQuery, err := rdbms_utils.MakeSelectQuery(
				context.Background(),
				logger,
				formatter,
				&api_service_protos.TSplit{
					Select: tc.selectReq,
					Payload: &api_service_protos.TSplit_Description{
						Description: splitDescriptionBytes,
					},
				},
				api_service_protos.TReadSplitsRequest_FILTERING_OPTIONAL,
				tc.selectReq.From.Table,
			)
			if tc.err != nil {
				require.True(t, errors.Is(err, tc.err))

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.outputQuery, readSplitsQuery.QueryText)
			require.Equal(t, tc.outputArgs, readSplitsQuery.QueryArgs.Values())

			for i, ydbType := range tc.outputYdbTypes {
				require.Equal(
					t, ydbType, readSplitsQuery.YdbColumns[i].Type,
					fmt.Sprintf("unequal types at index %d: expected=%v, actual=%v", i, ydbType, readSplitsQuery.YdbColumns[i].Type),
				)
			}
		})
	}
}
