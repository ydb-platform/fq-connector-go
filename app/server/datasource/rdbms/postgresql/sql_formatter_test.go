package postgresql

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/wrapperspb"

	ydb "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

func TestMakeSelectQuery(t *testing.T) {
	type testCase struct {
		testName         string
		selectReq        *api_service_protos.TSelect
		splitDescription *TSplitDescription
		outputQuery      string
		outputArgs       []any
		outputYdbTypes   []*ydb.Type
		err              error
	}

	logger := common.NewTestLogger(t)
	formatter := NewSQLFormatter(nil)
	singleSplit := &TSplitDescription{Payload: &TSplitDescription_Single{}}

	tcs := []testCase{
		{
			testName: "empty_table_name",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "",
				},
				What: &api_service_protos.TSelect_TWhat{},
			},
			splitDescription: singleSplit,
			outputQuery:      "",
			outputArgs:       nil,
			outputYdbTypes:   nil,
			err:              common.ErrEmptyTableName,
		},
		{
			testName: "empty_no columns",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: "tab",
				},
				What: &api_service_protos.TSelect_TWhat{},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT 0 FROM "tab"`,
			outputArgs:       []any{},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT64)}, // special case for empty select
			err:              nil,
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
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT "col" FROM "tab"`,
			outputArgs:       []any{},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32)},
			err:              nil,
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
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT "col0", "col1" FROM "tab" WHERE ("col1" IS NULL)`,
			outputArgs:       []any{},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:              nil,
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
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT "col0", "col1" FROM "tab" WHERE ("col2" IS NOT NULL)`,
			outputArgs:       []any{},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:              nil,
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
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT "col0", "col1" FROM "tab" WHERE "col2"`,
			outputArgs:       []any{},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:              nil,
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
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT "col0", "col1" FROM "tab" WHERE ((NOT ("col2" <= $1)) OR (("col1" <> $2) AND ("col3" IS NULL)))`,
			outputArgs:       []any{int32(42), int64(0)},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:              nil,
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
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT "col0", "col1" FROM "tab"`,
			outputArgs:       []any{},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:              nil,
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
								Operation:  api_service_protos.TPredicate_TComparison_EQ,
								LeftValue:  rdbms_utils.NewColumnExpression("col2"),
								RightValue: rdbms_utils.NewTextValueExpression("text"),
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT "col0", "col1" FROM "tab"`,
			outputArgs:       []any{},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:              nil,
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
												Operation:  api_service_protos.TPredicate_TComparison_EQ,
												LeftValue:  rdbms_utils.NewColumnExpression("col2"),
												RightValue: rdbms_utils.NewTextValueExpression("text"),
											},
										},
									},
								},
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT "col0", "col1" FROM "tab" WHERE ("col1" = $1)`,
			outputArgs:       []any{int32(32)},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:              nil,
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
												Operation:  api_service_protos.TPredicate_TComparison_EQ,
												LeftValue:  rdbms_utils.NewColumnExpression("col2"),
												RightValue: rdbms_utils.NewTextValueExpression("text"),
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
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT "col0", "col1" FROM "tab" WHERE (("col1" = $1) AND ("col3" IS NULL) AND ("col4" IS NOT NULL))`,
			outputArgs:       []any{int32(32)},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32), common.MakePrimitiveType(ydb.Type_STRING)},
			err:              nil,
		},
		{
			testName: "negative_sql_injection_by_table",
			selectReq: &api_service_protos.TSelect{
				From: &api_service_protos.TSelect_TFrom{
					Table: `information_schema.columns; DROP TABLE information_schema.columns`,
				},
				What: &api_service_protos.TSelect_TWhat{},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT 0 FROM "information_schema.columns; DROP TABLE information_schema.columns"`,
			outputArgs:       []any{},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT64)}, // special case for empty select
			err:              nil,
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
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT "0; DROP TABLE information_schema.columns" FROM "tab"`,
			outputArgs:       []any{},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32)},
			err:              nil,
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
									Name: `0"; DROP TABLE information_schema.columns;`,
									Type: common.MakePrimitiveType(ydb.Type_INT32),
								},
							},
						},
					},
				},
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind: api_common.EGenericDataSourceKind_POSTGRESQL,
				},
			},
			splitDescription: singleSplit,
			outputQuery:      `SELECT "0""; DROP TABLE information_schema.columns;" FROM "tab"`,
			outputArgs:       []any{},
			outputYdbTypes:   []*ydb.Type{common.MakePrimitiveType(ydb.Type_INT32)},
			err:              nil,
		},
		{
			testName: "YQ-4568",
			selectReq: rdbms_utils.MustTSelectFromLoggerOutput(
				"{\"table\":\"lineitem\"}",
				"{\"items\":[{\"column\":{\"name\":\"id\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"INT32\"}}}}},{\"column\":{\"name\":\"l_comment\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"UTF8\"}}}}},{\"column\":{\"name\":\"l_commitdate\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"UTF8\"}}}}},{\"column\":{\"name\":\"l_discount\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"FLOAT\"}}}}},{\"column\":{\"name\":\"l_extendedprice\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"FLOAT\"}}}}},{\"column\":{\"name\":\"l_linenumber\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"INT32\"}}}}},{\"column\":{\"name\":\"l_linestatus\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"UTF8\"}}}}},{\"column\":{\"name\":\"l_orderkey\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"INT32\"}}}}},{\"column\":{\"name\":\"l_partkey\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"INT32\"}}}}},{\"column\":{\"name\":\"l_quantity\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"FLOAT\"}}}}},{\"column\":{\"name\":\"l_receiptdate\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"UTF8\"}}}}},{\"column\":{\"name\":\"l_returnflag\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"UTF8\"}}}}},{\"column\":{\"name\":\"l_shipdate\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"UTF8\"}}}}},{\"column\":{\"name\":\"l_shipinstruct\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"UTF8\"}}}}},{\"column\":{\"name\":\"l_shipmode\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"UTF8\"}}}}},{\"column\":{\"name\":\"l_suppkey\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"INT32\"}}}}},{\"column\":{\"name\":\"l_tax\",\"type\":{\"optional_type\":{\"item\":{\"type_id\":\"FLOAT\"}}}}}]}",
				"{}",
				api_common.EGenericDataSourceKind_POSTGRESQL,
			),
			splitDescription: &TSplitDescription{
				Payload: &TSplitDescription_HistogramBounds{
					HistogramBounds: &TSplitDescription_THistogramBounds{
						ColumnName: "id",
						Payload: &TSplitDescription_THistogramBounds_Int32Bounds{
							Int32Bounds: &TInt32Bounds{
								Lower: &wrapperspb.Int32Value{Value: 906},
								Upper: &wrapperspb.Int32Value{Value: 677197},
							},
						},
					},
				},
			},
			outputQuery: `SELECT "id", "l_comment", "l_commitdate", "l_discount", "l_extendedprice", "l_linenumber", "l_linestatus", "l_orderkey", "l_partkey", "l_quantity", "l_receiptdate", "l_returnflag", "l_shipdate", "l_shipinstruct", "l_shipmode", "l_suppkey", "l_tax" FROM "lineitem" WHERE ("id" >= 906 AND "id" < 677197)`,
			outputArgs:  []any{},
			outputYdbTypes: []*ydb.Type{
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_INT32)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_UTF8)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_UTF8)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_FLOAT)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_FLOAT)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_INT32)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_UTF8)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_INT32)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_INT32)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_FLOAT)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_UTF8)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_UTF8)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_UTF8)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_UTF8)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_UTF8)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_INT32)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_FLOAT)),
			},
			err: nil,
		},
	}

	for _, tc := range tcs {
		tc := tc

		t.Run(tc.testName, func(t *testing.T) {
			splitDescriptionBytes, err := protojson.Marshal(tc.splitDescription)
			require.NoError(t, err)

			readSplitsQuery, err := rdbms_utils.MakeSelectQuery(
				context.Background(),
				logger,
				formatter,
				&api_service_protos.TSplit{Select: tc.selectReq,
					Payload: &api_service_protos.TSplit_Description{
						Description: splitDescriptionBytes,
					},
				},
				api_service_protos.TReadSplitsRequest_FILTERING_OPTIONAL,
				tc.selectReq.From.Table,
			)

			if tc.err != nil {
				require.True(t, errors.Is(err, tc.err), err, tc.err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.outputQuery, readSplitsQuery.QueryText)
			require.Equal(t, tc.outputArgs, readSplitsQuery.QueryArgs.Values())

			actualTypes := common.YDBColumnsToYDBTypes(readSplitsQuery.YdbColumns)
			require.Equal(t, len(tc.outputYdbTypes), len(actualTypes))

			for i := range tc.outputYdbTypes {
				require.True(t, proto.Equal(tc.outputYdbTypes[i], actualTypes[i]))
			}
		})
	}
}
