package clickhouse

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
	tests_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

type Suite struct {
	*suite.Base
	dataSource *datasource.DataSource
}

func (s *Suite) TestSelect() {
	testCaseNames := []string{"simple", "primitives", "optionals"}

	for _, tableName := range testCaseNames {
		s.ValidateTable(s.dataSource, tables[tableName])
	}
}

func (s *Suite) TestDatetimeFormatYQL() {
	s.ValidateTable(
		s.dataSource,
		tables["datetime_format_yql"],
		suite.WithDateTimeFormat(api_service_protos.EDateTimeFormat_YQL_FORMAT),
	)
}

func (s *Suite) TestDatetimeFormatString() {
	s.ValidateTable(
		s.dataSource,
		tables["datetime_format_string"],
		suite.WithDateTimeFormat(api_service_protos.EDateTimeFormat_STRING_FORMAT),
	)
}

func (s *Suite) TestPushdownComparisonL() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_L"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_int32",
				api_service_protos.TPredicate_TComparison_L,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(20)),
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonLE() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_LE"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_int32",
				api_service_protos.TPredicate_TComparison_LE,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(20)),
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonEQ() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_EQ"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_int32",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(20)),
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonGE() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_GE"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_int32",
				api_service_protos.TPredicate_TComparison_GE,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(20)),
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonG() {
	// WHERE col_01_int32 > id
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_G"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Comparison{
				Comparison: &api_service_protos.TPredicate_TComparison{
					LeftValue: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_Column{
							Column: "col_01_int32",
						},
					},
					Operation: api_service_protos.TPredicate_TComparison_G,
					RightValue: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_Column{
							Column: "id",
						},
					},
				},
			},
		}),
	)
}

func (s *Suite) TestPushdownComparisonNE() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_NE"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"id",
				api_service_protos.TPredicate_TComparison_NE,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(1)),
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonNULL() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_NULL"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateIsNullColumn(
				"col_01_int32",
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonNotNULL() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_NOT_NULL"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateIsNotNullColumn(
				"col_01_int32",
			),
		}),
	)
}

func (s *Suite) TestPushdownConjunction() {
	// WHERE col_01_int32 > 10 AND col_02_string IS NOT NULL
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_conjunction"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Conjunction{
				Conjunction: &api_service_protos.TPredicate_TConjunction{
					Operands: []*api_service_protos.TPredicate{
						{
							Payload: tests_utils.MakePredicateComparisonColumn(
								"col_01_int32",
								api_service_protos.TPredicate_TComparison_G,
								common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(10)),
							),
						},
						{
							Payload: tests_utils.MakePredicateIsNotNullColumn("col_02_string"),
						},
					},
				},
			},
		}),
	)
}

func (s *Suite) TestPushdownDisjunction() {
	// WHERE col_01_int32 > 10 OR col_02_string IS NOT NULL
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_disjunction"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Disjunction{
				Disjunction: &api_service_protos.TPredicate_TDisjunction{
					Operands: []*api_service_protos.TPredicate{
						{
							Payload: tests_utils.MakePredicateComparisonColumn(
								"col_01_int32",
								api_service_protos.TPredicate_TComparison_G,
								common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(10)),
							),
						},
						{
							Payload: tests_utils.MakePredicateIsNotNullColumn("col_02_string"),
						},
					},
				},
			},
		}),
	)
}

func (s *Suite) TestPushdownNegation() {
	// WHERE NOT (col_01_int32 IS NOT NULL)
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_negation"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Negation{
				Negation: &api_service_protos.TPredicate_TNegation{
					Operand: &api_service_protos.TPredicate{
						Payload: tests_utils.MakePredicateIsNotNullColumn(
							"col_01_int32",
						),
					},
				},
			},
		}),
	)
}

func (s *Suite) TestPushdownBetween() {
	// WHERE col_01_int32 BETWEEN 15 AND @%
	s.T().Skip() // Not implemented yet
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_between"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Between{
				Between: &api_service_protos.TPredicate_TBetween{
					Value: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_Column{
							Column: "col_01_int32",
						},
					},
					Least: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_TypedValue{
							TypedValue: common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(15)),
						},
					},
					Greatest: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_TypedValue{
							TypedValue: common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(25)),
						},
					},
				},
			},
		}),
	)
}

// Set of tests validating stats

func (s *Suite) TestPositiveStats() {
	suite.TestPositiveStats(s.Base, s.dataSource, tables["simple"])
}

func (s *Suite) TestMissingDataSource() {
	dsi := &api_common.TDataSourceInstance{
		Kind:     api_common.EDataSourceKind_CLICKHOUSE,
		Endpoint: &api_common.TEndpoint{Host: "missing_data_source", Port: 12345},
		Database: "it's not important",
		Credentials: &api_common.TCredentials{
			Payload: &api_common.TCredentials_Basic{
				Basic: &api_common.TCredentials_TBasic{
					Username: "it's not important",
					Password: "it's not important",
				},
			},
		},
		UseTls:   false,
		Protocol: api_common.EProtocol_NATIVE,
	}

	suite.TestMissingDataSource(s.Base, dsi)
}

func NewSuite(
	baseSuite *suite.Base,
) *Suite {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	result := &Suite{
		Base:       baseSuite,
		dataSource: ds,
	}

	return result
}
