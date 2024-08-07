package oracle

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"

	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

type Suite struct {
	*suite.Base[int64, *array.Int64Builder]
	dataSource *datasource.DataSource
}

func (s *Suite) TestSelect() {
	testCaseNames := []string{"simple", "primitives", "long_table", "longraw"}

	for _, testCase := range testCaseNames {
		s.ValidateTable(s.dataSource, tables[testCase])
	}
}

func (s *Suite) TestDatetimeFormatYQL() {
	testCaseNames := []string{"datetime_format_yql", "timestamps_format_yql"}

	for _, testCase := range testCaseNames {
		s.ValidateTable(
			s.dataSource,
			tables[testCase],
			suite.WithDateTimeFormat(api_service_protos.EDateTimeFormat_YQL_FORMAT),
		)
	}
}

func (s *Suite) TestDatetimeFormatString() {
	testCaseNames := []string{"datetime_format_string", "timestamps_format_string"}

	for _, testCase := range testCaseNames {
		s.ValidateTable(
			s.dataSource,
			tables[testCase],
			suite.WithDateTimeFormat(api_service_protos.EDateTimeFormat_STRING_FORMAT),
		)
	}
}

func (s *Suite) TestPushdownComparisonL() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_L"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: test_utils.MakePredicateComparisonColumn(
				"INT_COLUMN",
				api_service_protos.TPredicate_TComparison_L,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT64), int64(20)),
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonLE() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_LE"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: test_utils.MakePredicateComparisonColumn(
				"INT_COLUMN",
				api_service_protos.TPredicate_TComparison_LE,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT64), int64(20)),
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonEQ() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_EQ"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: test_utils.MakePredicateComparisonColumn(
				"INT_COLUMN",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT64), int64(20)),
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonGE() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_GE"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: test_utils.MakePredicateComparisonColumn(
				"INT_COLUMN",
				api_service_protos.TPredicate_TComparison_GE,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT64), int64(20)),
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonG() {
	// WHERE int_column > id
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_G"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Comparison{
				Comparison: &api_service_protos.TPredicate_TComparison{
					LeftValue: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_Column{
							Column: "INT_COLUMN",
						},
					},
					Operation: api_service_protos.TPredicate_TComparison_G,
					RightValue: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_Column{
							Column: "ID",
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
			Payload: test_utils.MakePredicateComparisonColumn(
				"ID",
				api_service_protos.TPredicate_TComparison_NE,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT64), int64(1)),
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonNULL() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_NULL"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: test_utils.MakePredicateIsNullColumn(
				"INT_COLUMN",
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonNotNULL() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_NOT_NULL"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: test_utils.MakePredicateIsNotNullColumn(
				"INT_COLUMN",
			),
		}),
	)
}

func (s *Suite) TestPushdownDisjunction() {
	// WHERE col_01_int > 10 OR col_02_string IS NOT NULL
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_disjunction"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Disjunction{
				Disjunction: &api_service_protos.TPredicate_TDisjunction{
					Operands: []*api_service_protos.TPredicate{
						{
							Payload: test_utils.MakePredicateComparisonColumn(
								"INT_COLUMN",
								api_service_protos.TPredicate_TComparison_G,
								common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT64), int64(10)),
							),
						},
						{
							Payload: test_utils.MakePredicateIsNotNullColumn("VARCHAR_COLUMN"),
						},
					},
				},
			},
		}),
	)
}

func (s *Suite) TestPushdownNegation() {
	// WHERE NOT (col_01_int IS NOT NULL)
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_negation"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Negation{
				Negation: &api_service_protos.TPredicate_TNegation{
					Operand: &api_service_protos.TPredicate{
						Payload: test_utils.MakePredicateIsNotNullColumn(
							"INT_COLUMN",
						),
					},
				},
			},
		}),
	)
}

func (s *Suite) TestPositiveStats() {
	suite.TestPositiveStats(s.Base, s.dataSource, tables["simple"])
}

func (s *Suite) TestMissingDataSource() {
	dsi := &api_common.TDataSourceInstance{
		Kind:     api_common.EDataSourceKind_ORACLE,
		Endpoint: &api_common.TEndpoint{Host: "missing_data_source", Port: 3306},
		Database: "it's not important",
		Credentials: &api_common.TCredentials{
			Payload: &api_common.TCredentials_Basic{
				Basic: &api_common.TCredentials_TBasic{
					Username: "it's not important",
					Password: "it's not important",
				},
			},
		},
		Options: &api_common.TDataSourceInstance_OracleOptions{
			OracleOptions: &api_common.TOracleDataSourceOptions{
				ServiceName: "it's not important",
			},
		},
		UseTls:   false,
		Protocol: api_common.EProtocol_NATIVE,
	}

	suite.TestMissingDataSource(s.Base, dsi)
}

func (s *Suite) TestInvalidLogin() {
	s.T().Skip()

	for _, dsi := range s.dataSource.Instances {
		suite.TestInvalidLogin(s.Base, dsi, tables["simple"])
	}
}

func (s *Suite) TestInvalidPassword() {
	for _, dsi := range s.dataSource.Instances {
		suite.TestInvalidPassword(s.Base, dsi, tables["simple"])
	}
}

func (s *Suite) TestInvalidServiceName() {
	for _, dsi := range s.dataSource.Instances {
		testInvalidServiceName(s.Base, dsi, tables["simple"])
	}
}

func NewSuite(
	baseSuite *suite.Base[int64, *array.Int64Builder],
) *Suite {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	// for _, dsi := range ds.Instances {
	// 	waiter := newDefaultDBWaiter(baseSuite, dsi)

	// 	err := waiter.wait()
	// 	baseSuite.Require().NoError(err)
	// }

	result := &Suite{
		Base:       baseSuite,
		dataSource: ds,
	}

	return result
}
