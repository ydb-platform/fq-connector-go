package mysql

import (
	"github.com/apache/arrow/go/v13/arrow/array"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	tests_utils "github.com/ydb-platform/fq-connector-go/tests/utils"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

type Suite struct {
	*suite.Base[int32, *array.Int32Builder]
	dataSource *datasource.DataSource
}

func (s *Suite) TestSelect() {
	testCaseNames := []string{"simple", "primitives"}

	for _, testCase := range testCaseNames {
		s.ValidateTable(s.dataSource, tables[testCase])
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

// Set of tests validating stats

func (s *Suite) TestPositiveStats() {
	suite.TestPositiveStats(s.Base, s.dataSource, tables["simple"])
}

func (s *Suite) TestMissingDataSource() {
	dsi := &api_common.TDataSourceInstance{
		Kind:     api_common.EDataSourceKind_MYSQL,
		Endpoint: &api_common.TEndpoint{Host: "www.google.com", Port: 3306},
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

func (s *Suite) TestPushdownComparisonL() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_L"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"int_column",
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
				"int_column",
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
				"int_column",
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
				"int_column",
				api_service_protos.TPredicate_TComparison_GE,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(20)),
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
							Column: "int_column",
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
				"int_column",
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
				"int_column",
			),
		}),
	)
}

func (s *Suite) TestPushdownConjunction() {
	// WHERE col_01_int > 10 AND col_02_string IS NOT NULL
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_conjunction"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Conjunction{
				Conjunction: &api_service_protos.TPredicate_TConjunction{
					Operands: []*api_service_protos.TPredicate{
						{
							Payload: tests_utils.MakePredicateComparisonColumn(
								"int_column",
								api_service_protos.TPredicate_TComparison_G,
								common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(10)),
							),
						},
						{
							Payload: tests_utils.MakePredicateIsNotNullColumn("varchar_column"),
						},
					},
				},
			},
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
							Payload: tests_utils.MakePredicateComparisonColumn(
								"int_column",
								api_service_protos.TPredicate_TComparison_G,
								common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(10)),
							),
						},
						{
							Payload: tests_utils.MakePredicateIsNotNullColumn("varchar_column"),
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
						Payload: tests_utils.MakePredicateIsNotNullColumn(
							"int_column",
						),
					},
				},
			},
		}),
	)
}

func (s *Suite) TestEmptyTable() {
	s.ValidateTable(
		s.dataSource,
		tables["empty_table"],
	)
}

// TODO: fix error mapping in `common/errors.go`
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

func NewSuite(
	baseSuite *suite.Base[int32, *array.Int32Builder],
) *Suite {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	result := &Suite{
		Base:       baseSuite,
		dataSource: ds,
	}

	return result
}
