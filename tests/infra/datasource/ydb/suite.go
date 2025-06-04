package ydb

import (
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
	tests_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

type Suite struct {
	*suite.Base[int32, *array.Int32Builder]
	dataSource    *datasource.DataSource
	connectorMode config.TYdbConfig_Mode
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

func (s *Suite) TestPushdownTimestampEQ() {
	t := time.Date(1988, 11, 20, 12, 55, 28, 123456000, time.UTC)

	s.ValidateTable(
		s.dataSource,
		tables["datetime_format_yql_pushdown_timestamp_EQ"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_03_timestamp",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_TIMESTAMP), t.UnixMicro()),
			),
		}),
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
				"col_01_int",
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
				"col_01_int",
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
				"col_01_int",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(20)),
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonEQNull() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_EQ_NULL"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_int",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), nil),
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
				"col_01_int",
				api_service_protos.TPredicate_TComparison_GE,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(20)),
			),
		}),
	)
}

func (s *Suite) TestPushdownComparisonG() {
	// WHERE col_01_int > id
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_comparison_G"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Comparison{
				Comparison: &api_service_protos.TPredicate_TComparison{
					LeftValue: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_Column{
							Column: "col_01_int",
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
				"col_01_int",
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
				"col_01_int",
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
								"col_01_int",
								api_service_protos.TPredicate_TComparison_G,
								common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(10)),
							),
						},
						{
							Payload: tests_utils.MakePredicateIsNotNullColumn("col_02_text"),
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
								"col_01_int",
								api_service_protos.TPredicate_TComparison_G,
								common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(10)),
							),
						},
						{
							Payload: tests_utils.MakePredicateIsNotNullColumn("col_02_text"),
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
							"col_01_int",
						),
					},
				},
			},
		}),
	)
}

func (s *Suite) TestPushdownStringsUtf8() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_strings_utf8"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_02_utf8",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UTF8), "a"),
			),
		}),
	)
}

func (s *Suite) TestPushdownStringsUtf8Optional() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_strings_utf8"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_02_utf8",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_UTF8)), "a"),
			),
		}),
	)
}

func (s *Suite) TestPushdownStringsString() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_strings_string"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_03_string",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), []byte("b")),
			),
		}),
	)
}

func (s *Suite) TestPushdownStringsStringOptional() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_strings_string"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_03_string",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)), []byte("b")),
			),
		}),
	)
}

// YQ-3949
func (s *Suite) TestJSONDocument() {
	s.ValidateTable(s.dataSource, tables["json_document"])
}

func (s *Suite) TestLargeTable() {
	// For tables larger than 1000 rows, scan queries must be used,
	// otherwise output will be truncated.
	// https://ydb.tech/docs/en/concepts/scan_query
	// This test makes sense only for Table Service.
	if s.connectorMode == config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE {
		s.T().Skip("Skipping test in QUERY_SERVICE_NATIVE mode")
	}

	s.ValidateTable(
		s.dataSource,
		tables["large"],
	)
}

func (s *Suite) TestTableInFolder() {
	// YDB allows to emplace tables in folders.
	// In this test case there is a folder called `parent` with a table called `child`
	s.ValidateTable(
		s.dataSource,
		tables["parent/child"],
	)
}

// Set of tests validating stats

func (s *Suite) TestPositiveStats() {
	suite.TestPositiveStats(s.Base, s.dataSource, tables["simple"])
}

func (s *Suite) TestMissingDataSource() {
	if s.connectorMode == config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE {
		s.T().Skip("Skipping test in QUERY_SERVICE_NATIVE mode")
	}

	dsi := &api_common.TGenericDataSourceInstance{
		Kind:     api_common.EGenericDataSourceKind_YDB,
		Endpoint: &api_common.TGenericEndpoint{Host: "www.google.com", Port: 2136},
		Database: "it's not important",
		Credentials: &api_common.TGenericCredentials{
			Payload: &api_common.TGenericCredentials_Basic{
				Basic: &api_common.TGenericCredentials_TBasic{
					Username: "it's not important",
					Password: "it's not important",
				},
			},
		},
		UseTls:   false,
		Protocol: api_common.EGenericProtocol_NATIVE,
	}

	suite.TestMissingDataSource(s.Base, dsi)
}

func (s *Suite) TestInvalidLogin() {
	for _, dsi := range s.dataSource.Instances {
		suite.TestInvalidLogin(s.Base, dsi, tables["simple"])
	}
}

func (s *Suite) TestInvalidPassword() {
	for _, dsi := range s.dataSource.Instances {
		suite.TestInvalidPassword(s.Base, dsi, tables["simple"])
	}
}

func (s *Suite) TestPushdownStringStartsWith() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_starts_with"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_string",
				api_service_protos.TPredicate_TComparison_STARTS_WITH,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), []byte("ab")),
			),
		}),
	)
}

func (s *Suite) TestPushdownStringEndsWith() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_ends_with"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_string",
				api_service_protos.TPredicate_TComparison_ENDS_WITH,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), []byte("ef")),
			),
		}),
	)
}

func (s *Suite) TestPushdownStringContains() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_contains"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_string",
				api_service_protos.TPredicate_TComparison_CONTAINS,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), []byte("h")),
			),
		}),
	)
}

func (s *Suite) TestPushdownUtf8StartsWith() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_starts_with"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_02_utf8",
				api_service_protos.TPredicate_TComparison_STARTS_WITH,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UTF8), "аб"),
			),
		}),
	)
}

func (s *Suite) TestPushdownUtf8EndsWith() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_ends_with"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_02_utf8",
				api_service_protos.TPredicate_TComparison_ENDS_WITH,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UTF8), "де"),
			),
		}),
	)
}

func (s *Suite) TestPushdownUtf8Contains() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_contains"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_02_utf8",
				api_service_protos.TPredicate_TComparison_CONTAINS,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UTF8), "ж"),
			),
		}),
	)
}

func (s *Suite) TestPushdownRegexpStringDigits() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_regexp_string_digits"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateRegexpColumn(
				"col_01_string",
				"\\d+",
			),
		}),
	)
}

func (s *Suite) TestPushdownRegexpStringLetters() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_regexp_string_letters"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateRegexpColumn(
				"col_01_string",
				"[a-z]+",
			),
		}),
	)
}

func (s *Suite) TestPushdownRegexpStringStartAnchor() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_regexp_string_start_anchor"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateRegexpColumn(
				"col_01_string",
				"^a",
			),
		}),
	)
}

func (s *Suite) TestPushdownRegexpUtf8Digits() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_regexp_utf8_digits"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateRegexpColumn(
				"col_02_utf8",
				"\\d+",
			),
		}),
	)
}

func (s *Suite) TestPushdownRegexpUtf8CyrillicLetters() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_regexp_utf8_cyrillic"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateRegexpColumn(
				"col_02_utf8",
				"[а-я]+",
			),
		}),
	)
}

func (s *Suite) TestPushdownRegexpUtf8EndAnchor() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_regexp_utf8_end_anchor"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateRegexpColumn(
				"col_02_utf8",
				"c$",
			),
		}),
	)
}

func (s *Suite) TestPushdownRegexpIf() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_regexp_if"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Regexp{
				Regexp: &api_service_protos.TPredicate_TRegexp{
					Value: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_If{
							If: &api_service_protos.TExpression_TIf{
								Predicate: &api_service_protos.TPredicate{
									Payload: &api_service_protos.TPredicate_IsNotNull{
										IsNotNull: &api_service_protos.TPredicate_TIsNotNull{
											Value: &api_service_protos.TExpression{
												Payload: &api_service_protos.TExpression_Column{
													Column: "col_02_utf8",
												},
											},
										},
									},
								},
								ThenExpression: &api_service_protos.TExpression{
									Payload: &api_service_protos.TExpression_Cast{
										Cast: &api_service_protos.TExpression_TCast{
											Value: &api_service_protos.TExpression{
												Payload: &api_service_protos.TExpression_Column{
													Column: "col_02_utf8",
												},
											},
											Type: &Ydb.Type{
												Type: &Ydb.Type_TypeId{
													TypeId: Ydb.Type_STRING,
												},
											},
										},
									},
								},
								ElseExpression: &api_service_protos.TExpression{
									Payload: &api_service_protos.TExpression_Null{
										Null: &api_service_protos.TExpression_TNull{},
									},
								},
							},
						},
					},
					Pattern: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_TypedValue{
							TypedValue: common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), []byte("^a")),
						},
					},
				},
			},
		}),
	)
}

func NewSuite(
	baseSuite *suite.Base[int32, *array.Int32Builder],
	connectorMode config.TYdbConfig_Mode,
) *Suite {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	result := &Suite{
		Base:          baseSuite,
		dataSource:    ds,
		connectorMode: connectorMode,
	}

	return result
}
