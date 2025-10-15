package postgresql

import (
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
	tests_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

type SuiteIDInt32 struct {
	*suite.Base[int32, *array.Int32Builder]
	dataSource *datasource.DataSource
}

func (s *SuiteIDInt32) TestSelect() {
	testCaseNames := []string{"simple", "primitives"}

	for _, testCase := range testCaseNames {
		s.ValidateTable(s.dataSource, tablesIDInt32[testCase])
	}
}

func (s *SuiteIDInt32) TestDatetimeFormatYQL() {
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["datetime_format_yql"],
		suite.WithDateTimeFormat(api_service_protos.EDateTimeFormat_YQL_FORMAT),
	)
}

func (s *SuiteIDInt32) TestPushdownTimestampEQ() {
	t := time.Date(1988, 11, 20, 12, 55, 28, 123000000, time.UTC)

	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["datetime_format_yql_pushdown_timestamp_EQ"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_timestamp",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_TIMESTAMP), t.UnixMicro()),
			),
		}),
		suite.WithDateTimeFormat(api_service_protos.EDateTimeFormat_YQL_FORMAT),
	)
}

func (s *SuiteIDInt32) TestDatetimeFormatString() {
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["datetime_format_string"],
		suite.WithDateTimeFormat(api_service_protos.EDateTimeFormat_STRING_FORMAT),
	)
}

func (s *SuiteIDInt32) TestPushdownComparisonL() {
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_comparison_L"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_int",
				api_service_protos.TPredicate_TComparison_L,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(20)),
			),
		}),
	)
}

func (s *SuiteIDInt32) TestPushdownComparisonLE() {
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_comparison_LE"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_int",
				api_service_protos.TPredicate_TComparison_LE,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(20)),
			),
		}),
	)
}

func (s *SuiteIDInt32) TestPushdownComparisonEQ() {
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_comparison_EQ"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_int",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(20)),
			),
		}),
	)
}

func (s *SuiteIDInt32) TestPushdownComparisonGE() {
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_comparison_GE"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_01_int",
				api_service_protos.TPredicate_TComparison_GE,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(20)),
			),
		}),
	)
}

func (s *SuiteIDInt32) TestPushdownComparisonG() {
	// WHERE col_01_int > id
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_comparison_G"],
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

func (s *SuiteIDInt32) TestPushdownComparisonNE() {
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_comparison_NE"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"id",
				api_service_protos.TPredicate_TComparison_NE,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(1)),
			),
		}),
	)
}

func (s *SuiteIDInt32) TestPushdownComparisonNULL() {
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_comparison_NULL"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateIsNullColumn(
				"col_01_int",
			),
		}),
	)
}

func (s *SuiteIDInt32) TestPushdownComparisonNotNULL() {
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_comparison_NOT_NULL"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateIsNotNullColumn(
				"col_01_int",
			),
		}),
	)
}

func (s *SuiteIDInt32) TestPushdownConjunction() {
	// WHERE col_01_int > 10 AND col_02_string IS NOT NULL
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_conjunction"],
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

func (s *SuiteIDInt32) TestPushdownDisjunction() {
	// WHERE col_01_int > 10 OR col_02_string IS NOT NULL
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_disjunction"],
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

func (s *SuiteIDInt32) TestPushdownNegation() {
	// WHERE NOT (col_01_int IS NOT NULL)
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_negation"],
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

// YQ-3702. In this couple of tests we check the case when the client
// required a pushdown that cannot be rendered by the server.
// Dependening on a value of filtering mode, server returns either an error
// or a full table.

func (s *SuiteIDInt32) TestPushdownUnsupportedFilteringOptional() {
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_unsupported_filtering_optional"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"id",
				api_service_protos.TPredicate_TComparison_EQ,
				// PostgreSQL does not support unsigned numbers
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UINT32), int32(1)),
			),
		}),
		suite.WithFiltering(api_service_protos.TReadSplitsRequest_FILTERING_OPTIONAL),
	)
}

func (s *SuiteIDInt32) TestPushdownUnsupportedFilteringMandatory() {
	for _, dsi := range s.dataSource.Instances {
		suite.TestUnsupportedPushdownFilteringMandatory(
			s.Base,
			dsi,
			tablesIDInt32["pushdown_unsupported_filtering_optional"],
			&api_service_protos.TPredicate{
				Payload: tests_utils.MakePredicateComparisonColumn(
					"id",
					api_service_protos.TPredicate_TComparison_EQ,
					// PostgreSQL does not support unsigned numbers
					common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UINT32), int32(1)),
				),
			})
	}
}

// Set of tests validating stats

func (s *SuiteIDInt32) TestPositiveStats() {
	suite.TestPositiveStats(s.Base, s.dataSource, tablesIDInt32["simple"])
}

func (s *SuiteIDInt32) TestMissingDataSource() {
	dsi := &api_common.TGenericDataSourceInstance{
		Kind:     api_common.EGenericDataSourceKind_POSTGRESQL,
		Endpoint: &api_common.TGenericEndpoint{Host: "www.google.com", Port: 5432},
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
		Options: &api_common.TGenericDataSourceInstance_PgOptions{
			PgOptions: &api_common.TPostgreSQLDataSourceOptions{
				Schema: "public",
			},
		},
	}

	suite.TestMissingDataSource(s.Base, dsi)
}

func (s *SuiteIDInt32) TestInvalidLogin() {
	for _, dsi := range s.dataSource.Instances {
		suite.TestInvalidLogin(s.Base, dsi, tablesIDInt32["simple"])
	}
}

func (s *SuiteIDInt32) TestInvalidPassword() {
	for _, dsi := range s.dataSource.Instances {
		suite.TestInvalidPassword(s.Base, dsi, tablesIDInt32["simple"])
	}
}

func (s *SuiteIDInt32) TestPushdownDecimalIntEQ() {
	// Test for: SELECT * FROM table WHERE col_27_numeric_int = Decimal("1", 10, 0);
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_decimal_int_EQ"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_27_numeric_int",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakeDecimalType(10, 0), []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			),
		}),
	)
}

func (s *SuiteIDInt32) TestPushdownDecimalRationalEQ() {
	// Test for: SELECT * FROM table WHERE col_28_numeric_rational = Decimal("-22.22", 4, 2);
	s.ValidateTable(
		s.dataSource,
		tablesIDInt32["pushdown_decimal_rational_EQ"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"col_28_numeric_rational",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(
					common.MakeDecimalType(4, 2),
					[]byte{82, 247, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				),
			),
		}),
	)
}

func NewSuiteIDInt32(
	baseSuite *suite.Base[int32, *array.Int32Builder],
) *SuiteIDInt32 {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	result := &SuiteIDInt32{
		Base:       baseSuite,
		dataSource: ds,
	}

	return result
}

func (s *SuiteIDInt32) TestPrimaryKeyInt() {
	s.ValidateTable(s.dataSource, tablesIDInt32["primary_key_int"])
}

type SuiteIDInt64 struct {
	*suite.Base[int64, *array.Int64Builder]
	dataSource *datasource.DataSource
}

func (s *SuiteIDInt64) TestPrimaryKeyBigint() {
	s.ValidateTable(s.dataSource, tablesIDInt64["primary_key_bigint"])
}

func NewSuiteIDInt64(
	baseSuite *suite.Base[int64, *array.Int64Builder],
) *SuiteIDInt64 {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	result := &SuiteIDInt64{
		Base:       baseSuite,
		dataSource: ds,
	}

	return result
}

type SuiteIDDecimal struct {
	*suite.Base[[]byte, *array.FixedSizeBinaryBuilder]
	dataSource *datasource.DataSource
}

func (s *SuiteIDDecimal) TestPrimaryKeyNumericPrecision10Scale0() {
	s.ValidateTable(s.dataSource, tablesIDDecimal["primary_key_numeric_10_0"])
}

func (s *SuiteIDDecimal) TestPrimaryKeyNumericPrecision4Scale2() {
	s.ValidateTable(s.dataSource, tablesIDDecimal["primary_key_numeric_4_2"])
}

// Yet not supported
// func (s *SuiteIDDecimal) TestPrimaryKeyNumericUnconstrained() {
// 	s.ValidateTable(s.dataSource, tablesIDDecimal["primary_key_numeric_unconstrained"])
// }

func NewSuiteIDDecimal(baseSuite *suite.Base[[]byte, *array.FixedSizeBinaryBuilder]) *SuiteIDDecimal {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	result := &SuiteIDDecimal{
		Base:       baseSuite,
		dataSource: ds,
	}

	return result
}

//
