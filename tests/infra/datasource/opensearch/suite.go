package opensearch

import (
	"github.com/apache/arrow/go/v13/arrow/array"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
	tests_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

type Suite struct {
	*suite.Base[string, *array.StringBuilder]
	dataSource *datasource.DataSource
}

func (s *Suite) TestDescribeTable() {
	testCaseNames := []string{"simple", "list", "nested", "nested_list", "optional"}

	for _, testCase := range testCaseNames {
		s.ValidateTableMetadata(s.dataSource, tables[testCase])
	}
}

func (s *Suite) TestReadSplitPrimitives() {
	testCaseNames := []string{"simple", "nested", "optional"}

	for _, testCase := range testCaseNames {
		s.ValidateTable(s.dataSource, tables[testCase])
	}
}

func (s *Suite) TestPushdownProjection() {
	what := &api_service_protos.TSelect_TWhat{
		Items: []*api_service_protos.TSelect_TWhat_TItem{
			{
				Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
					Column: &Ydb.Column{
						Name: "_id",
						Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
					},
				},
			},
			{
				Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
					Column: &Ydb.Column{
						Name: "int32_field",
						Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
					},
				},
			},
		},
	}

	s.ValidateTable(
		s.dataSource,
		tables["pushdown_projection"],
		suite.WithWhat(what),
	)
}

func (s *Suite) TestPushdownIsNull() {
	testCaseNames := []string{"int32", "double", "boolean", "string", "objectid"}
	for _, testCase := range testCaseNames {
		s.ValidateTable(
			s.dataSource,
			tables["pushdown_null"],
			suite.WithPredicate(&api_service_protos.TPredicate{
				Payload: tests_utils.MakePredicateIsNullColumn(
					testCase,
				),
			}),
		)
	}
}

func (s *Suite) TestPushdownIsNotNull() {
	testCaseNames := []string{"int32", "double", "boolean", "string", "objectid"}
	for _, testCase := range testCaseNames {
		s.ValidateTable(
			s.dataSource,
			tables["pushdown_not_null"],
			suite.WithPredicate(&api_service_protos.TPredicate{
				Payload: tests_utils.MakePredicateIsNotNullColumn(
					testCase,
				),
			}),
		)
	}
}

func (s *Suite) TestPushdownComparisonEQ() {
	testcases := map[string]*Ydb.TypedValue{
		"ind":      common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(0)),
		"int32":    common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(64)),
		"int64":    common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT64)), int64(23423)),
		"string":   common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)), "outer"),
		"double":   common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_DOUBLE)), 1.1),
		"boolean":  common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_BOOL)), false),
		"objectid": common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)), "507f1f77bcf86cd799439011"),
	}

	for column, value := range testcases {
		s.ValidateTable(
			s.dataSource,
			tables["pushdown_comparisons_eq"],
			suite.WithPredicate(&api_service_protos.TPredicate{
				Payload: tests_utils.MakePredicateComparisonColumn(
					column,
					api_service_protos.TPredicate_TComparison_EQ,
					value,
				),
			}),
		)
	}
}

func (s *Suite) TestPushdownStringComparison() {
	fieldName := "a"
	value := common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)), "abc")

	testCases := map[string]*api_service_protos.TPredicate_Comparison{
		"strcomp_0": tests_utils.MakePredicateComparisonColumn(
			fieldName,
			api_service_protos.TPredicate_TComparison_STARTS_WITH,
			value,
		),
		"strcomp_1": tests_utils.MakePredicateComparisonColumn(
			fieldName,
			api_service_protos.TPredicate_TComparison_ENDS_WITH,
			value,
		),
		"strcomp": tests_utils.MakePredicateComparisonColumn(
			fieldName,
			api_service_protos.TPredicate_TComparison_CONTAINS,
			value,
		),
	}

	for table, predicate := range testCases {
		s.ValidateTable(
			s.dataSource,
			tables[table],
			suite.WithPredicate(&api_service_protos.TPredicate{
				Payload: predicate,
			}),
		)
	}
}

func (s *Suite) TestPushdownComparisonLG() {
	fieldName := "ind"

	testCases := map[string]*api_service_protos.TPredicate_Comparison{
		"columns_l": tests_utils.MakePredicateComparisonColumn(
			fieldName,
			api_service_protos.TPredicate_TComparison_L,
			common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(3)),
		),
		"columns_le": tests_utils.MakePredicateComparisonColumn(
			fieldName,
			api_service_protos.TPredicate_TComparison_LE,
			common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(3)),
		),
		"columns_g": tests_utils.MakePredicateComparisonColumn(
			fieldName,
			api_service_protos.TPredicate_TComparison_G,
			common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(1)),
		),
		"columns_ge": tests_utils.MakePredicateComparisonColumn(
			fieldName,
			api_service_protos.TPredicate_TComparison_GE,
			common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(1)),
		),
	}

	for table, predicate := range testCases {
		s.ValidateTable(
			s.dataSource,
			tables[table],
			suite.WithPredicate(&api_service_protos.TPredicate{
				Payload: predicate,
			}),
		)
	}
}

func (s *Suite) TestPushdownConjunction() {
	s.ValidateTable(
		s.dataSource,
		tables["conj"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Conjunction{
				Conjunction: &api_service_protos.TPredicate_TConjunction{
					Operands: []*api_service_protos.TPredicate{
						{
							Payload: tests_utils.MakePredicateComparisonColumn(
								"a",
								api_service_protos.TPredicate_TComparison_EQ,
								common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(1)),
							),
						},
						{
							Payload: tests_utils.MakePredicateComparisonColumn(
								"b",
								api_service_protos.TPredicate_TComparison_EQ,
								common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)), "hello"),
							),
						},
					},
				},
			},
		}),
	)
}

func (s *Suite) TestPushdownDisjunction() {
	s.ValidateTable(
		s.dataSource,
		tables["disj"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Disjunction{
				Disjunction: &api_service_protos.TPredicate_TDisjunction{
					Operands: []*api_service_protos.TPredicate{
						{
							Payload: tests_utils.MakePredicateComparisonColumn(
								"a",
								api_service_protos.TPredicate_TComparison_EQ,
								common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(1)),
							),
						},
						{
							Payload: tests_utils.MakePredicateComparisonColumn(
								"b",
								api_service_protos.TPredicate_TComparison_EQ,
								common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)), "hi")),
						},
					},
				},
			},
		}),
	)
}

func (s *Suite) TestPushdownNegation() {
	testCases := []*api_service_protos.TPredicate{
		{
			Payload: tests_utils.MakePredicateIsNotNullColumn(
				"int64",
			),
		},
		{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"string",
				api_service_protos.TPredicate_TComparison_NE,
				common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)), "exists2"),
			),
		},
		{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"int32",
				api_service_protos.TPredicate_TComparison_GE,
				common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(42)),
			),
		},
	}

	for _, testCase := range testCases {
		s.ValidateTable(
			s.dataSource,
			tables["neg"],
			suite.WithPredicate(&api_service_protos.TPredicate{
				Payload: &api_service_protos.TPredicate_Negation{
					Negation: &api_service_protos.TPredicate_TNegation{
						Operand: testCase,
					},
				},
			}),
		)
	}
}

func (s *Suite) TestPushdownBoolExpression() {
	s.ValidateTable(
		s.dataSource,
		tables["bool"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateBoolExpressionColumn("boolean"),
		}),
	)
}

func (s *Suite) TestPushdownNegBoolExpression() {
	s.ValidateTable(
		s.dataSource,
		tables["neg_bool"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Negation{
				Negation: &api_service_protos.TPredicate_TNegation{
					Operand: &api_service_protos.TPredicate{
						Payload: tests_utils.MakePredicateBoolExpressionColumn("boolean"),
					},
				},
			},
		}),
	)
}

func (s *Suite) TestPushdownBetween() {
	s.ValidateTable(
		s.dataSource,
		tables["pushdown_between"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateBetweenColumn(
				"ind",
				common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(3)),
				common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(4)),
			),
		}),
	)
}

func (s *Suite) TestPushdownIn() {
	testCases := map[string][]*Ydb.TypedValue{
		"ind": {
			common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(5)),
			common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(2)),
			common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(4)),
		},
		"b": {
			common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)), "hi"),
			common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)), "two"),
			common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)), "four"),
		},
	}

	for column, valueSet := range testCases {
		s.ValidateTable(
			s.dataSource,
			tables["pushdown_in"],
			suite.WithPredicate(&api_service_protos.TPredicate{
				Payload: tests_utils.MakePredicateInColumn(
					column, valueSet,
				),
			}),
		)
	}
}

func (s *Suite) TestPushdownRegex() {
	toastPatterns := []string{
		"toast",
		"t.+ast?",
		"t.{4}",
	}

	for _, pattern := range toastPatterns {
		s.ValidateTable(
			s.dataSource,
			tables["pushdown_regex"],
			suite.WithPredicate(&api_service_protos.TPredicate{
				Payload: tests_utils.MakePredicateRegexpColumn("a", pattern),
			}),
		)
	}
}

func NewSuite(
	baseSuite *suite.Base[string, *array.StringBuilder],
) *Suite {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	result := &Suite{
		Base:       baseSuite,
		dataSource: ds,
	}

	return result
}
