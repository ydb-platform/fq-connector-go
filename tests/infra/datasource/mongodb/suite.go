package mongodb

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
	*suite.Base[int32, *array.Int32Builder]
	dataSource *datasource.DataSource
}

func (s *Suite) SetDefaultOptions() {
	for _, instance := range s.dataSource.Instances {
		instance.Options = defaultMongoDbOptions
	}
}

func (s *Suite) SetAsStringOptions() {
	for _, instance := range s.dataSource.Instances {
		instance.Options = asStringMongoDbOptions
	}
}

func (s *Suite) TestReadSplitPrimitives() {
	s.SetDefaultOptions()

	testCaseNames := []string{"simple", "primitives", "missing", "uneven"}

	for _, testCase := range testCaseNames {
		s.ValidateTable(s.dataSource, tables[testCase])
	}
}

func (s *Suite) TestIncludeUnsupported() {
	s.SetAsStringOptions()

	testCaseNames := []string{"unsupported"}
	for _, testCase := range testCaseNames {
		s.ValidateTable(s.dataSource, tables[testCase])
	}
}

func (s *Suite) TestPushdownProjection() {
	s.SetDefaultOptions()

	what := &api_service_protos.TSelect_TWhat{
		Items: []*api_service_protos.TSelect_TWhat_TItem{
			{
				Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
					Column: &Ydb.Column{
						Name: "_id",
						Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
					},
				},
			},
			{
				Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
					Column: &Ydb.Column{
						Name: "int32",
						Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
					},
				},
			},
		},
	}

	s.ValidateTable(
		s.dataSource,
		tables["primitives_int32"],
		suite.WithWhat(what),
	)
}

func (s *Suite) TestPushdownIsNull() {
	s.SetDefaultOptions()

	testCaseNames := []string{"int32", "double", "boolean"}
	for _, testCase := range testCaseNames {
		s.ValidateTable(
			s.dataSource,
			tables["missing_2"],
			suite.WithPredicate(&api_service_protos.TPredicate{
				Payload: tests_utils.MakePredicateIsNullColumn(
					testCase,
				),
			}),
		)
	}
}

func (s *Suite) TestPushdownIsNotNull() {
	s.SetDefaultOptions()

	testCaseNames := []string{"int64", "string", "objectid"}
	for _, testCase := range testCaseNames {
		s.ValidateTable(
			s.dataSource,
			tables["missing_0"],
			suite.WithPredicate(&api_service_protos.TPredicate{
				Payload: tests_utils.MakePredicateIsNotNullColumn(
					testCase,
				),
			}),
		)
	}
}

func (s *Suite) TestPushdownComparisonEQ() {
	s.SetDefaultOptions()

	testcases := map[string]*Ydb.TypedValue{
		"_id":     common.MakeTypedValue(common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)), int32(0)),
		"int32":   common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(64)),
		"int64":   common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT64), int64(23423)),
		"string":  common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UTF8), "outer"),
		"binary":  common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), []byte{0xab, 0xcd}),
		"double":  common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_DOUBLE), float64(1.1)),
		"boolean": common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_BOOL), false),
		"objectid": common.MakeTypedValue(
			common.MakeTaggedType("ObjectId",
				common.MakePrimitiveType(Ydb.Type_STRING)),
			[]byte{0x17, 0x1e, 0x75, 0x50, 0x0e, 0xcd, 0xe1, 0xc7, 0x5c, 0x59, 0x13, 0x9e},
		),
	}

	for column, value := range testcases {
		s.ValidateTable(
			s.dataSource,
			tables["missing_0"],
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
	s.SetDefaultOptions()

	fieldName := "a"
	value := common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UTF8), "abc")

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

func (s *Suite) TestPushdownComparisonTwoColumns() {
	s.SetDefaultOptions()

	s.ValidateTable(
		s.dataSource,
		tables["similar_056"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Comparison{
				Comparison: &api_service_protos.TPredicate_TComparison{
					LeftValue: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_Column{Column: "_id"},
					},
					Operation: api_service_protos.TPredicate_TComparison_L,
					RightValue: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_Column{Column: "a"},
					},
				},
			}}),
	)
}

func (s *Suite) TestPushdownConjunction() {
	s.SetDefaultOptions()

	s.ValidateTable(
		s.dataSource,
		tables["similar_0"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Conjunction{
				Conjunction: &api_service_protos.TPredicate_TConjunction{
					Operands: []*api_service_protos.TPredicate{
						{
							Payload: tests_utils.MakePredicateComparisonColumn(
								"a",
								api_service_protos.TPredicate_TComparison_EQ,
								common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(1)),
							),
						},
						{
							Payload: tests_utils.MakePredicateComparisonColumn(
								"b",
								api_service_protos.TPredicate_TComparison_EQ,
								common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UTF8), "hello"),
							),
						},
					},
				},
			},
		}),
	)
}

func (s *Suite) TestPushdownDisjunction() {
	s.SetDefaultOptions()

	s.ValidateTable(
		s.dataSource,
		tables["similar_01"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Disjunction{
				Disjunction: &api_service_protos.TPredicate_TDisjunction{
					Operands: []*api_service_protos.TPredicate{
						{
							Payload: tests_utils.MakePredicateComparisonColumn(
								"_id",
								api_service_protos.TPredicate_TComparison_EQ,
								common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(0)),
							),
						},
						{
							Payload: tests_utils.MakePredicateComparisonColumn(
								"b",
								api_service_protos.TPredicate_TComparison_EQ,
								common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UTF8), "hi"),
							),
						},
					},
				},
			},
		}),
	)
}

func (s *Suite) TestPushdownNegation() {
	s.SetDefaultOptions()

	testCases := []*api_service_protos.TPredicate{
		{
			Payload: tests_utils.MakePredicateIsNotNullColumn(
				"int64",
			),
		},
		{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"_id",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(0)),
			),
		},
		{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"int32",
				api_service_protos.TPredicate_TComparison_GE,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(64)),
			),
		},
	}

	for _, testCase := range testCases {
		s.ValidateTable(
			s.dataSource,
			tables["missing_12"],
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
	s.SetDefaultOptions()

	s.ValidateTable(
		s.dataSource,
		tables["missing_1"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateBoolExpressionColumn("boolean"),
		}),
	)
}

func (s *Suite) TestPushdownBetween() {
	s.SetDefaultOptions()

	s.ValidateTable(
		s.dataSource,
		tables["similar_234"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: tests_utils.MakePredicateBetweenColumn(
				"_id",
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(2)),
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(4)),
			),
		}),
	)
}

func (s *Suite) TestPushdownIn() {
	s.SetDefaultOptions()

	testCases := map[string][]*Ydb.TypedValue{
		"_id": {
			common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(1)),
			common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(4)),
			common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(6)),
		},
		"b": {
			common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UTF8), "hi"),
			common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UTF8), "two"),
			common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UTF8), "four"),
		},
	}

	for column, valueSet := range testCases {
		s.ValidateTable(
			s.dataSource,
			tables["similar_146"],
			suite.WithPredicate(&api_service_protos.TPredicate{
				Payload: tests_utils.MakePredicateInColumn(
					column, valueSet,
				),
			}),
		)
	}
}

func (s *Suite) TestPushdownRegex() {
	s.SetDefaultOptions()

	toastPatterns := []string{
		"toast",
		".+ast?",
		"t.{4}",
	}

	for _, pattern := range toastPatterns {
		s.ValidateTable(
			s.dataSource,
			tables["simple_last"],
			suite.WithPredicate(&api_service_protos.TPredicate{
				Payload: tests_utils.MakePredicateRegexpColumn("a", pattern),
			}),
		)
	}
}

func (s *Suite) TestPushdownWithCoalesce() {
	s.SetDefaultOptions()

	// SELECT * FROM missing WHERE COALESCE('int32', 12) < 60;
	predicate := &api_service_protos.TPredicate_Comparison{
		Comparison: &api_service_protos.TPredicate_TComparison{
			LeftValue: &api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_Coalesce{
					Coalesce: &api_service_protos.TExpression_TCoalesce{
						Operands: []*api_service_protos.TExpression{
							{
								Payload: &api_service_protos.TExpression_Column{Column: "int32"},
							},
							{
								Payload: &api_service_protos.TExpression_TypedValue{
									TypedValue: common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(12)),
								},
							},
						},
					},
				},
			},
			Operation: api_service_protos.TPredicate_TComparison_L,
			RightValue: &api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_TypedValue{
					TypedValue: common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_INT32), int32(60)),
				},
			},
		},
	}

	s.ValidateTable(
		s.dataSource,
		tables["missing_12"],
		suite.WithPredicate(&api_service_protos.TPredicate{
			Payload: predicate,
		}),
	)
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
