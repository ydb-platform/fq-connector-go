package utils

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

func MakePredicateComparisonColumn(
	columnName string,
	operation api_service_protos.TPredicate_TComparison_EOperation,
	value *Ydb.TypedValue,
) *api_service_protos.TPredicate_Comparison {
	return &api_service_protos.TPredicate_Comparison{
		Comparison: &api_service_protos.TPredicate_TComparison{
			LeftValue: &api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_Column{Column: columnName},
			},
			Operation: operation,
			RightValue: &api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_TypedValue{
					TypedValue: value,
				},
			},
		},
	}
}

func MakePredicateIsNullColumn(columnName string) *api_service_protos.TPredicate_IsNull {
	return &api_service_protos.TPredicate_IsNull{
		IsNull: &api_service_protos.TPredicate_TIsNull{Value: &api_service_protos.TExpression{
			Payload: &api_service_protos.TExpression_Column{
				Column: columnName,
			},
		}},
	}
}

func MakePredicateIsNotNullColumn(columnName string) *api_service_protos.TPredicate_IsNotNull {
	return &api_service_protos.TPredicate_IsNotNull{
		IsNotNull: &api_service_protos.TPredicate_TIsNotNull{Value: &api_service_protos.TExpression{
			Payload: &api_service_protos.TExpression_Column{
				Column: columnName,
			},
		}},
	}
}

func MakePredicateBoolExpressionColumn(columnName string) *api_service_protos.TPredicate_BoolExpression {
	return &api_service_protos.TPredicate_BoolExpression{
		BoolExpression: &api_service_protos.TPredicate_TBoolExpression{
			Value: &api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_Column{
					Column: columnName,
				},
			},
		},
	}
}

func MakePredicateBetweenColumn(columnName string, least, greatest *Ydb.TypedValue) *api_service_protos.TPredicate_Between {
	return &api_service_protos.TPredicate_Between{
		Between: &api_service_protos.TPredicate_TBetween{
			Value: &api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_Column{
					Column: columnName,
				},
			},
			Least: &api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_TypedValue{
					TypedValue: least,
				},
			},
			Greatest: &api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_TypedValue{
					TypedValue: greatest,
				},
			},
		},
	}
}

func MakePredicateInColumn(columnName string, values []*Ydb.TypedValue) *api_service_protos.TPredicate_In {
	set := make([]*api_service_protos.TExpression, 0, len(values))
	for _, value := range values {
		set = append(set,
			&api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_TypedValue{
					TypedValue: value,
				},
			},
		)
	}

	return &api_service_protos.TPredicate_In{
		In: &api_service_protos.TPredicate_TIn{
			Value: &api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_Column{
					Column: columnName,
				},
			},
			Set: set,
		},
	}
}

func MakePredicateRegexpColumn(columnName, pattern string) *api_service_protos.TPredicate_Regexp {
	return &api_service_protos.TPredicate_Regexp{
		Regexp: &api_service_protos.TPredicate_TRegexp{
			Value: &api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_Column{
					Column: columnName,
				},
			},
			Pattern: &api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_TypedValue{
					TypedValue: common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_UTF8), pattern),
				},
			},
		},
	}
}

func MakePredicateRegexpIfCastColumn(
	columnName string,
	targetTypeId Ydb.Type_PrimitiveTypeId,
	pattern string,
) *api_service_protos.TPredicate_Regexp {
	return &api_service_protos.TPredicate_Regexp{
		Regexp: &api_service_protos.TPredicate_TRegexp{
			Value: &api_service_protos.TExpression{
				Payload: &api_service_protos.TExpression_If{
					If: &api_service_protos.TExpression_TIf{
						Predicate: &api_service_protos.TPredicate{
							Payload: &api_service_protos.TPredicate_IsNotNull{
								IsNotNull: &api_service_protos.TPredicate_TIsNotNull{
									Value: &api_service_protos.TExpression{
										Payload: &api_service_protos.TExpression_Column{
											Column: columnName,
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
											Column: columnName,
										},
									},
									Type: &Ydb.Type{
										Type: &Ydb.Type_TypeId{
											TypeId: targetTypeId,
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
					TypedValue: common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), []byte(pattern)),
				},
			},
		},
	}
}
