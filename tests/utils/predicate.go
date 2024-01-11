package utils

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
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
