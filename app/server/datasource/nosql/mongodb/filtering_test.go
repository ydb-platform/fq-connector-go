package mongodb

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
	tests_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
)

func TestYqlStringFilter(t *testing.T) {
	logger := common.NewDefaultLogger()
	maySuppressConjunctionErrors := false

	validHexString := "171e75500ecde1c75c59139e"
	invalidHexString := "171"

	hexEncodedValid, _ := hex.DecodeString(validHexString)
	hexEncodedInvalid, _ := hex.DecodeString(invalidHexString)

	objectId, _ := primitive.ObjectIDFromHex(validHexString)

	testCases := map[*api_service_protos.TPredicate]bson.D{
		{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"a",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), hexEncodedValid),
			),
		}: {{Key: "$expr",
			Value: bson.D{{Key: "$or", Value: []bson.D{
				{{Key: "$eq", Value: bson.A{"$a", hexEncodedValid}}},
				{{Key: "$eq", Value: bson.A{"$a", objectId}}},
			}}},
		}},
		{
			Payload: tests_utils.MakePredicateComparisonColumn(
				"a",
				api_service_protos.TPredicate_TComparison_EQ,
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), hexEncodedInvalid),
			),
		}: {{Key: "$expr",
			Value: bson.D{{Key: "$or", Value: []bson.D{
				{{Key: "$eq", Value: bson.A{"$a", hexEncodedInvalid}}},
			}}},
		}},
		{
			Payload: tests_utils.MakePredicateInColumn(
				"a",
				[]*Ydb.TypedValue{
					common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), hexEncodedValid),
					common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), hexEncodedInvalid),
					common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_BOOL), false),
				},
			),
		}: {{Key: "a",
			Value: bson.D{{Key: "$in", Value: []any{
				hexEncodedValid,
				objectId,
				hexEncodedInvalid,
				false,
			}}},
		}},
		{
			Payload: tests_utils.MakePredicateBetweenColumn(
				"a",
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), hexEncodedValid),
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), hexEncodedInvalid),
			),
		}: {{Key: "$or",
			Value: []bson.D{
				{{Key: "a", Value: bson.D{
					{Key: "$gte", Value: hexEncodedValid},
					{Key: "$lte", Value: hexEncodedInvalid},
				}}},
				{{Key: "a", Value: bson.D{
					{Key: "$gte", Value: objectId},
					{Key: "$lte", Value: hexEncodedInvalid},
				}}},
			},
		}},
		{
			Payload: tests_utils.MakePredicateBetweenColumn(
				"a",
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), hexEncodedValid),
				common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), hexEncodedValid),
			),
		}: {{Key: "$or",
			Value: []bson.D{
				{{Key: "a", Value: bson.D{
					{Key: "$gte", Value: hexEncodedValid},
					{Key: "$lte", Value: hexEncodedValid},
				}}},
				{{Key: "a", Value: bson.D{
					{Key: "$gte", Value: objectId},
					{Key: "$lte", Value: objectId},
				}}},
			},
		}},
		{
			// SELECT * FROM object_ids WHERE COALESCE("$b", hexEncodedValid, hexEncodedValid) = a;
			Payload: &api_service_protos.TPredicate_Comparison{
				Comparison: &api_service_protos.TPredicate_TComparison{
					LeftValue: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_Column{Column: "a"},
					},
					Operation: api_service_protos.TPredicate_TComparison_EQ,
					RightValue: &api_service_protos.TExpression{
						Payload: &api_service_protos.TExpression_Coalesce{
							Coalesce: &api_service_protos.TExpression_TCoalesce{
								Operands: []*api_service_protos.TExpression{
									{
										Payload: &api_service_protos.TExpression_Column{Column: "b"},
									},
									{
										Payload: &api_service_protos.TExpression_TypedValue{
											TypedValue: common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), hexEncodedValid),
										},
									},
									{
										Payload: &api_service_protos.TExpression_TypedValue{
											TypedValue: common.MakeTypedValue(common.MakePrimitiveType(Ydb.Type_STRING), hexEncodedValid),
										},
									},
								},
							},
						},
					},
				},
			},
		}: {{Key: "$expr",
			Value: bson.D{{Key: "$or", Value: []bson.D{
				{{Key: "$eq", Value: bson.A{"$a", bson.D{{Key: "$ifNull", Value: []any{"$b", hexEncodedValid, hexEncodedValid}}}}}},
				{{Key: "$eq", Value: bson.A{"$a", bson.D{{Key: "$ifNull", Value: []any{"$b", objectId, objectId}}}}}},
			}}},
		}},
	}

	for fromPredicate, toFilter := range testCases {
		filter, err := makePredicateFilter(
			logger,
			fromPredicate,
			maySuppressConjunctionErrors,
		)

		if assert.NoError(t, err) {
			assert.Equal(t, filter, toFilter)
		}
	}
}
