package prometheus_test

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/prometheus"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/common"
)

func TestEmptyBuilder(t *testing.T) {
	logger := common.NewTestLogger(t)

	pbQuery, err := prometheus.NewPromQLBuilder(logger).ToQuery()

	assert.NoError(t, err)
	assert.Empty(t, pbQuery.GetMatchers())
	assert.Nil(t, pbQuery.GetHints())
	assert.Zero(t, pbQuery.GetStartTimestampMs())
	assert.Greater(t, pbQuery.GetEndTimestampMs(), time.Now().UnixMilli())
}

func TestFrom(t *testing.T) {
	logger := common.NewTestLogger(t)

	pbQuery, err := prometheus.NewPromQLBuilder(logger).
		From("some_metric").
		ToQuery()

	assert.NoError(t, err)
	assert.Nil(t, pbQuery.GetHints())
	assert.Zero(t, pbQuery.GetStartTimestampMs())
	assert.Greater(t, pbQuery.GetEndTimestampMs(), time.Now().UnixMilli())
	assert.NotNil(t, pbQuery.GetMatchers())
	assert.Len(t, pbQuery.GetMatchers(), 1)
	assert.Equal(t, &prompb.LabelMatcher{
		Type:  prompb.LabelMatcher_EQ,
		Name:  "__name__",
		Value: "some_metric",
	}, pbQuery.GetMatchers()[0])
}

func TestWithStartTime(t *testing.T) {
	logger := common.NewTestLogger(t)
	startTime := time.UnixMilli(1744537552067)

	pbQuery, err := prometheus.NewPromQLBuilder(logger).
		WithStartTime(startTime).
		ToQuery()

	assert.NoError(t, err)
	assert.Empty(t, pbQuery.GetMatchers())
	assert.Nil(t, pbQuery.GetHints())
	assert.Greater(t, pbQuery.GetEndTimestampMs(), time.Now().UnixMilli())
	assert.Equal(t, startTime.UnixMilli(), pbQuery.GetStartTimestampMs())
}

func TestWithEndTime(t *testing.T) {
	logger := common.NewTestLogger(t)
	endTime := time.UnixMilli(1744537552067)

	pbQuery, err := prometheus.NewPromQLBuilder(logger).
		WithEndTime(endTime).
		ToQuery()

	assert.NoError(t, err)
	assert.Empty(t, pbQuery.GetMatchers())
	assert.Nil(t, pbQuery.GetHints())
	assert.Zero(t, pbQuery.GetStartTimestampMs())
	assert.Equal(t, endTime.UnixMilli(), pbQuery.GetEndTimestampMs())
}

func TestWithYdbWhereNilWhere(t *testing.T) {
	logger := common.NewTestLogger(t)

	builder, err := prometheus.NewPromQLBuilder(logger).WithYdbWhere(nil, 0)

	assert.NoError(t, err)

	pbQuery, err := builder.ToQuery()

	assert.NoError(t, err)
	assert.Empty(t, pbQuery.GetMatchers())
	assert.Nil(t, pbQuery.GetHints())
	assert.Zero(t, pbQuery.GetStartTimestampMs())
	assert.Greater(t, pbQuery.GetEndTimestampMs(), time.Now().UnixMilli())
}

func TestWithYdbWhereNilFilteredType(t *testing.T) {
	logger := common.NewTestLogger(t)

	builder, err := prometheus.NewPromQLBuilder(logger).
		WithYdbWhere(&api_service_protos.TSelect_TWhere{}, 0)

	assert.NoError(t, err)

	pbQuery, err := builder.ToQuery()

	assert.NoError(t, err)
	assert.Empty(t, pbQuery.GetMatchers())
	assert.Nil(t, pbQuery.GetHints())
	assert.Zero(t, pbQuery.GetStartTimestampMs())
	assert.Greater(t, pbQuery.GetEndTimestampMs(), time.Now().UnixMilli())
}

func TestWithYdbWhereUnsupportedPredicate(t *testing.T) {
	logger := common.NewTestLogger(t)
	where := &api_service_protos.TSelect_TWhere{
		FilterTyped: &api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Coalesce{}, // Unsupported now
		},
	}

	//
	// Without filtering mandatory parsing
	//
	builder, err := prometheus.NewPromQLBuilder(logger).
		WithYdbWhere(where, 0)

	assert.NoError(t, err)

	pbQuery, err := builder.ToQuery()

	assert.NoError(t, err)
	assert.Empty(t, pbQuery.GetMatchers())
	assert.Nil(t, pbQuery.GetHints())
	assert.Zero(t, pbQuery.GetStartTimestampMs())
	assert.Greater(t, pbQuery.GetEndTimestampMs(), time.Now().UnixMilli())

	//
	// With filtering mandatory parsing
	//
	_, err = prometheus.NewPromQLBuilder(logger).
		WithYdbWhere(where, api_service_protos.TReadSplitsRequest_FILTERING_MANDATORY)

	assert.ErrorIs(t, err, common.ErrUnimplementedPredicateType)
}

func TestWithYdbWhereInvalidComparison(t *testing.T) {
	logger := common.NewTestLogger(t)
	where := &api_service_protos.TSelect_TWhere{
		FilterTyped: &api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Comparison{
				Comparison: &api_service_protos.TPredicate_TComparison{
					Operation: api_service_protos.TPredicate_TComparison_COMPARISON_OPERATION_UNSPECIFIED, // invalid op
				},
			},
		},
	}

	//
	// Without filtering mandatory parsing
	//
	_, err := prometheus.NewPromQLBuilder(logger).
		WithYdbWhere(where, 0)

	assert.ErrorIs(t, err, common.ErrInvalidRequest)

	//
	// With filtering mandatory parsing
	//
	_, err = prometheus.NewPromQLBuilder(logger).
		WithYdbWhere(where, api_service_protos.TReadSplitsRequest_FILTERING_MANDATORY)

	assert.ErrorIs(t, err, common.ErrInvalidRequest)
}

func TestWithYdbWhereUnsupportedExpression(t *testing.T) {
	logger := common.NewTestLogger(t)
	where := &api_service_protos.TSelect_TWhere{
		FilterTyped: &api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Comparison{
				Comparison: &api_service_protos.TPredicate_TComparison{
					Operation:  api_service_protos.TPredicate_TComparison_LE,
					LeftValue:  utils.NewInt32ValueExpression(1),
					RightValue: utils.NewColumnExpression(""),
				},
			},
		},
	}

	//
	// Without filtering mandatory parsing
	//
	builder, err := prometheus.NewPromQLBuilder(logger).
		WithYdbWhere(where, 0)

	assert.NoError(t, err)

	pbQuery, err := builder.ToQuery()

	assert.NoError(t, err)
	assert.Empty(t, pbQuery.GetMatchers())
	assert.Nil(t, pbQuery.GetHints())
	assert.Zero(t, pbQuery.GetStartTimestampMs())
	assert.Greater(t, pbQuery.GetEndTimestampMs(), time.Now().UnixMilli())

	//
	// With filtering mandatory parsing
	//
	_, err = prometheus.NewPromQLBuilder(logger).
		WithYdbWhere(where, api_service_protos.TReadSplitsRequest_FILTERING_MANDATORY)

	assert.ErrorIs(t, err, common.ErrUnsupportedExpression)
}

func TestWithYdbWhereOneExpression(t *testing.T) {
	logger := common.NewTestLogger(t)
	timestamp := time.UnixMilli(1744537552067)

	where := &api_service_protos.TSelect_TWhere{
		FilterTyped: &api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Comparison{
				Comparison: &api_service_protos.TPredicate_TComparison{
					Operation:  api_service_protos.TPredicate_TComparison_LE,
					LeftValue:  utils.NewColumnExpression("timestamp"),
					RightValue: utils.NewTimestampExpression(uint64(timestamp.UnixMicro())),
				},
			},
		},
	}

	//
	// Without filtering mandatory parsing
	//
	builder, err := prometheus.NewPromQLBuilder(logger).
		WithYdbWhere(where, 0)

	assert.NoError(t, err)

	pbQuery, err := builder.ToQuery()

	assert.NoError(t, err)
	assert.Empty(t, pbQuery.GetMatchers())
	assert.Nil(t, pbQuery.GetHints())
	assert.Zero(t, pbQuery.GetStartTimestampMs())
	assert.Equal(t, timestamp.UnixMilli(), pbQuery.GetEndTimestampMs())

	//
	// With filtering mandatory parsing
	//
	builder, err = prometheus.NewPromQLBuilder(logger).
		WithYdbWhere(where, api_service_protos.TReadSplitsRequest_FILTERING_MANDATORY)

	assert.NoError(t, err)

	pbQuery, err = builder.ToQuery()

	assert.NoError(t, err)
	assert.Empty(t, pbQuery.GetMatchers())
	assert.Nil(t, pbQuery.GetHints())
	assert.Zero(t, pbQuery.GetStartTimestampMs())
	assert.Equal(t, timestamp.UnixMilli(), pbQuery.GetEndTimestampMs())
}

func TestWithYdbWhereFullExpression(t *testing.T) {
	logger := common.NewTestLogger(t)
	timestamp := time.UnixMilli(1744537552067)
	expectedLabels := []*prompb.LabelMatcher{
		{
			Type:  prompb.LabelMatcher_EQ,
			Name:  "label",
			Value: "wow",
		},
	}

	where := &api_service_protos.TSelect_TWhere{
		FilterTyped: &api_service_protos.TPredicate{
			Payload: &api_service_protos.TPredicate_Conjunction{
				Conjunction: &api_service_protos.TPredicate_TConjunction{
					Operands: []*api_service_protos.TPredicate{
						{
							Payload: &api_service_protos.TPredicate_Comparison{
								Comparison: &api_service_protos.TPredicate_TComparison{
									Operation:  api_service_protos.TPredicate_TComparison_G,
									LeftValue:  utils.NewColumnExpression("timestamp"),
									RightValue: utils.NewTimestampExpression(uint64(timestamp.UnixMicro())),
								},
							},
						},
						{
							Payload: &api_service_protos.TPredicate_Comparison{
								Comparison: &api_service_protos.TPredicate_TComparison{
									Operation:  api_service_protos.TPredicate_TComparison_L,
									LeftValue:  utils.NewColumnExpression("timestamp"),
									RightValue: utils.NewTimestampExpression(uint64(timestamp.Add(10 * time.Second).UnixMicro())),
								},
							},
						},
						{
							Payload: &api_service_protos.TPredicate_Comparison{
								Comparison: &api_service_protos.TPredicate_TComparison{
									Operation:  api_service_protos.TPredicate_TComparison_EQ,
									LeftValue:  utils.NewColumnExpression("label"),
									RightValue: utils.NewStringValueExpression("wow"),
								},
							},
						},
					},
				},
			},
		},
	}

	//
	// Without filtering mandatory parsing
	//
	builder, err := prometheus.NewPromQLBuilder(logger).
		WithYdbWhere(where, 0)

	assert.NoError(t, err)

	pbQuery, err := builder.ToQuery()

	assert.NoError(t, err)
	assert.ElementsMatch(t, expectedLabels, pbQuery.GetMatchers())
	assert.Nil(t, pbQuery.GetHints())
	assert.Equal(t, timestamp.UnixMilli()+1, pbQuery.GetStartTimestampMs())
	assert.Equal(t, timestamp.Add(10*time.Second).UnixMilli()-1, pbQuery.GetEndTimestampMs())

	//
	// With filtering mandatory parsing
	//
	builder, err = prometheus.NewPromQLBuilder(logger).
		WithYdbWhere(where, api_service_protos.TReadSplitsRequest_FILTERING_MANDATORY)

	assert.NoError(t, err)

	pbQuery, err = builder.ToQuery()

	assert.NoError(t, err)
	assert.ElementsMatch(t, expectedLabels, pbQuery.GetMatchers())
	assert.Nil(t, pbQuery.GetHints())
	assert.Equal(t, timestamp.UnixMilli()+1, pbQuery.GetStartTimestampMs())
	assert.Equal(t, timestamp.Add(10*time.Second).UnixMilli()-1, pbQuery.GetEndTimestampMs())
}
