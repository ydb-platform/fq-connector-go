package logging

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	ydb "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	rdbms_ydb "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/fq-connector-go/library/go/ptr"
)

func mustParseISOTime(timeStr string) time.Time {
	out, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		panic(err)
	}

	return out
}

//nolint:lll
func TestMakeSelectQuery(t *testing.T) {
	type testCase struct {
		testName         string
		selectReq        *api_service_protos.TSelect
		outputQuery      string
		outputArgs       []any
		outputYdbTypes   []*ydb.Type
		splitDescription *TSplitDescription
		err              error
	}

	logger := common.NewTestLogger(t)
	formatter := NewSQLFormatter(rdbms_ydb.NewSQLFormatter(
		config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE,
		&config.TPushdownConfig{
			EnableTimestampPushdown: true,
		},
	))

	tcs := []testCase{
		//nolint:revive
		{
			/*
				SELECT *
				FROM external_data_source.`cloud-trail`
				WHERE timestamp >= Timestamp("2025-05-06T16:00:00Z")
				  AND timestamp <= Timestamp("2025-05-06T16:00:01Z")
				  AND LEVEL = 'ERROR';
			*/
			testName: "YQ-4277",
			selectReq: rdbms_utils.MustTSelectFromLoggerOutput(
				"{\"table\":\"cloud-trail\"}",
				"{\"items\":[{\"column\":{\"name\":\"level\", \"type\":{\"optional_type\":{\"item\":{\"type_id\":\"UTF8\"}}}}}, {\"column\":{\"name\":\"message\", \"type\":{\"optional_type\":{\"item\":{\"type_id\":\"UTF8\"}}}}}, {\"column\":{\"name\":\"meta\", \"type\":{\"optional_type\":{\"item\":{\"type_id\":\"JSON\"}}}}}, {\"column\":{\"name\":\"timestamp\", \"type\":{\"optional_type\":{\"item\":{\"type_id\":\"TIMESTAMP\"}}}}}]}",
				"{\"filter_typed\":{\"conjunction\":{\"operands\":[{\"coalesce\":{\"operands\":[{\"comparison\":{\"operation\":\"GE\", \"left_value\":{\"column\":\"timestamp\"}, \"right_value\":{\"typed_value\":{\"type\":{\"type_id\":\"TIMESTAMP\"}, \"value\":{\"int64_value\":\"1746547200000000\"}}}}}, {\"bool_expression\":{\"value\":{\"typed_value\":{\"type\":{\"type_id\":\"BOOL\"}, \"value\":{\"bool_value\":false}}}}}]}}, {\"coalesce\":{\"operands\":[{\"comparison\":{\"operation\":\"LE\", \"left_value\":{\"column\":\"timestamp\"}, \"right_value\":{\"typed_value\":{\"type\":{\"type_id\":\"TIMESTAMP\"}, \"value\":{\"int64_value\":\"1746547201000000\"}}}}}, {\"bool_expression\":{\"value\":{\"typed_value\":{\"type\":{\"type_id\":\"BOOL\"}, \"value\":{\"bool_value\":false}}}}}]}}, {\"coalesce\":{\"operands\":[{\"comparison\":{\"operation\":\"EQ\", \"left_value\":{\"column\":\"level\"}, \"right_value\":{\"typed_value\":{\"type\":{\"type_id\":\"STRING\"}, \"value\":{\"bytes_value\":\"RVJST1I=\"}}}}}, {\"bool_expression\":{\"value\":{\"typed_value\":{\"type\":{\"type_id\":\"BOOL\"}, \"value\":{\"bool_value\":false}}}}}]}}]}}}",
				api_common.EGenericDataSourceKind_LOGGING,
			),
			splitDescription: &TSplitDescription{
				Payload: &TSplitDescription_Ydb{
					Ydb: &TSplitDescription_TYdb{
						DatabaseName: "/pre-prod_vla/yc.logs.cloud/cc8jliaf18k2b9ae2bio",
						TableName:    "logs/origin/aoeoqusjtbo4m549jrom/aoe3cidh5dfee2s6cqu5/af3731rdp83d8gd8fjcv",
						TabletIds:    []uint64{72075186234644944},
					},
				},
			},
			outputQuery: "SELECT `level`, `message`, `json_payload`, `timestamp` FROM `logs/origin/aoeoqusjtbo4m549jrom/aoe3cidh5dfee2s6cqu5/af3731rdp83d8gd8fjcv` WITH TabletId='72075186234644944' WHERE (COALESCE((`timestamp` >= $p0), false) AND COALESCE((`timestamp` <= $p1), false) AND COALESCE((`level` = $p2), false))",
			outputArgs: []any{
				mustParseISOTime("2025-05-06T16:00:00Z"),
				mustParseISOTime("2025-05-06T16:00:01Z"),
				ptr.Int32(5), // stands for ERROR
			},
			outputYdbTypes: []*ydb.Type{
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_UTF8)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_UTF8)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_JSON)),
				common.MakeOptionalType(common.MakePrimitiveType(ydb.Type_TIMESTAMP)),
			},
			err: nil,
		},
	}

	for _, tc := range tcs {
		tc := tc

		t.Run(tc.testName, func(t *testing.T) {
			splitDescriptionBytes, err := protojson.Marshal(tc.splitDescription)
			require.NoError(t, err)

			readSplitsQuery, err := rdbms_utils.MakeSelectQuery(
				context.Background(),
				logger,
				formatter,
				&api_service_protos.TSplit{
					Select: tc.selectReq,
					Payload: &api_service_protos.TSplit_Description{
						Description: splitDescriptionBytes,
					},
				},
				api_service_protos.TReadSplitsRequest_FILTERING_OPTIONAL,
				tc.splitDescription.GetYdb().GetTableName(),
			)
			if tc.err != nil {
				require.True(t, errors.Is(err, tc.err))
				return
			}

			require.NoError(t, err)

			require.Equal(t, tc.outputQuery, readSplitsQuery.QueryText)

			require.Equal(t, len(tc.outputArgs), readSplitsQuery.QueryArgs.Count())
			for i := range tc.outputArgs {
				require.Equal(t, tc.outputArgs[i], readSplitsQuery.QueryArgs.Values()[i])
			}

			for i, ydbType := range tc.outputYdbTypes {
				require.Equal(
					t, ydbType, readSplitsQuery.YdbColumns[i].Type,
					fmt.Sprintf("unequal types at index %d: expected=%v, actual=%v", i, ydbType, readSplitsQuery.YdbColumns[i].Type),
				)
			}
		})
	}
}
