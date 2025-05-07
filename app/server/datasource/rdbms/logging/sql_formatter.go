package logging

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

var _ rdbms_utils.SQLFormatter = (*sqlFormatter)(nil)

type sqlFormatter struct {
	ydb.SQLFormatter
}

func (s sqlFormatter) RenderSelectQueryText(
	parts *rdbms_utils.SelectQueryParts,
	split *api_service_protos.TSplit,
) (string, error) {
	var dst TSplitDescription

	if err := protojson.Unmarshal(split.GetDescription(), &dst); err != nil {
		return "", fmt.Errorf("unmarshal split description: %w", err)
	}

	return s.RenderSelectQueryTextForColumnShard(parts, dst.GetYdb().TabletIds)
}

func (sqlFormatter) TransformSelectWhat(src *api_service_protos.TSelect_TWhat) *api_service_protos.TSelect_TWhat {
	dst := &api_service_protos.TSelect_TWhat{}

	for _, item := range src.GetItems() {
		name := item.GetColumn().Name
		switch name {
		case metaColumnName:
			dst.Items = append(dst.Items, &api_service_protos.TSelect_TWhat_TItem{
				Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
					Column: &Ydb.Column{
						Name: "json_payload",
						Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_JSON)),
					},
				},
			})
		case levelColumnName:
			dst.Items = append(dst.Items, &api_service_protos.TSelect_TWhat_TItem{
				Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
					Column: &Ydb.Column{
						Name: "level",
						Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
					},
				},
			})
		default:
			dst.Items = append(dst.Items, item)
		}
	}

	return dst
}

func (sqlFormatter) TransformPredicateComparison(
	src *api_service_protos.TPredicate_TComparison,
) (*api_service_protos.TPredicate_TComparison, error) {
	dst := proto.Clone(src).(*api_service_protos.TPredicate_TComparison)

	// For the comparison related to `level` field
	if src.LeftValue.GetColumn() == levelColumnName && src.RightValue.GetTypedValue() != nil {
		if src.Operation != api_service_protos.TPredicate_TComparison_EQ {
			return nil, fmt.Errorf("unsupported operation %v for `level` column comparison", src.Operation)
		}

		// Extract filter value of a string type
		var levelValue string
		switch src.RightValue.GetTypedValue().GetType().GetTypeId() {
		case Ydb.Type_UTF8:
			levelValue = src.RightValue.GetTypedValue().GetValue().GetTextValue()
		case Ydb.Type_STRING:
			levelValue = string(src.RightValue.GetTypedValue().GetValue().GetBytesValue())
		default:
			return nil, fmt.Errorf(
				"unsupported typed value of type %v for `level` column comparison",
				src.RightValue.GetTypedValue().GetType(),
			)
		}

		// Replace it with number representation
		switch levelValue {
		case levelTraceValue:
			dst.RightValue.Payload = makeTypedValueForLevel(1)
		case levelDebugValue:
			dst.RightValue.Payload = makeTypedValueForLevel(2)
		case levelInfoValue:
			dst.RightValue.Payload = makeTypedValueForLevel(3)
		case levelWarnValue:
			dst.RightValue.Payload = makeTypedValueForLevel(4)
		case levelErrorValue:
			dst.RightValue.Payload = makeTypedValueForLevel(5)
		case levelFatalValue:
			dst.RightValue.Payload = makeTypedValueForLevel(6)
		default:
			return nil, fmt.Errorf("unsupported `level` value %s", levelValue)
		}
	}

	return dst, nil
}

func makeTypedValueForLevel(level int32) *api_service_protos.TExpression_TypedValue {
	return &api_service_protos.TExpression_TypedValue{
		TypedValue: common.MakeTypedValue(
			common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_INT32)),
			level,
		),
	}
}

func NewSQLFormatter(ydbSQLFormatter ydb.SQLFormatter) rdbms_utils.SQLFormatter {
	return &sqlFormatter{
		SQLFormatter: ydbSQLFormatter,
	}
}
