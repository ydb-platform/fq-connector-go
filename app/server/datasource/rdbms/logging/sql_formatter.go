package logging

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.SQLFormatter = (*sqlFormatter)(nil)

type sqlFormatter struct {
	ydb.SQLFormatter
}

const queryPrefix = `
	$build_labels = ($j) -> {
		$a = DictItems(Yson::ConvertToDict($j));
		$f = ListFilter($a, ($x) -> { return StartsWith($x.0, "labels.") });
		$g = ListMap($f, ($x) -> { return (substring($x.0, 7), $x.1) });
		return Yson::SerializeJson(Yson::From(ToDict($g)));
	};
`

func (sqlFormatter) FormatSelect(src *api_service_protos.TSelect_TWhat) (string, error) {
	return "", nil
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

func (sqlFormatter) TransformSelectWhat(src *api_service_protos.TSelect_TWhat) (*api_service_protos.TSelect_TWhat, error) {
	dst := &api_service_protos.TSelect_TWhat{}

	// collect internal column names
	internalColumnNamesSet := make(map[string]struct{}, 4)

	for _, item := range src.GetItems() {
		externalName := item.GetColumn().Name

		internalName, ok := externalToInternalColumnName[externalName]
		if !ok {
			return nil, fmt.Errorf("unknown external column name: %s", externalName)
		}

		internalColumnNamesSet[internalName] = struct{}{}
	}

	// get unique internal column names and sort them to increase the degree of determinism
	internalColumnNamesOrdered := make([]string, 0, len(internalColumnNamesSet))

	for internalName := range internalColumnNamesSet {
		internalColumnNamesOrdered = append(internalColumnNamesOrdered, internalName)
	}

	sort.Strings(internalColumnNamesOrdered)

	for _, internalName := range internalColumnNamesOrdered {
		internalType, ok := internalColumnTypes[internalName]

		if !ok {
			return nil, fmt.Errorf("unknown internal column name: %s", internalName)
		}

		dst.Items = append(dst.Items, &api_service_protos.TSelect_TWhat_TItem{
			Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
				Column: &Ydb.Column{
					Name: internalName,
					Type: internalType,
				},
			},
		})
	}

	return dst, nil
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
			common.MakePrimitiveType(Ydb.Type_INT32),
			level,
		),
	}
}

func NewSQLFormatter(ydbSQLFormatter ydb.SQLFormatter) rdbms_utils.SQLFormatter {
	return &sqlFormatter{
		SQLFormatter: ydbSQLFormatter,
	}
}
