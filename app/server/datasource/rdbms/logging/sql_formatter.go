package logging

import (
	"fmt"
	"strings"

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
	$y = Yson::ParseJson(CAST ($j as STRING));
	$a = DictItems(Yson::ConvertToDict($y));
	$f = ListFilter($a, ($x) -> { return StartsWith($x.0, "labels.") });
	$g = ListMap($f, ($x) -> { return (substring($x.0, 7), $x.1) });
	return Yson::SerializeJson(Yson::From(ToDict($g)));
};

$build_pure_meta = ($j) -> {
	$y = Yson::ParseJson(CAST ($j as STRING));
	$a = DictItems(Yson::ConvertToDict($y));
	$f = ListFilter($a, ($x) -> { return StartsWith($x.0, "meta.")});
	$g = ListMap($f, ($x) -> { return (substring($x.0, 5), $x.1) });
    return $g;
};

$hostname_keys = AsList(
    "host", "hostname", "host.name"
);

$trace_id_keys = AsList(
    "trace.id", "trace_id", "traceId", "traceID",
);

$span_id_keys = AsList(
    "span.id", "span_id", "spanId", "spanID",
);

$excluded_from_meta = ListExtend(
    $hostname_keys,
    $trace_id_keys,
    $span_id_keys
);

$build_other_meta = ($j) -> {
	$y = Yson::ParseJson(CAST ($j as STRING));
	$a = DictItems(Yson::ConvertToDict($y));
	$f = ListFilter($a, ($x) -> { 
        return 
            NOT StartsWith($x.0, "labels.") 
                AND 
            NOT StartsWith($x.0, "meta.")
                AND
            $x.0 NOT IN $excluded_from_meta
    });
	$g = ListMap($f, ($x) -> { return ($x.0, $x.1) });
    return $g;
};

$build_meta = ($j) -> {
    $pure = $build_pure_meta($j);
    $other = $build_other_meta($j);
    return Yson::SerializeJson(Yson::From(ToDict(ListExtend($pure, $other))));
};

$build_hostname = ($j) -> {
    $y = Yson::ParseJson(CAST ($j as STRING));
	$a = DictItems(Yson::ConvertToDict($y));
	$f = ListFilter($a, ($x) -> { return $x.0 IN $hostname_keys });
    return CAST(Yson::ConvertToString($f[0].1) AS Utf8);
};

$build_level = ($src) -> {
    RETURN CAST(
        CASE $src
            WHEN 1 THEN "TRACE"
            WHEN 2 THEN "DEBUG"
            WHEN 3 THEN "INFO"
            WHEN 4 THEN "WARN"
            WHEN 5 THEN "ERROR"
            WHEN 6 THEN "FATAL"
            ELSE "UNKNOWN"
        END AS Utf8
    );
};
`

func (sqlFormatter) FormatWhat(src *api_service_protos.TSelect_TWhat, tableName string) (string, error) {
	items := strings.Split(tableName, "/")
	if len(items) != 5 {
		return "", fmt.Errorf("invalid table name format: %s", tableName)
	}

	var (
		project = items[2]
		cluster = items[3]
		service = items[4]
		buf     strings.Builder
	)

	for i, item := range src.GetItems() {
		switch item.GetColumn().GetName() {
		case clusterColumnName:
			buf.WriteString(fmt.Sprintf("CAST(\"%s\" AS Utf8) AS cluster", cluster))
		case jsonPayloadColumnName:
			buf.WriteString(jsonPayloadColumnName)
		case hostnameColumnName:
			buf.WriteString("$build_hostname(json_payload) AS hostname")
		case labelsColumnName:
			buf.WriteString("$build_labels(json_payload) AS labels")
		case levelColumnName:
			buf.WriteString("$build_level(level) AS level")
		case messageColumnName:
			buf.WriteString(messageColumnName)
		case metaColumnName:
			buf.WriteString("$build_meta(json_payload) AS meta")
		case projectColumnName:
			buf.WriteString(fmt.Sprintf("CAST(\"%s\" AS Utf8) AS project", project))
		case serviceColumnName:
			buf.WriteString(fmt.Sprintf("CAST(\"%s\" AS Utf8) AS service", service))
		case timestampColumnName:
			buf.WriteString(timestampColumnName)
		default:
			return "", fmt.Errorf("unexpected column name: %s", item.GetColumn().Name)
		}

		if i != len(src.GetItems())-1 {
			buf.WriteString(", ")
		}
	}

	return buf.String(), nil
}

func (s sqlFormatter) RenderSelectQueryText(
	parts *rdbms_utils.SelectQueryParts,
	split *api_service_protos.TSplit,
) (string, error) {
	var dst TSplitDescription

	if err := protojson.Unmarshal(split.GetDescription(), &dst); err != nil {
		return "", fmt.Errorf("unmarshal split description: %w", err)
	}

	var sb strings.Builder

	sb.WriteString(queryPrefix)
	sb.WriteString("\n")

	body, err := s.RenderSelectQueryTextForColumnShard(parts, dst.GetYdb().TabletIds)
	if err != nil {
		return "", fmt.Errorf("render select query text for column shard: %w", err)
	}

	sb.WriteString(body)

	return sb.String(), nil
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
