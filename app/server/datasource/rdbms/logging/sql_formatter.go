package logging

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

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

func (s sqlFormatter) TransformSelectWhat(src *api_service_protos.TSelect_TWhat) *api_service_protos.TSelect_TWhat {
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

func NewSQLFormatter(ydbSQLFormatter ydb.SQLFormatter) rdbms_utils.SQLFormatter {
	return &sqlFormatter{
		SQLFormatter: ydbSQLFormatter,
	}
}
