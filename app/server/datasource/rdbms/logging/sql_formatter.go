package logging

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
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

func NewSQLFormatter(ydbSQLFormatter ydb.SQLFormatter) rdbms_utils.SQLFormatter {
	return &sqlFormatter{
		SQLFormatter: ydbSQLFormatter,
	}
}
