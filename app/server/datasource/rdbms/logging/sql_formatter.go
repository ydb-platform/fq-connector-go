package logging

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

var _ rdbms_utils.SQLFormatter = (*sqlFormatter)(nil)

type sqlFormatter struct {
	rdbms_utils.SQLFormatter
}

func (sqlFormatter) RenderSelectQueryText(
	parts *rdbms_utils.SelectQueryParts,
	split *api_service_protos.TSplit,
) (string, error) {
	var dst TSplitDescription

	if err := protojson.Unmarshal(split.GetDescription(), &dst); err != nil {
		return "", fmt.Errorf("unmarshal src: %w", err)
	}

	return rdbms_utils.DefaultSelectQueryRender(parts, nil)
}

func NewSQLFormatter(ydbSQLFormatter rdbms_utils.SQLFormatter) rdbms_utils.SQLFormatter {
	return &sqlFormatter{
		SQLFormatter: ydbSQLFormatter,
	}
}
