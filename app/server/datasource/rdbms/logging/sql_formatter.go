package logging

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
)

type sqlFormatter struct {
	rdbms_utils.SQLFormatter
}

func (sqlFormatter) RenderSelectQueryText(
	parts *rdbms_utils.SelectQueryParts,
	split *api_service_protos.TSplit,
) (string, error) {
	// Deserialize split description
	var splitDescription TSplitDescription
	if err := protojson.Unmarshal(split.GetDescription(), &splitDescription); err != nil {
		return "", fmt.Errorf("unmarshal split description: %w", err)
	}

	// WITH(ShardId="72075186224054918")
	head, err := rdbms_utils.DefaultSelectQueryRender(parts, split)
	if err != nil {
		return "", fmt.Errorf("default select query render: %w", err)
	}

	result := head + fmt.Sprintf(" WITH (ShardId=\"%d\")", splitDescription.ShardIds[0])

	return result, nil
}

func NewSQLFormatter(mode config.TYdbConfig_Mode, cfg *config.TPushdownConfig) rdbms_utils.SQLFormatter {
	return sqlFormatter{
		SQLFormatter: ydb.NewSQLFormatter(mode, cfg),
	}
}
