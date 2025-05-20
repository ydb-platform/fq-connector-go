package utils

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

func makeTSelectTWhatForEmptyColumnsRequest() *api_service_protos.TSelect_TWhat {
	return &api_service_protos.TSelect_TWhat{
		Items: []*api_service_protos.TSelect_TWhat_TItem{
			{
				Payload: &api_service_protos.TSelect_TWhat_TItem_Column{
					Column: &Ydb.Column{
						Name: "",
						Type: common.MakePrimitiveType(Ydb.Type_INT64),
					},
				},
			},
		},
	}
}

func formatSelectClause(
	formatter SQLFormatter,
	src *api_service_protos.TSelect_TWhat,
) (string, *api_service_protos.TSelect_TWhat, error) {
	columns := common.SelectWhatToYDBColumns(src)

	// If no columns were requested, select some constant to construct valid SQL statement
	if len(columns) == 0 {

		// YQ-3314: is needed only in select COUNT(*) for ydb datasource.
		// 		In PostgreSQL or ClickHouse type_mapper is based on typeNames that are extracted
		// 		from column.DatabaseTypeName().
		//		But in YDB type_mapper is based on YDB types, that are extracted from TSelect_TWhat.
		dst := makeTSelectTWhatForEmptyColumnsRequest()

		return "0", dst, nil
	}

	out, err := formatter.FormatSelect(src)
	if err != nil {
		return "", nil, fmt.Errorf("format select: %w", err)
	}

	return out, src, nil
}

func FormatSelectDefault(formatter SQLFormatter, src *api_service_protos.TSelect_TWhat) string {
	var sb strings.Builder

	for i, item := range src.Items {
		sb.WriteString(formatter.SanitiseIdentifier(item.GetColumn().GetName()))

		if i != len(src.Items)-1 {
			sb.WriteString(", ")
		}
	}

	return sb.String()
}
