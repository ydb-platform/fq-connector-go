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
	// Apply necessary transformations to the list of requested items and extract columns
	dst, err := formatter.TransformSelectWhat(src)
	if err != nil {
		return "", nil, fmt.Errorf("transform select what: %w", err)
	}

	columns := common.SelectWhatToYDBColumns(dst)

	// This buffer will hold the part of SELECT query that occures between SELECT and FROM keywords
	var sb strings.Builder

	// If no columns were requested, select some constant to construct valid SQL statement
	if len(columns) == 0 {
		sb.WriteString("0")

		// YQ-3314: is needed only in select COUNT(*) for ydb datasource.
		// 		In PostgreSQL or ClickHouse type_mapper is based on typeNames that are extraceted from column.DatabaseTypeName().
		//		But in ydb type_mapper is based on ydbTypes, that are extracted from TSelect_TWhat
		dst = makeTSelectTWhatForEmptyColumnsRequest()
	} else {
		for i, column := range columns {
			sb.WriteString(formatter.SanitiseIdentifier(column.GetName()))

			if i != len(columns)-1 {
				sb.WriteString(", ")
			}
		}
	}

	return sb.String(), dst, nil
}
