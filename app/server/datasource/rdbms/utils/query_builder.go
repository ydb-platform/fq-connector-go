package utils

import (
	"fmt"
	"strings"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

func MakeReadSplitQuery(logger *zap.Logger, formatter SQLFormatter, request *api_service_protos.TSelect) (string, []any, *api_service_protos.TSelect_TWhat, error) {
	var (
		sb   strings.Builder
		args []any
	)

	selectPart, newSelectWhat, err := formatSelectColumns(formatter, request.What, request.GetFrom().GetTable(), true)
	if err != nil {
		return "", nil, nil, fmt.Errorf("failed to format select statement: %w", err)
	}

	sb.WriteString(selectPart)

	if request.Where != nil {
		var clause string

		clause, args, err = formatWhereClause(formatter, request.Where)
		if err != nil {
			logger.Error("Failed to format WHERE clause", zap.Error(err), zap.String("where", request.Where.String()))
		} else {
			sb.WriteString(" ")
			sb.WriteString(clause)
		}
	}

	query := sb.String()

	if args == nil {
		args = []any{}
	}

	return query, args, newSelectWhat, nil
}
