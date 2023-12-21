package utils

import (
	"fmt"
	"strings"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/library/go/core/log"
)

func MakeReadSplitQuery(logger log.Logger, formatter SQLFormatter, request *api_service_protos.TSelect) (string, []any, error) {
	var (
		sb   strings.Builder
		args []any
	)

	selectPart, err := formatSelectColumns(formatter, request.What, request.GetFrom().GetTable(), true)
	if err != nil {
		return "", nil, fmt.Errorf("failed to format select statement: %w", err)
	}

	sb.WriteString(selectPart)

	if request.Where != nil {
		var clause string

		clause, args, err = formatWhereClause(formatter, request.Where)
		if err != nil {
			logger.Error("Failed to format WHERE clause", log.Error(err), log.String("where", request.Where.String()))
		} else {
			sb.WriteString(" ")
			sb.WriteString(clause)
		}
	}

	query := sb.String()

	if args == nil {
		args = []any{}
	}

	return query, args, nil
}
