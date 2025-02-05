package utils

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

type ReadSplitsQuery struct {
	QueryParams
	What *api_service_protos.TSelect_TWhat
}

func MakeReadSplitsQuery(
	ctx context.Context,
	logger *zap.Logger,
	formatter SQLFormatter,
	slct *api_service_protos.TSelect,
	filtering api_service_protos.TReadSplitsRequest_EFiltering,
	databaseName, tableName string,
) (*ReadSplitsQuery, error) {
	selectPart, newSelectWhat, err := formatSelectHead(
		formatter,
		slct.GetWhat(),
		databaseName,
		tableName,
		true,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to format select statement: %w", err)
	}

	var (
		sb        strings.Builder
		queryArgs *QueryArgs
	)

	sb.WriteString(selectPart)

	if slct.Where != nil {
		var clause string

		clause, queryArgs, err = formatWhereClause(logger, filtering, formatter, slct.Where)
		if err != nil {
			return nil, fmt.Errorf("format where clause: %w", err)
		}

		if len(clause) != 0 {
			sb.WriteString(" WHERE " + clause)
		}
	}

	queryText := sb.String()

	return &ReadSplitsQuery{
		QueryParams: QueryParams{
			Ctx:       ctx,
			Logger:    logger,
			QueryText: queryText,
			QueryArgs: queryArgs,
		},
		What: newSelectWhat,
	}, nil
}
