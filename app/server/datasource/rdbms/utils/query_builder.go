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
) (*ReadSplitsQuery, error) {
	selectPart, newSelectWhat, err := formatSelectHead(
		ctx,
		logger,
		formatter,
		slct.GetWhat(),
		slct.GetFrom().GetTable(),
		slct.DataSourceInstance,
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

		clause, queryArgs, err = formatWhereClause(formatter, slct.Where)
		if err != nil {
			switch filtering {
			case api_service_protos.TReadSplitsRequest_FILTERING_UNSPECIFIED, api_service_protos.TReadSplitsRequest_FILTERING_OPTIONAL:
				// Pushdown error is suppressed in this mode. Connector will ask for table full scan,
				// and it's YDB is in charge for appropriate filtering
				logger.Warn("Failed to format WHERE clause", zap.Error(err), zap.String("where", slct.Where.String()))
			case api_service_protos.TReadSplitsRequest_FILTERING_MANDATORY:
				// Pushdown is mandatory in this mode.
				// If connector doesn't support some types or expressions, the request will fail.
				return nil, fmt.Errorf("failed to format WHERE clause: %w", err)
			default:
				return nil, fmt.Errorf("unknown filtering mode: %d", filtering)
			}
		} else {
			sb.WriteString(" ")
			sb.WriteString(clause)
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
