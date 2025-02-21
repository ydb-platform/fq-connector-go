package utils

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

type SelectQuery struct {
	QueryParams
	// Types of the columns that will be returned by the query in terms of YDB type system.
	YdbTypes []*Ydb.Type
}

func MakeSelectQuery(
	ctx context.Context,
	logger *zap.Logger,
	formatter SQLFormatter,
	split *api_service_protos.TSplit,
	filtering api_service_protos.TReadSplitsRequest_EFiltering,
	databaseName, tableName string,
) (*SelectQuery, error) {
	var (
		parts        SelectQueryParts
		modifiedWhat *api_service_protos.TSelect_TWhat
		err          error
	)

	// Render SELECT clause
	parts.SelectClause, modifiedWhat, err = formatSelectClause(formatter, split.Select.What, true)
	if err != nil {
		return nil, fmt.Errorf("failed to format select statement: %w", err)
	}

	ydbTypes, err := common.SelectWhatToYDBTypes(modifiedWhat)
	if err != nil {
		return nil, fmt.Errorf("convert Select.What to Ydb types: %w", err)
	}

	// Render FROM clause
	parts.FromClause = formatter.FormatFrom(databaseName, tableName)

	// Render WHERE clause
	var queryArgs *QueryArgs
	if split.Select.Where != nil {
		parts.WhereClause, queryArgs, err = formatWhereClause(logger, filtering, formatter, split.Select.Where)
		if err != nil {
			return nil, fmt.Errorf("format where clause: %w", err)
		}
	}

	// Render whole query
	queryText, err := formatter.RenderSelectQueryText(&parts, split)
	if err != nil {
		return nil, fmt.Errorf("render query text: %w", err)
	}

	return &SelectQuery{
		QueryParams: QueryParams{
			Ctx:       ctx,
			Logger:    logger,
			QueryText: queryText,
			QueryArgs: queryArgs,
		},
		YdbTypes: ydbTypes,
	}, nil
}
