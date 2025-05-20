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
	// Types and names of the columns that will be returned by the query in terms of YDB type system.
	YdbColumns []*Ydb.Column
}

func MakeSelectQuery(
	ctx context.Context,
	logger *zap.Logger,
	formatter SQLFormatter,
	split *api_service_protos.TSplit,
	filtering api_service_protos.TReadSplitsRequest_EFiltering,
	tableName string,
) (*SelectQuery, error) {
	var (
		parts        SelectQueryParts
		modifiedWhat *api_service_protos.TSelect_TWhat
		err          error
	)

	// Render SELECT clause
	parts.SelectClause, modifiedWhat, err = formatWhat(formatter, split.Select.What)
	if err != nil {
		return nil, fmt.Errorf("format select clause: %w", err)
	}

	ydbColumns := common.SelectWhatToYDBColumns(modifiedWhat)

	// Render FROM clause
	if tableName == "" {
		return nil, common.ErrEmptyTableName
	}

	parts.FromClause = formatter.FormatFrom(tableName)

	// Render WHERE clause
	var queryArgs *QueryArgs
	if split.Select.Where != nil {
		parts.WhereClause, queryArgs, err = formatWhereClause(
			logger,
			filtering,
			formatter,
			split.Select.Where,
			split.Select.DataSourceInstance.Kind,
		)

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
		YdbColumns: ydbColumns,
	}, nil
}
