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
	tableName string,
) (*SelectQuery, error) {
	var (
		parts        SelectQueryParts
		modifiedWhat *api_service_protos.TSelect_TWhat
		err          error
	)

	// Render SELECT clause
	parts.SelectClause, modifiedWhat, err = formatSelectClause(formatter, split.Select.What, true)
	if err != nil {
		return nil, fmt.Errorf("format select clause: %w", err)
	}

	ydbTypes, err := common.SelectWhatToYDBTypes(modifiedWhat)
	if err != nil {
		return nil, fmt.Errorf("convert Select.What to Ydb types: %w", err)
	}

	// Render FROM clause
	if tableName == "" {
		return nil, common.ErrEmptyTableName
	}

	parts.FromClause = formatter.FormatFrom(tableName)

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

	// FIXME: remove after debugging
	queryText =
		"SELECT `index`, `ingested_at`, `json_payload`, `level`, `message`, `offset`, `partition`, `request_id`, `resource_id`, `resource_type`, `saved_at`, `stream_name`, `timestamp` " +
			"FROM `logs/origin/aoeoqusjtbo4m549jrom/aoe3cidh5dfee2s6cqu5/af3731rdp83d8gd8fjcv` " +
			"WHERE (COALESCE((`timestamp` >= Timestamp('2025-03-17T18:00:00Z')), false) AND COALESCE((`timestamp` <= Timestamp('2025-03-17T18:00:00Z')), false))"
	queryArgs = nil

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
