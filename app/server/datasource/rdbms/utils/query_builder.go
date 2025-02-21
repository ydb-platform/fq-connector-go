package utils

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type ReadSplitsQuery struct {
	QueryParams
	// Types of the columns that will be returned by the query in terms of YDB type system.
	YDBTypes []*Ydb.Type
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

	ydbTypes, err := common.SelectWhatToYDBTypes(newSelectWhat)
	if err != nil {
		return nil, fmt.Errorf("convert Select.What to Ydb types: %w", err)
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
		YDBTypes: ydbTypes,
	}, nil
}
