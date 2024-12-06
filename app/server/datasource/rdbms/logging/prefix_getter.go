package logging

import (
	"context"
	"fmt"
	"path"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_ydb "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

var _ rdbms_ydb.PrefixGetter = (*prefixGetter)(nil)

type prefixGetter struct {
	resolver Resolver
}

func (p *prefixGetter) GetPrefix(
	ctx context.Context,
	logger *zap.Logger,
	_ *ydb.Driver,
	request *api_service_protos.TDescribeTableRequest,
) (string, error) {
	params := &resolveParams{
		ctx:          ctx,
		logger:       logger,
		folderId:     request.DataSourceInstance.GetLoggingOptions().GetFolderId(),
		logGroupName: request.Table,
	}

	response, err := p.resolver.resolve(params)
	if err != nil {
		return "", fmt.Errorf("resolve log group: %w", err)
	}

	return path.Join(response.databaseName, response.tableName), nil

}

func NewPrefixGetter(resolver Resolver) rdbms_ydb.PrefixGetter {
	return &prefixGetter{resolver: resolver}
}
