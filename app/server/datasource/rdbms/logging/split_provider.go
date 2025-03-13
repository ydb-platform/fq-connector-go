package logging

import (
	"context"
	"fmt"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"go.uber.org/zap"
)

var _ rdbms_utils.SplitProvider = (*splitProviderImpl)(nil)

type splitProviderImpl struct {
	resolver Resolver
}

func (s *splitProviderImpl) ListSplits(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	request *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult,
) error {
	// Turn log group name into physical YDB endpoints
	// via static config or Cloud Logging API call.
	params := &resolveParams{
		ctx:          ctx,
		logger:       logger,
		folderId:     slct.DataSourceInstance.GetLoggingOptions().GetFolderId(),
		logGroupName: slct.From.Table,
		credentials:  slct.DataSourceInstance.GetCredentials(),
	}

	response, err := s.resolver.resolve(params)
	if err != nil {
		return fmt.Errorf("resolve YDB endpoint: %w", err)
	}

	for _, src := range response.sources {
		split := &datasource.ListSplitResult{
			Slct: slct,
			Description: &TSplitDescription{
				Payload: &TSplitDescription_Ydb{
					Ydb: &TSplitDescription_TYdb{
						Endpoint:     src.endpoint,
						DatabaseName: src.databaseName,
						TableName:    src.tableName,
					},
				},
			},
		}

		select {
		case resultChan <- split:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func NewSplitProvider(resolver Resolver) rdbms_utils.SplitProvider {
	return &splitProviderImpl{
		resolver: resolver,
	}
}
