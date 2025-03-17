package logging

import (
	"fmt"

	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
)

var _ rdbms_utils.SplitProvider = (*splitProviderImpl)(nil)

type splitProviderImpl struct {
	resolver Resolver
}

func (s *splitProviderImpl) ListSplits(
	params *rdbms_utils.ListSplitsParams,
) error {
	// Turn log group name into physical YDB endpoints
	// via static config or Cloud Logging API call.
	request := &resolveRequest{
		ctx:          params.Ctx,
		logger:       params.Logger,
		folderId:     params.Select.DataSourceInstance.GetLoggingOptions().GetFolderId(),
		logGroupName: params.Select.From.Table,
		credentials:  params.Select.DataSourceInstance.GetCredentials(),
	}

	response, err := s.resolver.resolve(request)
	if err != nil {
		return fmt.Errorf("resolve YDB endpoint: %w", err)
	}

	for _, src := range response.sources {
		split := &datasource.ListSplitResult{
			Slct: params.Select,
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
		case params.ResultChan <- split:
		case <-params.Ctx.Done():
			return params.Ctx.Err()
		}
	}

	return nil
}

func NewSplitProvider(resolver Resolver) rdbms_utils.SplitProvider {
	return &splitProviderImpl{
		resolver: resolver,
	}
}
