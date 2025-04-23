package logging

import (
	"fmt"
	"sort"

	"golang.org/x/sync/errgroup"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
)

var _ rdbms_utils.SplitProvider = (*splitProviderImpl)(nil)

type splitProviderImpl struct {
	resolver         Resolver
	ydbSplitProvider ydb.SplitProvider
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

	fmt.Println(">> HERE <<")

	response, err := s.resolver.resolve(request)
	if err != nil {
		return fmt.Errorf("resolve YDB endpoint: %w", err)
	}

	var errGroup errgroup.Group

	// Load tablet ids from YDB databases living in different data centers concurrently.
	for _, src := range response.sources {
		src := src

		errGroup.Go(func() error {
			return s.handleYDBSource(params, src)
		})
	}

	if err := errGroup.Wait(); err != nil {
		return fmt.Errorf("handle YDB source: %w", err)
	}

	return nil
}

func (s *splitProviderImpl) handleYDBSource(
	params *rdbms_utils.ListSplitsParams,
	src *ydbSource,
) error {
	// Connect YDB to get some table metadata
	var cs []rdbms_utils.Connection

	err := params.MakeConnectionRetrier.Run(params.Ctx, params.Logger,
		func() error {
			var makeConnErr error

			makeConnectionParams := &rdbms_utils.ConnectionParams{
				Ctx:    params.Ctx,
				Logger: params.Logger,
				DataSourceInstance: &api_common.TGenericDataSourceInstance{
					Kind:        api_common.EGenericDataSourceKind_YDB,
					Endpoint:    src.endpoint,
					Credentials: src.credentials,
					Database:    src.databaseName,
					UseTls:      true,
					Protocol:    api_common.EGenericProtocol_NATIVE,
				},
				TableName:  src.tableName,
				QueryPhase: rdbms_utils.QueryPhaseListSplits,
			}

			cs, makeConnErr = params.ConnectionManager.Make(makeConnectionParams)
			if makeConnErr != nil {
				return fmt.Errorf("make connection: %w", makeConnErr)
			}

			return nil
		},
	)

	if err != nil {
		return fmt.Errorf("retry: %w", err)
	}

	defer params.ConnectionManager.Release(params.Ctx, params.Logger, cs)

	tabletIDs, err := s.ydbSplitProvider.GetColumnShardTabletIDs(
		params.Ctx,
		params.Logger,
		cs[0],
	)

	if err != nil {
		return fmt.Errorf("get column shard tablet ids: %w", err)
	}

	// FIXME: remove after debug
	sort.Slice(tabletIDs, func(i, j int) bool { return tabletIDs[i] < tabletIDs[j] })
	dbName, _ := cs[0].From()
	fmt.Println(">>> TABLET IDS", dbName, tabletIDs)

	// 1 tablet id <-> 1 column shard <-> 1 split
	for _, tabletId := range tabletIDs {
		split := &datasource.ListSplitResult{
			Slct: params.Select,
			Description: &TSplitDescription{
				Payload: &TSplitDescription_Ydb{
					Ydb: &TSplitDescription_TYdb{
						Endpoint:     src.endpoint,
						DatabaseName: src.databaseName,
						TableName:    src.tableName,
						TabletIds:    []uint64{tabletId},
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

func NewSplitProvider(resolver Resolver, ydbSplitProvider ydb.SplitProvider) rdbms_utils.SplitProvider {
	return &splitProviderImpl{
		resolver:         resolver,
		ydbSplitProvider: ydbSplitProvider,
	}
}
