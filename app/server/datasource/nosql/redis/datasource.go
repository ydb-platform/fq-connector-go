package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.DataSource[any] = (*dataSource)(nil)

type dataSource struct {
	retrierSet *retry.RetrierSet
	cfg        *config.TRedisConfig
	cc         conversion.Collection
}

func NewDataSource(retrierSet *retry.RetrierSet, cfg *config.TRedisConfig, cc conversion.Collection) datasource.DataSource[any] {
	return &dataSource{
		retrierSet: retrierSet,
		cfg:        cfg,
		cc:         cc,
	}
}

func (ds *dataSource) DescribeTable(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	dsi := request.DataSourceInstance

	if dsi.Protocol != api_common.EGenericProtocol_NATIVE {
		return nil, fmt.Errorf("cannot run Redis connection with protocol '%v'", dsi.Protocol)
	}

	var client *redis.Client

	err := ds.retrierSet.MakeConnection.Run(ctx, logger,
		func() error {
			var err error
			client, err = ds.makeConnection(ctx, logger, dsi)
			return err
		},
	)
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	defer func() {
		if err = client.Close(); err != nil {
			common.LogCloserError(logger, client, "close connection")
		}
	}()

	count := ds.cfg.GetCountDocsToDeduceSchema()

	if request == nil || (request != nil && request.Table == "") {
		return nil, common.ErrEmptyTableName
	}

	// Scan up to 'count' keys from Redis.
	keys, _, err := client.Scan(ctx, 0, "*", int64(count)).Result()
	if err != nil {
		return nil, fmt.Errorf("scan keys: %w", err)
	}

	// If there are no keys, return an empty schema.
	if len(keys) == 0 {
		return &api_service_protos.TDescribeTableResponse{
			Schema: &api_service_protos.TSchema{Columns: nil},
		}, nil
	}

	var stringExists bool
	var hashExists bool
	unionHashFields := make(map[string]struct{})

	// Iterate over the obtained keys.
	for _, key := range keys {
		typ, err := client.Type(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get type for key %s: %w", key, err)
		}

		switch typ {
		case redisTypeString:
			stringExists = true
		case redisTypeHash:
			hashExists = true
			// Get the list of fields for the hash key.
			fields, err := client.HKeys(ctx, key).Result()
			if err != nil {
				return nil, fmt.Errorf("failed to get hash keys: %w", err)
			}

			for _, field := range fields {
				unionHashFields[field] = struct{}{}
			}
		default:
			logger.Info("DescribeTable found and skipped currently unsupported type", zap.String("value", typ))
		}
	}

	var columns []*Ydb.Column

	// Column "key" - always.
	keyColumn := &Ydb.Column{
		Name: KeyColumnName,
		Type: common.MakePrimitiveType(Ydb.Type_STRING),
	}
	columns = append(columns, keyColumn)

	// If string values exist, add the "stringValues" column.
	if stringExists {
		stringColumn := &Ydb.Column{
			Name: StringColumnName,
			Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
		}
		columns = append(columns, stringColumn)
	}

	// If hash values exist, build the "hashValues" column.
	if hashExists {
		var structMembers []*Ydb.StructMember
		// For consistency, sort the list of fields.
		var fields []string
		for field := range unionHashFields {
			fields = append(fields, field)
		}
		sort.Strings(fields)

		for _, field := range fields {
			member := &Ydb.StructMember{
				Name: field,
				Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
			}
			structMembers = append(structMembers, member)
		}
		// Build YDB StructType.
		structType := &Ydb.Type{
			Type: &Ydb.Type_StructType{
				StructType: &Ydb.StructType{
					Members: structMembers,
				},
			},
		}
		hashColumn := &Ydb.Column{
			Name: HashColumnName,
			Type: common.MakeOptionalType(structType),
		}
		columns = append(columns, hashColumn)
	}

	return &api_service_protos.TDescribeTableResponse{
		Schema: &api_service_protos.TSchema{Columns: columns},
	}, nil
}

func (ds *dataSource) ListSplits(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult,
) error {
	// By default, we deny table splitting.
	select {
	case resultChan <- &datasource.ListSplitResult{Slct: slct, Description: nil}:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (ds *dataSource) ReadSplit(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sinkFactory paging.SinkFactory[any],
) error {
	dsi := split.Select.DataSourceInstance

	if dsi.Protocol != api_common.EGenericProtocol_NATIVE {
		return fmt.Errorf("cannot run Redis connection with protocol '%v'", dsi.Protocol)
	}

	var client *redis.Client

	err := ds.retrierSet.MakeConnection.Run(ctx, logger, func() error {
		var err error
		client, err = ds.makeConnection(ctx, logger, dsi)
		return err
	})
	if err != nil {
		common.LogCloserError(logger, client, "make connection")
		return fmt.Errorf("make connection: %w", err)
	}

	defer func() {
		if err = client.Close(); err != nil {
			logger.Error("close connection", zap.Error(err))
		}
	}()

	sinks, err := sinkFactory.MakeSinks([]*paging.SinkParams{{Logger: logger}})
	if err != nil {
		return fmt.Errorf("make sinks: %w", err)
	}

	sink := sinks[0]

	// Get schemas (Arrow and YDB) from the SELECT query.
	arrowSchema, err := common.SelectWhatToArrowSchema(split.Select.What)
	if err != nil {
		return fmt.Errorf("select what to Arrow schema: %w", err)
	}

	ydbSchema, err := common.SelectWhatToYDBTypes(split.Select.What)
	if err != nil {
		return fmt.Errorf("select what to YDB schema: %w", err)
	}

	reader, err := makeRedisRowReader(arrowSchema, ydbSchema, ds.cc)
	if err != nil {
		return fmt.Errorf("make redis row reader: %w", err)
	}

	// If a pattern is specified in select.From.Table, use it; otherwise, return error.
	if split.Select.From == nil || (split.Select.From != nil && split.Select.From.Table == "") {
		return common.ErrEmptyTableName
	}

	var cursor uint64

	rowData := make(map[string]any)
	// Use SCAN to iterate over keys.
	for {
		keys, newCursor, err := client.Scan(ctx, cursor, split.Select.From.Table, scanBatchSize).Result()
		if err != nil {
			return fmt.Errorf("scan keys: %w", err)
		}

		cursor = newCursor

		for _, key := range keys {
			typ, err := client.Type(ctx, key).Result()
			if err != nil {
				return fmt.Errorf("failed to get type for key %s: %w", key, err)
			}

			// Build raw row data as a map, where keys are column names.
			clear(rowData)
			rowData[KeyColumnName] = key

			switch typ {
			case "string":
				val, err := client.Get(ctx, key).Result()
				if err != nil {
					logger.Error("get key value", zap.String(KeyColumnName, key), zap.Error(err))
					rowData[StringColumnName] = nil
				} else {
					rowData[StringColumnName] = val
				}
				// For string key, hashValues column remains nil.
				rowData[HashColumnName] = nil
			case "hash":
				hashMap, err := client.HGetAll(ctx, key).Result()
				if err != nil {
					logger.Error("get hash value", zap.String(KeyColumnName, key), zap.Error(err))
					rowData[HashColumnName] = nil
				} else {
					rowData[HashColumnName] = hashMap
				}
				// For hash key, stringValues column remains nil.
				rowData[StringColumnName] = nil
			default:
				// If type is not supported, skip the key.
				continue
			}

			// Convert raw row data into a set of acceptors matching the selected schema.
			if err := reader.accept(logger, rowData); err != nil {
				return fmt.Errorf("accept row: %w", err)
			}

			if err := sink.AddRow(reader.transformer); err != nil {
				return fmt.Errorf("add row to sink: %w", err)
			}
		}

		if cursor == 0 {
			break
		}
	}

	sink.Finish()

	return nil
}

func (ds *dataSource) makeConnection(
	ctx context.Context,
	logger *zap.Logger,
	dsi *api_common.TGenericDataSourceInstance,
) (*redis.Client, error) {
	// Assume that dsi contains necessary fields: Endpoint, Credentials, UseTls.
	addr := fmt.Sprintf("%s:%d", dsi.Endpoint.Host, dsi.Endpoint.Port)
	options := &redis.Options{
		Addr:     addr,
		Password: dsi.Credentials.GetBasic().Password,
		Username: dsi.Credentials.GetBasic().Username, // use if required
		DB:       0,                                   // can be extended if dsi.Database specifies a DB number
	}
	// Configure TLS if required.
	if dsi.UseTls {
		options.TLSConfig = &tls.Config{InsecureSkipVerify: true} // For production, a proper TLSConfig is required
	}

	client := redis.NewClient(options)

	// Parse timeouts from configuration.
	openTimeout, err := time.ParseDuration(ds.cfg.OpenConnectionTimeout)
	if err != nil {
		openTimeout = 5 * time.Second
	}
	// Ping Redis using a context with timeout.
	logger.Debug("trying to connect to Redis", zap.String("addr", addr))

	openCtx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()

	if err := client.Ping(openCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	logger.Info("successfully connected to Redis", zap.String("addr", addr))

	return client, nil
}
