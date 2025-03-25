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
	return nil
}

// DescribeTable retrieves table metadata by scanning Redis keys with a given prefix.
// It accumulates keys until at least 'count' keys are collected or the scan finishes,
// then analyzes key types and builds the schema.
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

	err := ds.retrierSet.MakeConnection.Run(ctx, logger, func() error {
		var err error
		client, err = ds.makeConnection(ctx, logger, dsi)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	defer func() {
		if err = client.Close(); err != nil {
			common.LogCloserError(logger, client, "close connection")
		}
	}()

	count := ds.cfg.GetCountDocsToDeduceSchema()
	if request.Table == "" {
		return nil, common.ErrEmptyTableName
	}

	pattern := fmt.Sprintf("%s:*", request.Table)
	allKeys, err := ds.accumulateKeys(ctx, client, pattern, int(count))

	if err != nil {
		return nil, err
	}
	// If no keys found, return an empty schema.
	if len(allKeys) == 0 {
		return &api_service_protos.TDescribeTableResponse{
			Schema: &api_service_protos.TSchema{Columns: nil},
		}, nil
	}

	stringExists, hashExists, unionHashFields, err := ds.analyzeKeys(ctx, client, allKeys, logger)
	if err != nil {
		return nil, err
	}

	columns := buildSchema(stringExists, hashExists, unionHashFields)
	return &api_service_protos.TDescribeTableResponse{
		Schema: &api_service_protos.TSchema{Columns: columns},
	}, nil
}

// accumulateKeys scans Redis keys matching the given pattern until at least 'count' keys are collected
// or the scan is finished.
func (ds *dataSource) accumulateKeys(ctx context.Context, client *redis.Client, pattern string, count int) ([]string, error) {
	var (
		allKeys []string
		cursor  uint64 = 0
	)

	for {
		keys, newCursor, err := client.Scan(ctx, cursor, pattern, int64(count)).Result()
		if err != nil {
			return nil, fmt.Errorf("scan keys: %w", err)
		}
		allKeys = append(allKeys, keys...)
		cursor = newCursor
		if cursor == 0 || len(allKeys) >= count {
			break
		}
	}
	return allKeys, nil
}

// analyzeKeys iterates over all keys, determines each key's type,
// sets flags for string and hash keys, and accumulates all hash fields.
func (ds *dataSource) analyzeKeys(
	ctx context.Context,
	client *redis.Client,
	keys []string,
	logger *zap.Logger,
) (bool, bool, map[string]struct{}, error) {
	var stringExists, hashExists bool
	unionHashFields := make(map[string]struct{})

	for _, key := range keys {
		typ, err := client.Type(ctx, key).Result()
		if err != nil {
			return false, false, nil, fmt.Errorf("failed to get type for key %s: %w", key, err)
		}

		switch typ {
		case redisTypeString:
			stringExists = true
		case redisTypeHash:
			hashExists = true
			fields, err := client.HKeys(ctx, key).Result()

			if err != nil {
				return false, false, nil, fmt.Errorf("failed to get hash keys for key %s: %w", key, err)
			}

			for _, field := range fields {
				unionHashFields[field] = struct{}{}
			}
		default:
			logger.Info("DescribeTable skipped unsupported type", zap.String("value", typ))
		}
	}
	return stringExists, hashExists, unionHashFields, nil
}

// buildSchema creates the schema (list of columns) based on the presence of string and hash keys
// and the set of hash fields.
func buildSchema(stringExists, hashExists bool, unionHashFields map[string]struct{}) []*Ydb.Column {
	var columns []*Ydb.Column

	// Always add the "key" column.
	keyColumn := &Ydb.Column{
		Name: KeyColumnName,
		Type: common.MakePrimitiveType(Ydb.Type_STRING),
	}
	columns = append(columns, keyColumn)

	// Add "string_values" column if string keys exist.
	if stringExists {
		stringColumn := &Ydb.Column{
			Name: StringColumnName,
			Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
		}
		columns = append(columns, stringColumn)
	}

	// Add "hash_values" column if hash keys exist.
	if hashExists {
		var structMembers []*Ydb.StructMember
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
	return columns
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
