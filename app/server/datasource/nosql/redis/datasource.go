package redis

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

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

type (
	dataSource struct {
		retrierSet *retry.RetrierSet
		cfg        *config.TRedisConfig
		cc         conversion.Collection
	}

	keysSpec struct {
		stringExists    bool
		hashExists      bool
		unionHashFields map[string]struct{}
	}
)

func NewDataSource(retrierSet *retry.RetrierSet, cfg *config.TRedisConfig, cc conversion.Collection) datasource.DataSource[any] {
	return &dataSource{
		retrierSet: retrierSet,
		cfg:        cfg,
		cc:         cc,
	}
}

func (*dataSource) ListSplits(
	ctx context.Context,
	_ *zap.Logger,
	_ *api_service_protos.TListSplitsRequest,
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

func (*dataSource) ReadSplit(
	_ context.Context,
	_ *zap.Logger,
	_ *api_service_protos.TReadSplitsRequest,
	_ *api_service_protos.TSplit,
	_ paging.SinkFactory[any],
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
		common.LogCloserError(logger, client, "close connection")
	}()

	count := ds.cfg.GetCountDocsToDeduceSchema()

	if request.Table == "" {
		return nil, common.ErrEmptyTableName
	}

	allKeys, err := ds.accumulateKeys(ctx, client, request.Table, int(count))

	if err != nil {
		return nil, fmt.Errorf("accumulate keys: %w", err)
	}

	// If no keys found, return an empty schema.
	if len(allKeys) == 0 {
		return &api_service_protos.TDescribeTableResponse{
			Schema: &api_service_protos.TSchema{Columns: nil},
		}, nil
	}

	keysInfo, err := ds.analyzeKeys(ctx, logger, client, allKeys)
	if err != nil {
		return nil, fmt.Errorf("analyze keys: %w", err)
	}

	columns := buildSchema(*keysInfo)

	return &api_service_protos.TDescribeTableResponse{
		Schema: &api_service_protos.TSchema{Columns: columns},
	}, nil
}

// accumulateKeys scans Redis keys matching the given pattern until at least 'count' keys are collected
// or the scan is finished.
func (*dataSource) accumulateKeys(ctx context.Context, client *redis.Client, pattern string, count int) ([]string, error) {
	var (
		allKeys []string
		cursor  uint64
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
func (*dataSource) analyzeKeys(
	ctx context.Context,
	logger *zap.Logger,
	client *redis.Client,
	keys []string,
) (*keysSpec, error) {
	var res keysSpec
	res.unionHashFields = make(map[string]struct{})

	for _, key := range keys {
		typ, err := client.Type(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("get type for key %s: %w", key, err)
		}

		switch typ {
		case redisTypeString:
			res.stringExists = true
		case redisTypeHash:
			res.hashExists = true
			fields, err := client.HKeys(ctx, key).Result()

			if err != nil {
				return nil, fmt.Errorf("get hash keys for key %s: %w", key, err)
			}

			for _, field := range fields {
				res.unionHashFields[field] = struct{}{}
			}
		default:
			logger.Warn("skipped unsupported type", zap.String("value", typ))
		}
	}

	return &res, nil
}

// buildSchema creates the schema (list of columns) based on the presence of string and hash keys
// and the set of hash fields.
func buildSchema(spec keysSpec) []*Ydb.Column {
	var columns []*Ydb.Column

	// Always add the "key" column.
	keyColumn := &Ydb.Column{
		Name: KeyColumnName,
		Type: common.MakePrimitiveType(Ydb.Type_STRING),
	}
	columns = append(columns, keyColumn)

	// Add "string_values" column if string keys exist.
	if spec.stringExists {
		stringColumn := &Ydb.Column{
			Name: StringColumnName,
			Type: common.MakeOptionalType(common.MakePrimitiveType(Ydb.Type_STRING)),
		}
		columns = append(columns, stringColumn)
	}

	// Add "hash_values" column if hash keys exist.
	if spec.hashExists {
		var structMembers []*Ydb.StructMember

		var fields []string

		for field := range spec.unionHashFields {
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
	// Assume that dsi contains necessary fields: Endpoint, Credentials.
	addr := fmt.Sprintf("%s:%d", dsi.Endpoint.Host, dsi.Endpoint.Port)
	options := &redis.Options{
		Addr:     addr,
		Password: dsi.Credentials.GetBasic().Password,
		Username: dsi.Credentials.GetBasic().Username, // use if required
		DB:       0,                                   // can be extended if dsi.Database specifies a DB number
	}

	client := redis.NewClient(options)

	// Parse timeouts from configuration.
	pingTimeout, err := time.ParseDuration(ds.cfg.PingConnectionTimeout)
	if err != nil {
		return nil, fmt.Errorf("parse duration value '%v': %w", ds.cfg.PingConnectionTimeout, err)
	}
	// Ping Redis using a context with timeout.
	logger.Debug("trying to connect to database", zap.String("addr", addr))

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	logger.Info("successfully connected to database", zap.String("addr", addr))

	return client, nil
}
