package redis

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/observation"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.DataSource[any] = (*dataSource)(nil)

type (
	dataSource struct {
		retrierSet  *retry.RetrierSet
		cfg         *config.TRedisConfig
		cc          conversion.Collection
		queryLogger common.QueryLogger
	}

	keysSpec struct {
		stringExists    bool
		hashExists      bool
		unionHashFields map[string]struct{}
	}

	redisRowTransformer struct {
		key        string
		stringVal  *string
		hashVal    *map[string]string
		items      []*api_service_protos.TSelect_TWhat_TItem
		hashFields []string
		acceptors  []any
	}
)

func newRedisRowTransformer(items []*api_service_protos.TSelect_TWhat_TItem) (*redisRowTransformer, error) {
	hashFields, err := getHashFields(items)
	if err != nil {
		return nil, fmt.Errorf("getHashFields: %w", err)
	}

	t := &redisRowTransformer{
		items:      items,
		hashFields: hashFields,
		acceptors:  make([]any, len(items)),
	}

	for i, item := range items {
		column := item.GetColumn()
		switch column.Name {
		case KeyColumnName:
			t.acceptors[i] = &t.key
		case StringColumnName:
			t.acceptors[i] = &t.stringVal
		case HashColumnName:
			t.acceptors[i] = &t.hashVal
		}
	}

	return t, nil
}

func (t *redisRowTransformer) clean() {
	t.key = ""
	t.stringVal = nil
	t.hashVal = nil
}

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

// getHashFields retrieves HASH fields from request schema
func getHashFields(items []*api_service_protos.TSelect_TWhat_TItem) ([]string, error) {
	var hashFields []string

	for _, item := range items {
		column := item.GetColumn()
		if column == nil {
			return nil, fmt.Errorf("select.what has nil column")
		}

		if column.Name == HashColumnName {
			structType := column.Type.GetOptionalType().GetItem().GetStructType()
			for _, member := range structType.Members {
				hashFields = append(hashFields, member.Name)
			}

			break
		}
	}

	return hashFields, nil
}

// Redis Pipeline Docs https://redis.io/docs/latest/develop/clients/go/transpipe/
// readKeys orchestrates a batched SCAN over Redis keys matching 'pattern', and processes string and hash keys.
//
//nolint:gocyclo
func (*dataSource) readKeys(
	ctx context.Context,
	client *redis.Client,
	split *api_service_protos.TSplit,
	transformer *redisRowTransformer,
	sink paging.Sink[any],
	logger *zap.Logger,
) error {
	var (
		comp      *api_service_protos.TPredicate_TComparison
		operation api_service_protos.TPredicate_TComparison_EOperation
		column    string
		pattern   = "*"
	)

	if split.Select.Where != nil && split.Select.Where.FilterTyped != nil {
		comp = split.Select.Where.FilterTyped.GetComparison()
		operation = comp.Operation
		column = comp.GetLeftValue().GetColumn()
		pattern = string(comp.GetRightValue().GetTypedValue().GetValue().GetBytesValue())
	}

	if column == KeyColumnName && operation == api_service_protos.TPredicate_TComparison_EQ {
		typ, err := client.Type(ctx, pattern).Result()
		if err != nil {
			return fmt.Errorf("TYPE command failed for key %s: %w", pattern, err)
		}

		switch typ {
		case TypeString:
			return processStringKeys(ctx, client, []string{pattern}, transformer, sink)
		case TypeHash:
			if len(transformer.hashFields) > 0 {
				return processHashKeys(ctx, client, []string{pattern}, transformer, sink)
			}

			return nil
		default:
			logger.Warn("unsupported key type for specific key", zap.String("key", pattern), zap.String("type", typ))
			return nil
		}
	}

	switch operation {
	case api_service_protos.TPredicate_TComparison_STARTS_WITH:
		// LIKE 'foo%' → 'foo*'
		pattern += "*"
	case api_service_protos.TPredicate_TComparison_ENDS_WITH:
		// LIKE '%foo' → '*foo'
		pattern = "*" + pattern
	case api_service_protos.TPredicate_TComparison_CONTAINS:
		// LIKE '%foo%' → '*foo*'
		pattern = "*" + pattern + "*"
	}

	var cursor, unsupported uint64

	for {
		// 1) Scan a batch of keys
		keys, nextCursor, err := client.Scan(ctx, cursor, pattern, scanBatchSize).Result()
		if err != nil {
			return fmt.Errorf("scan keys: %w", err)
		}

		// 2) Determine types via pipeline
		strKeys, hashKeys, batchUnsupported, err := splitKeysByType(ctx, client, keys)
		if err != nil {
			return err
		}

		unsupported += batchUnsupported

		// 3) Fetch and emit string key rows
		if len(strKeys) > 0 {
			if err = processStringKeys(ctx, client, strKeys, transformer, sink); err != nil {
				return err
			}
		}

		// 4) Fetch and emit hash key rows
		if len(hashKeys) > 0 && len(transformer.hashFields) > 0 {
			if err = processHashKeys(ctx, client, hashKeys, transformer, sink); err != nil {
				return err
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	if unsupported > 0 {
		logger.Warn("unsupported key types encountered", zap.Uint64("count", unsupported))
	}

	return nil
}

// splitKeysByType issues a pipeline of TYPE commands, then partitions keys into string and hash slices.
func splitKeysByType(
	ctx context.Context,
	client *redis.Client,
	keys []string,
) (strKeys []string, hashKeys []string, unsupported uint64, err error) {
	pipe := client.Pipeline()
	typeCmds := make([]*redis.StatusCmd, len(keys))

	for i, key := range keys {
		typeCmds[i] = pipe.Type(ctx, key)
	}

	if _, err = pipe.Exec(ctx); err != nil {
		return nil, nil, 0, fmt.Errorf("TYPE pipeline exec failed: %w", err)
	}

	for i, cmd := range typeCmds {
		t, err := cmd.Result()
		if err != nil {
			return nil, nil, 0, fmt.Errorf("TYPE command result failed: %w", err)
		}

		switch t {
		case TypeString:
			strKeys = append(strKeys, keys[i])
		case TypeHash:
			hashKeys = append(hashKeys, keys[i])
		default:
			unsupported++
		}
	}

	return strKeys, hashKeys, unsupported, nil
}

// processStringKeys pipelines GET commands for string keys and writes rows to the sink.
func processStringKeys(
	ctx context.Context,
	client *redis.Client,
	keys []string,
	transformer *redisRowTransformer,
	sink paging.Sink[any],
) error {
	pipe := client.Pipeline()
	getCmds := make([]*redis.StringCmd, len(keys))

	for i, key := range keys {
		getCmds[i] = pipe.Get(ctx, key)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("GET pipeline exec failed: %w", err)
	}

	for i, cmd := range getCmds {
		val, e := cmd.Result()
		if e != nil {
			return fmt.Errorf("GET command result failed: %w", e)
		}

		transformer.key = keys[i]
		transformer.stringVal = &val
		transformer.hashVal = nil

		if err := sink.AddRow(transformer); err != nil {
			return fmt.Errorf("add row: %w", err)
		}

		transformer.clean()
	}

	return nil
}

// processHashKeys pipelines HMGET commands for hash keys and writes rows to the sink.
func processHashKeys(
	ctx context.Context,
	client *redis.Client,
	keys []string,
	transformer *redisRowTransformer,
	sink paging.Sink[any],
) error {
	pipe := client.Pipeline()
	hmgetCmds := make([]*redis.SliceCmd, len(keys))

	for i, key := range keys {
		hmgetCmds[i] = pipe.HMGet(ctx, key, transformer.hashFields...)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("HMGET pipeline exec failed: %w", err)
	}

	for i, cmd := range hmgetCmds {
		vals, e := cmd.Result()
		if e != nil {
			return fmt.Errorf("HMGET command result failed: %w", e)
		}

		transformer.key = keys[i]
		m := make(map[string]string, len(transformer.hashFields))

		for j, field := range transformer.hashFields {
			if vals[j] != nil {
				m[field] = vals[j].(string)
			}
		}

		transformer.hashVal = &m
		transformer.stringVal = nil

		if err := sink.AddRow(transformer); err != nil {
			return fmt.Errorf("add row: %w", err)
		}

		transformer.clean()
	}

	return nil
}

func (ds *dataSource) ReadSplit(
	ctx context.Context,
	logger *zap.Logger,
	_ observation.IncomingQueryID,
	_ *api_service_protos.TReadSplitsRequest,
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
		return fmt.Errorf("make connection: %w", err)
	}

	defer common.LogCloserError(logger, client, "close connection")

	if split.Select.From.Table == "" {
		return common.ErrEmptyTableName
	}

	ds.queryLogger.Dump(split.Select.From.Table, split.Select.What.String())

	sinks, err := sinkFactory.MakeSinks([]*paging.SinkParams{{Logger: logger}})
	if err != nil {
		return fmt.Errorf("make sinks: %w", err)
	}

	sink := sinks[0]

	transformer, err := newRedisRowTransformer(split.Select.What.GetItems())
	if err != nil {
		return fmt.Errorf("create transformer: %w", err)
	}

	if err = ds.readKeys(ctx, client, split, transformer, sink, logger); err != nil {
		return fmt.Errorf("readKeys: %w", err)
	}

	sink.Finish()

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
	if !strings.Contains(pattern, "*") {
		return []string{pattern}, nil
	}

	var (
		allKeys []string
		cursor  uint64
	)

	for {
		keys, newCursor, err := client.Scan(ctx, cursor, pattern, scanBatchSize).Result()
		if err != nil {
			return nil, fmt.Errorf("scan keys: %w", err)
		}

		for _, key := range keys {
			allKeys = append(allKeys, key)
			if len(allKeys) >= count {
				break
			}
		}

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

	var unsupportedTypesCount uint64

	res.unionHashFields = make(map[string]struct{})

	for _, key := range keys {
		typ, err := client.Type(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("get type for key %s: %w", key, err)
		}

		switch typ {
		case TypeString:
			res.stringExists = true
		case TypeHash:
			res.hashExists = true
			fields, err := client.HKeys(ctx, key).Result()

			if err != nil {
				return nil, fmt.Errorf("get hash keys for key %s: %w", key, err)
			}

			for _, field := range fields {
				res.unionHashFields[field] = struct{}{}
			}
		default:
			unsupportedTypesCount++
		}
	}

	if unsupportedTypesCount > 0 {
		logger.Warn("number of unsupported types encountered: ", zap.Uint64("value", unsupportedTypesCount))
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
		Addr:         addr,
		Password:     dsi.Credentials.GetBasic().Password,
		Username:     dsi.Credentials.GetBasic().Username, // use if required
		DB:           0,                                   // can be extended if dsi.Database specifies a DB number
		PoolSize:     50,
		MinIdleConns: 10,
		DialTimeout:  10 * time.Second, // time for TCP‑connect + AUTH
		ReadTimeout:  10 * time.Second,
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

func (t *redisRowTransformer) AppendToArrowBuilders(_ *arrow.Schema, builders []array.Builder) error {
	for i, item := range t.items {
		column := item.GetColumn()
		if column == nil {
			return fmt.Errorf("item #%d is not a column", i)
		}

		builder := builders[i]

		switch column.Name {
		case KeyColumnName:
			if err := t.appendKey(builder); err != nil {
				return fmt.Errorf("append key: %w", err)
			}
		case StringColumnName:
			if err := t.appendStringValue(builder); err != nil {
				return fmt.Errorf("append string value: %w", err)
			}
		case HashColumnName:
			if err := t.appendHashValue(builder); err != nil {
				return fmt.Errorf("append hash value: %w", err)
			}
		default:
			return fmt.Errorf("unknown column: %s", column.Name)
		}
	}

	return nil
}

func (t *redisRowTransformer) appendKey(builderIn array.Builder) error {
	if builder, ok := builderIn.(*array.BinaryBuilder); ok {
		builder.Append([]byte(t.key))
		return nil
	}

	return fmt.Errorf("unexpected builder type for key: %T", builderIn)
}

func (t *redisRowTransformer) appendStringValue(builderIn array.Builder) error {
	if builder, ok := builderIn.(*array.BinaryBuilder); ok {
		if t.stringVal != nil {
			builder.Append([]byte(*t.stringVal))
		} else {
			builder.AppendNull()
		}

		return nil
	}

	return fmt.Errorf("unexpected builder type for string value: %T", builderIn)
}

func (t *redisRowTransformer) appendHashValue(builderIn array.Builder) error {
	builder, ok := builderIn.(*array.StructBuilder)
	if !ok {
		return fmt.Errorf("unexpected builder type for hash value: %T", builderIn)
	}

	if t.hashVal == nil {
		builder.AppendNull()
		return nil
	}

	for i, fieldName := range t.hashFields {
		binaryBuilder, ok := builder.FieldBuilder(i).(*array.BinaryBuilder)
		if !ok {
			return fmt.Errorf("unexpected builder type for hash field %s: %T", fieldName, builder.FieldBuilder(i))
		}

		if val, exists := (*t.hashVal)[fieldName]; exists {
			binaryBuilder.Append([]byte(val))
		} else {
			builder.FieldBuilder(i).AppendNull()
		}
	}

	builder.Append(true)

	return nil
}

func (t *redisRowTransformer) GetAcceptors() []any {
	return t.acceptors
}

func (*redisRowTransformer) SetAcceptors(_ []any) {
	panic("not implemented")
}
