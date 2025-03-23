package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/redis/go-redis/v9"
	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"
	"sort"
	"time"
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
	logger.Info("describe table")
	if dsi.Protocol != api_common.EGenericProtocol_NATIVE {
		return nil, fmt.Errorf("cannot run Redis connection with protocol '%v'", dsi.Protocol)
	}

	var client *redis.Client

	err := ds.retrierSet.MakeConnection.Run(ctx, logger,
		func() error {
			var err error
			client, err = ds.makeConnection(ctx, logger, dsi)
			//client = connectRedis()

			return err
		},
	)
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			logger.Error("close connection", zap.Error(err))
		}
	}()

	count := ds.cfg.GetCountDocsToDeduceSchema()
	// Сканируем до 'count' ключей из Redis.
	keys, _, err := client.Scan(ctx, 0, "*", int64(count)).Result()
	if err != nil {
		return nil, fmt.Errorf("scan keys: %w", err)
	}

	// Если ключей нет – возвращаем пустую схему.
	if len(keys) == 0 {
		return &api_service_protos.TDescribeTableResponse{
			Schema: &api_service_protos.TSchema{Columns: nil},
		}, nil
	}

	var stringExists bool
	var hashExists bool
	unionHashFields := make(map[string]struct{})

	// Обходим полученные ключи.
	for _, key := range keys {
		typ, err := client.Type(ctx, key).Result()
		if err != nil {
			logger.Error("get key type", zap.String("key", key), zap.Error(err))
			continue
		}
		switch typ {
		case "string":
			stringExists = true
		case "hash":
			hashExists = true
			// Получаем список полей для hash-ключа.
			fields, err := client.HKeys(ctx, key).Result()
			if err != nil {
				logger.Error("get hash keys", zap.String("key", key), zap.Error(err))
				continue
			}
			for _, field := range fields {
				unionHashFields[field] = struct{}{}
			}
		default:
			// Игнорируем другие типы.
		}
	}

	var columns []*Ydb.Column

	// Колонка key – всегда.
	keyColumn := &Ydb.Column{
		Name: "key",
		Type: common.MakePrimitiveType(Ydb.Type_STRING),
	}
	columns = append(columns, keyColumn)

	// Если есть строковые значения – добавляем колонку stringValues.
	if stringExists {
		stringColumn := &Ydb.Column{
			Name: "stringValues",
			Type: common.MakePrimitiveType(Ydb.Type_STRING),
		}
		columns = append(columns, stringColumn)
	}

	// Если есть hash-значения – формируем колонку hashValues.
	if hashExists {
		var structMembers []*Ydb.StructMember
		// Для консистентности приводим список полей к отсортированному виду.
		var fields []string
		for field := range unionHashFields {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		for _, field := range fields {
			member := &Ydb.StructMember{
				Name: field,
				Type: common.MakePrimitiveType(Ydb.Type_STRING),
			}
			structMembers = append(structMembers, member)
		}
		// Формируем YDB StructType.
		structType := &Ydb.Type{
			Type: &Ydb.Type_StructType{
				StructType: &Ydb.StructType{
					Members: structMembers,
				},
			},
		}
		hashColumn := &Ydb.Column{
			Name: "hashValues",
			Type: structType,
		}
		columns = append(columns, hashColumn)
	}

	return &api_service_protos.TDescribeTableResponse{
		Schema: &api_service_protos.TSchema{Columns: columns},
	}, nil
}

func (ds *dataSource) ListSplits(ctx context.Context, logger *zap.Logger, request *api_service_protos.TListSplitsRequest, slct *api_service_protos.TSelect, resultChan chan<- *datasource.ListSplitResult) error {
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
		//client = connectRedis()
		return err
	})
	if err != nil {
		return fmt.Errorf("make connection: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			logger.Error("close connection", zap.Error(err))
		}
	}()

	sinks, err := sinkFactory.MakeSinks([]*paging.SinkParams{{Logger: logger}})
	if err != nil {
		return fmt.Errorf("make sinks: %w", err)
	}
	sink := sinks[0]

	// Получаем схемы (Arrow и YDB) из запроса выбора.
	arrowSchema, err := common.SelectWhatToArrowSchema(split.Select.What)
	if err != nil {
		return fmt.Errorf("select what to Arrow schema: %w", err)
	}
	ydbSchema, err := common.SelectWhatToYDBTypes(split.Select.What)
	if err != nil {
		return fmt.Errorf("select what to YDB schema: %w", err)
	}

	// Создаём redisRowReader (аналог documentReader для MongoDB)
	reader, err := makeRedisRowReader(arrowSchema, ydbSchema, ds.cc)
	if err != nil {
		return fmt.Errorf("make redis row reader: %w", err)
	}

	// Если в select.From.Table указан шаблон – используем его, иначе сканируем все ключи.
	pattern := "*"
	if split.Select.From != nil && split.Select.From.Table != "" {
		pattern = split.Select.From.Table
	}

	var cursor uint64
	// Используем SCAN для перебора ключей
	for {
		keys, newCursor, err := client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("scan keys: %w", err)
		}
		cursor = newCursor

		for _, key := range keys {
			typ, err := client.Type(ctx, key).Result()
			if err != nil {
				logger.Error("get key type", zap.String("key", key), zap.Error(err))
				continue
			}

			// Формируем сырую строку данных как карту, где ключи – имена столбцов.
			rowData := make(map[string]any)
			rowData["key"] = key

			switch typ {
			case "string":
				val, err := client.Get(ctx, key).Result()
				if err != nil {
					logger.Error("get key value", zap.String("key", key), zap.Error(err))
					rowData["stringValues"] = nil
				} else {
					rowData["stringValues"] = val
				}
				// Для строкового ключа колонка hashValues остается nil.
				rowData["hashValues"] = nil
			case "hash":
				hashMap, err := client.HGetAll(ctx, key).Result()
				if err != nil {
					logger.Error("get hash value", zap.String("key", key), zap.Error(err))
					rowData["hashValues"] = nil
				} else {
					rowData["hashValues"] = hashMap
				}
				// Для hash‑ключа колонка stringValues остается nil.
				rowData["stringValues"] = nil
			default:
				// Если тип не поддерживается – пропускаем.
				continue
			}

			// Преобразуем сырые данные в набор acceptors, соответствующий выбранной схеме.
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

func (ds *dataSource) makeConnection(ctx context.Context, logger *zap.Logger, dsi *api_common.TGenericDataSourceInstance) (*redis.Client, error) {
	// Предполагаем, что в dsi присутствуют необходимые поля: Endpoint, Credentials, UseTls.
	addr := fmt.Sprintf("%s:%d", dsi.Endpoint.Host, dsi.Endpoint.Port)
	options := &redis.Options{
		Addr:     addr,
		Password: dsi.Credentials.GetBasic().Password,
		Username: dsi.Credentials.GetBasic().Username, // использовать, если требуется
		DB:       0,                                   // можно расширить, если dsi.Database задаёт номер базы
	}
	// Настройка TLS, если требуется.
	if dsi.UseTls {
		options.TLSConfig = &tls.Config{InsecureSkipVerify: true} // Для продакшена необходимо корректное TLSConfig
	}

	client := redis.NewClient(options)

	// Парсинг таймаутов из конфигурации.
	openTimeout, err := time.ParseDuration(ds.cfg.OpenConnectionTimeout)
	if err != nil {
		openTimeout = 5 * time.Second
	}
	// Пингуем Redis, используя контекст с таймаутом.
	openCtx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()

	if err := client.Ping(openCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	logger.Debug("Connected to Redis", zap.String("addr", addr))
	return client, nil
}

func connectRedis() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // Без пароля
		DB:       0,  // Используем базу 0
	})
}
