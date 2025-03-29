package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/redis/go-redis/v9"

	"github.com/ydb-platform/fq-connector-go/tests/infra/datasource"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
)

type Suite struct {
	*suite.Base[[]byte, *array.BinaryBuilder]
	dataSource *datasource.DataSource
}

func connectRedisFromDS(ctx context.Context, ds *datasource.DataSource) (*redis.Client, error) {
	if len(ds.Instances) == 0 {
		return nil, fmt.Errorf("no data source instances")
	}

	dsi := ds.Instances[0]

	addr := fmt.Sprintf("%s:%d", dsi.Endpoint.Host, dsi.Endpoint.Port)

	options := &redis.Options{
		Addr:     addr,
		Password: dsi.Credentials.GetBasic().Password,
		Username: dsi.Credentials.GetBasic().Username,
		DB:       0,
	}

	if dsi.UseTls {
		options.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}

	client := redis.NewClient(options)

	openTimeout, err := time.ParseDuration("5s")
	if err != nil {
		openTimeout = 5 * time.Second
	}

	pingCtx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	return client, nil
}

// populateTestDataForCase creates a Redis client, populates test data for the given case,
// and ensures the client and context are properly closed.
func (s *Suite) populateTestDataForCase(caseName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := connectRedisFromDS(ctx, s.dataSource)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	defer func() {
		s.Require().NoError(client.Close())
	}()

	// Проверяем, что Redis пустой
	keys, err := client.Keys(ctx, "*").Result()
	if err != nil {
		return fmt.Errorf("failed to get keys: %w", err)
	}
	fmt.Printf("\nBefore population: keys=%v\n", keys)

	err = PopulateTestData(ctx, client, caseName)
	if err != nil {
		return err
	}

	// Проверяем, что данные добавились
	keys, err = client.Keys(ctx, "*").Result()
	if err != nil {
		return fmt.Errorf("failed to get keys: %w", err)
	}
	fmt.Printf("After population: keys=%v\n", keys)

	// Проверяем значения для каждого ключа
	for _, key := range keys {
		typ, err := client.Type(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("failed to get type for key %s: %w", key, err)
		}
		fmt.Printf("Key %s: type=%s\n", key, typ)

		switch typ {
		case "string":
			val, err := client.Get(ctx, key).Result()
			if err != nil {
				return fmt.Errorf("failed to get value for key %s: %w", key, err)
			}
			fmt.Printf("String value for %s: %s\n", key, val)
		case "hash":
			val, err := client.HGetAll(ctx, key).Result()
			if err != nil {
				return fmt.Errorf("failed to get hash for key %s: %w", key, err)
			}
			fmt.Printf("Hash value for %s: %v\n", key, val)
		}
	}

	// Проверяем, что данные соответствуют ожидаемым
	table := tables[caseName]
	if table == nil {
		return fmt.Errorf("unknown test case: %s", caseName)
	}

	fmt.Printf("\nExpected schema:\n")
	for name, typ := range table.Schema.Columns {
		fmt.Printf("Column %s: %v\n", name, typ)
	}

	fmt.Printf("\nExpected records:\n")
	for i, record := range table.Records {
		fmt.Printf("Record %d:\n", i)
		for name, values := range record.Columns {
			fmt.Printf("  %s: %v\n", name, values)
		}
	}

	return nil
}

// TestDescribeTable populates Redis with test data for each test case and validates the table metadata.
func (s *Suite) TestDescribeTable() {
	testCaseNames := []string{
		"stringOnly",
		"hashOnly",
		"mixed",
		"empty",
	}
	for _, testCase := range testCaseNames {
		s.Require().NoError(s.populateTestDataForCase(testCase))
		s.ValidateTableMetadata(s.dataSource, tables[testCase])
	}
}

func (s *Suite) TestReadSplit() {
	testCaseNames := []string{
		"stringOnly",
		"hashOnly",
		"mixed",
		"empty",
	}
	for _, testCase := range testCaseNames {
		s.Require().NoError(s.populateTestDataForCase(testCase))
		s.ValidateTable(s.dataSource, tables[testCase])
	}
}

// func (s *Suite) TestPositiveStats() {
// 	s.Require().NoError(s.populateTestDataForCase("mixed"))

// 	// Получаем метрики до чтения
// 	snapshot1, err := s.Connector.MetricsSnapshot()
// 	s.Require().NoError(err)

// 	// Читаем таблицу
// 	s.ValidateTable(s.dataSource, tables["mixed"])

// 	// Получаем метрики после чтения
// 	snapshot2, err := s.Connector.MetricsSnapshot()
// 	s.Require().NoError(err)

// 	// Проверяем, что метрики успешных запросов увеличились
// 	describeTableStatusOK, err := common.DiffStatusSensors(snapshot1, snapshot2, "RATE", "DescribeTable", "status_total", "OK")
// 	s.Require().NoError(err)
// 	s.Require().Equal(float64(len(s.dataSource.Instances)), describeTableStatusOK)

// 	listSplitsStatusOK, err := common.DiffStatusSensors(snapshot1, snapshot2, "RATE", "ListSplits", "stream_status_total", "OK")
// 	s.Require().NoError(err)
// 	s.Require().Equal(float64(len(s.dataSource.Instances)), listSplitsStatusOK)

// 	readSplitsStatusOK, err := common.DiffStatusSensors(snapshot1, snapshot2, "RATE", "ReadSplits", "stream_status_total", "OK")
// 	s.Require().NoError(err)
// 	s.Require().Equal(float64(len(s.dataSource.Instances)), readSplitsStatusOK)
// }

func NewSuite(
	baseSuite *suite.Base[[]byte, *array.BinaryBuilder],
) *Suite {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	return &Suite{
		Base:       baseSuite,
		dataSource: ds,
	}
}
