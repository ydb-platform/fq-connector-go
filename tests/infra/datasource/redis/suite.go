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
	*suite.Base[int32, *array.Int32Builder]
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

	openCtx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()

	if err := client.Ping(openCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	return client, nil
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		client, err := connectRedisFromDS(ctx, s.dataSource)
		s.Require().NoError(err)

		err = PopulateTestData(ctx, client, testCase)
		s.Require().NoError(err)

		client.Close()
		cancel()

		s.ValidateTableMetadata(s.dataSource, tables[testCase])
	}
}

func NewSuite(
	baseSuite *suite.Base[int32, *array.Int32Builder],
) *Suite {
	ds, err := deriveDataSourceFromDockerCompose(baseSuite.EndpointDeterminer)
	baseSuite.Require().NoError(err)

	return &Suite{
		Base:       baseSuite,
		dataSource: ds,
	}
}
