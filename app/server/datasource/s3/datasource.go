package s3

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/observation"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.DataSource[string] = (*dataSource)(nil)

type dataSource struct {
}

func (*dataSource) DescribeTable(
	_ context.Context,
	_ *zap.Logger,
	_ *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	return nil, fmt.Errorf("table description is not implemented for schemaless data sources: %w", common.ErrMethodNotSupported)
}

func (*dataSource) ListSplits(
	_ context.Context,
	_ *zap.Logger,
	_ *api_service_protos.TListSplitsRequest,
	_ *api_service_protos.TSelect,
	_ chan<- *datasource.ListSplitResult,
) error {
	return nil
}

func (ds *dataSource) ReadSplit(
	ctx context.Context,
	logger *zap.Logger,
	_ observation.IncomingQueryID,
	_ *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sinkFactory paging.SinkFactory[string]) error {
	return ds.doReadSplit(ctx, logger, split, sinkFactory)
}

func (*dataSource) doReadSplit(
	ctx context.Context,
	logger *zap.Logger,
	split *api_service_protos.TSplit,
	sinkFactory paging.SinkFactory[string]) error {
	conn := makeConnection()

	var (
		bucket string
		key    string
	)

	if bucket = split.Select.DataSourceInstance.GetS3Options().GetBucket(); bucket == "" {
		return fmt.Errorf("empty field `bucket`: %w", common.ErrInvalidRequest)
	}

	if key = split.Select.From.GetObjectKey(); key == "" {
		return fmt.Errorf("empty field `key`: %w", common.ErrInvalidRequest)
	}

	params := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}

	response, err := conn.GetObject(ctx, params)
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}

	defer response.Body.Close()

	csvReader := csv.NewReader(response.Body)

	ydbTypes, err := common.SelectWhatToYDBTypes(split.Select.What)
	if err != nil {
		return fmt.Errorf("select what to YDB types: %w", err)
	}

	sink, err := sinkFactory.MakeSink(logger, ydbTypes)
	if err != nil {
		return fmt.Errorf("make sinks: %w", err)
	}

	if err := transformCSV(split.Select.What, split.Select.PredefinedSchema, csvReader, sink); err != nil {
		return fmt.Errorf("transform csv: %w", err)
	}

	sink.Finish()

	return nil
}

func makeAppender(ydbType *Ydb.Type) (func(acceptor string, builder array.Builder) error, error) {
	var appender func(acceptor string, builder array.Builder) error

	typeID := ydbType.GetTypeId()
	switch typeID {
	case Ydb.Type_INT32:
		appender = func(acceptor string, builder array.Builder) error {
			value, err := strconv.Atoi(acceptor)
			if err != nil {
				return fmt.Errorf("strconv atoi '%v': %w", acceptor, err)
			}

			builder.(*array.Int32Builder).Append(int32(value))

			return nil
		}
	case Ydb.Type_STRING:
		appender = func(acceptor string, builder array.Builder) error {
			builder.(*array.StringBuilder).Append(acceptor)

			return nil
		}
	default:
		return nil, fmt.Errorf("unexpected type %v: %w", typeID, common.ErrDataTypeNotSupported)
	}

	return appender, nil
}

func prepareReading(
	selectWhat *api_service_protos.TSelect_TWhat,
	schema *api_service_protos.TSchema,
) ([]int, []func(acceptor string, builder array.Builder) error, error) {
	result := make([]int, 0, len(selectWhat.Items))
	appenders := make([]func(acceptor string, builder array.Builder) error, 0, len(selectWhat.Items))

	for _, item := range selectWhat.Items {
		for i, column := range schema.Columns {
			if item.GetColumn().Name == column.Name {
				result = append(result, i)

				appender, err := makeAppender(column.Type)
				if err != nil {
					return nil, nil, fmt.Errorf("make appender for column #%d: %w", i, err)
				}

				appenders = append(appenders, appender)
			}
		}
	}

	if len(result) != len(selectWhat.Items) {
		return nil, nil, fmt.Errorf(
			"requested column with schema mismatch (wanted %d columns, found only %d): %w",
			len(selectWhat.Items), len(result), common.ErrInvalidRequest,
		)
	}

	return result, appenders, nil
}

func transformCSV(
	selectWhat *api_service_protos.TSelect_TWhat,
	schema *api_service_protos.TSchema,
	csvReader *csv.Reader,
	_ paging.Sink[string],
) error {
	wantedColumnIDs, appenders, err := prepareReading(selectWhat, schema)
	if err != nil {
		return fmt.Errorf("get wanted columns ids: %w", err)
	}

	transformer := paging.NewRowTransformer[string](nil, appenders, wantedColumnIDs)

	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("csv reader failure: %w", err)
		}

		if len(schema.Columns) != len(row) {
			return fmt.Errorf("schema and data mismatch: expected %d columns, got %d", len(schema.Columns), len(row))
		}

		// Save the row that was just read to make data accessible for other pipeline stages
		transformer.SetAcceptors(row)
	}

	return nil
}

func makeConnection() *s3.Client {
	resolver := aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...any) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               "http://127.0.0.1:9000",
			SigningRegion:     "us-east-2",
			HostnameImmutable: true,
		}, nil
	})

	conn := s3.NewFromConfig(aws.Config{
		Region:                      "us-east-2",
		Credentials:                 credentials.NewStaticCredentialsProvider("admin", "password", ""),
		EndpointResolverWithOptions: resolver,
	}, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return conn
}

func NewDataSource() datasource.DataSource[string] {
	return &dataSource{}
}
