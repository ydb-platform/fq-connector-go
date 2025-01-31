package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.DataSource[string] = (*dataSource)(nil)

type dataSource struct {
	retrierSet *retry.RetrierSet
	cfg        *config.TMongoDbConfig
}

func NewDataSource(retrierSet *retry.RetrierSet, cfg *config.TMongoDbConfig) datasource.DataSource[string] {
	return &dataSource{retrierSet: retrierSet, cfg: cfg}
}

func (ds *dataSource) DescribeTable(
	ctx context.Context,
	logger *zap.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	dsi := request.DataSourceInstance

	if dsi.Protocol != api_common.EGenericProtocol_NATIVE {
		return nil, fmt.Errorf("cannot run MongoDb connection with protocol '%v'", dsi.Protocol)
	}

	mongoDbOptions := dsi.GetMongodbOptions()
	if mongoDbOptions == nil {
		return nil, fmt.Errorf("TMongoDbDataSourceOptions not provided")
	}

	var conn *mongo.Client

	err := ds.retrierSet.MakeConnection.Run(ctx, logger,
		func() error {
			var makeConnErr error

			uri := fmt.Sprintf(
				"mongodb://%s:%s@%s:%d/%s?%v&authSource=admin",
				dsi.Credentials.GetBasic().Username,
				dsi.Credentials.GetBasic().Password,
				dsi.Endpoint.Host,
				dsi.Endpoint.Port,
				dsi.Database,
				fmt.Sprintf("tls=%v", dsi.UseTls),
			)

			openCtx, openCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(ds.cfg.OpenConnectionTimeout))
			defer openCtxCancel()

			conn, makeConnErr = mongo.Connect(openCtx, options.Client().ApplyURI(uri))
			if makeConnErr != nil {
				return fmt.Errorf("mongo.Connect: %w", makeConnErr)
			}

			pingCtx, pingCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(ds.cfg.PingConnectionTimeout))
			defer pingCtxCancel()

			if makeConnErr = conn.Ping(pingCtx, nil); makeConnErr != nil {
				if err := conn.Disconnect(ctx); err != nil {
					logger.Fatal(fmt.Sprintf("conn.Disconnect: %v", err))
				}

				return fmt.Errorf("conn.Ping: %w", makeConnErr)
			}

			logger.Debug("Connected to MongoDB!")

			return nil
		},
	)

	if err != nil {
		return nil, fmt.Errorf("retry: %w", err)
	}

	defer func() {
		if err = conn.Disconnect(ctx); err != nil {
			logger.Fatal(fmt.Sprintf("conn.Disconnect: %v", err))
		}
	}()

	collection := conn.Database(dsi.Database).Collection(request.Table)

	cursor, err := collection.Find(ctx, bson.D{}, options.Find().SetLimit(int64(ds.cfg.GetCountDocsToDeduceSchema())))
	if err != nil {
		return nil, fmt.Errorf("colection.Find: %w", err)
	}

	defer func() {
		if err = cursor.Close(ctx); err != nil {
			logger.Fatal(fmt.Sprintf("cursor.Close: %v", err))
		}
	}()

	docs := make([]bson.Raw, 0, ds.cfg.GetCountDocsToDeduceSchema())
	for cursor.Next(ctx) {
		docs = append(docs, cursor.Current)
	}

	if err = cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor.Err(): %w", err)
	}

	omitUnsupported :=
		mongoDbOptions.UnsupportedTypeDisplayMode == api_common.TMongoDbDataSourceOptions_UNSUPPORTED_OMIT

	columns, err := bsonToYql(logger, docs, omitUnsupported)
	if err != nil {
		return nil, fmt.Errorf("bsonToYqlColumn: %w", err)
	}

	return &api_service_protos.TDescribeTableResponse{Schema: &api_service_protos.TSchema{Columns: columns}}, nil
}

func (*dataSource) ReadSplit(
	_ context.Context,
	_ *zap.Logger,
	_ *api_service_protos.TReadSplitsRequest,
	_ *api_service_protos.TSplit,
	_ paging.SinkFactory[string]) error {
	return fmt.Errorf("unimplemented")
}
