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
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils/retry"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.DataSource[any] = (*dataSource)(nil)

type dataSource struct {
	retrierSet *retry.RetrierSet
	cc         conversion.Collection
	cfg        *config.TMongoDbConfig
}

func NewDataSource(retrierSet *retry.RetrierSet, cc conversion.Collection, cfg *config.TMongoDbConfig) datasource.DataSource[any] {
	return &dataSource{retrierSet: retrierSet, cc: cc, cfg: cfg}
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

	if mongoDbOptions.ReadingMode != api_common.TMongoDbDataSourceOptions_TABLE {
		return nil, fmt.Errorf("unsupported reading_mode: %s", mongoDbOptions.ReadingMode.String())
	}

	var conn *mongo.Client

	err := ds.retrierSet.MakeConnection.Run(ctx, logger,
		func() error {
			var err error
			conn, err = ds.makeConnection(ctx, logger, dsi)

			return err
		},
	)

	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	defer func() {
		if err = conn.Disconnect(ctx); err != nil {
			logger.Error(fmt.Sprintf("disconnect: %v", err))
		}
	}()

	collection := conn.Database(dsi.Database).Collection(request.Table)

	cursor, err := collection.Find(ctx, bson.D{}, options.Find().SetLimit(int64(ds.cfg.GetCountDocsToDeduceSchema())))
	if err != nil {
		return nil, fmt.Errorf("find in collection: %w", err)
	}

	defer func() {
		if err = cursor.Close(ctx); err != nil {
			logger.Error(fmt.Sprintf("cursor close: %v", err))
		}
	}()

	docs := make([]bson.Raw, 0, ds.cfg.GetCountDocsToDeduceSchema())
	for cursor.Next(ctx) {
		docs = append(docs, cursor.Current)
	}

	if err = cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor: %w", err)
	}

	objectIdType := ds.cfg.GetObjectIdYqlType()

	omitUnsupported :=
		mongoDbOptions.UnsupportedTypeDisplayMode == api_common.TMongoDbDataSourceOptions_UNSUPPORTED_OMIT

	columns, err := bsonToYql(logger, docs, omitUnsupported, objectIdType)
	if err != nil {
		return nil, fmt.Errorf("bsonToYqlColumn: %w", err)
	}

	return &api_service_protos.TDescribeTableResponse{Schema: &api_service_protos.TSchema{Columns: columns}}, nil
}

func (*dataSource) ListSplits(
	ctx context.Context,
	_ *zap.Logger,
	_ *api_service_protos.TListSplitsRequest,
	slct *api_service_protos.TSelect,
	resultChan chan<- *datasource.ListSplitResult) error {
	// By default we deny table splitting
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
	sinkFactory paging.SinkFactory[any]) error {
	dsi := split.Select.DataSourceInstance

	if dsi.Protocol != api_common.EGenericProtocol_NATIVE {
		return fmt.Errorf("cannot run MongoDb connection with protocol '%v'", dsi.Protocol)
	}

	mongoDbOptions := dsi.GetMongodbOptions()
	if mongoDbOptions == nil {
		return fmt.Errorf("TMongoDbDataSourceOptions not provided")
	}

	if mongoDbOptions.ReadingMode != api_common.TMongoDbDataSourceOptions_TABLE {
		return fmt.Errorf("unsupported reading_mode: %s", mongoDbOptions.ReadingMode.String())
	}

	var conn *mongo.Client

	err := ds.retrierSet.MakeConnection.Run(ctx, logger,
		func() error {
			var connErr error
			conn, connErr = ds.makeConnection(ctx, logger, dsi)

			return connErr
		},
	)

	if err != nil {
		return fmt.Errorf("make connection: %w", err)
	}

	defer func() {
		if err = conn.Disconnect(ctx); err != nil {
			logger.Error(fmt.Sprintf("disconnect: %v", err))
		}
	}()

	sinks, err := sinkFactory.MakeSinks([]*paging.SinkParams{{Logger: logger}})
	if err != nil {
		return fmt.Errorf("make sinks: %w", err)
	}

	return ds.doReadSplitSingleConn(ctx, logger, dsi, mongoDbOptions, request, split, sinks[0], conn)
}

func (ds *dataSource) makeConnection(
	ctx context.Context,
	logger *zap.Logger,
	dsi *api_common.TGenericDataSourceInstance,
) (*mongo.Client, error) {
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

	conn, makeConnErr := mongo.Connect(openCtx, options.Client().ApplyURI(uri))
	if makeConnErr != nil {
		return nil, fmt.Errorf("connect: %w", makeConnErr)
	}

	pingCtx, pingCtxCancel := context.WithTimeout(ctx, common.MustDurationFromString(ds.cfg.PingConnectionTimeout))
	defer pingCtxCancel()

	if makeConnErr = conn.Ping(pingCtx, nil); makeConnErr != nil {
		if err := conn.Disconnect(ctx); err != nil {
			logger.Error(fmt.Sprintf("disconnect: %v", err))
		}

		return nil, fmt.Errorf("ping: %w", makeConnErr)
	}

	logger.Debug("Connected to MongoDB!")

	return conn, nil
}

func (ds *dataSource) doReadSplitSingleConn(
	ctx context.Context,
	logger *zap.Logger,
	dsi *api_common.TGenericDataSourceInstance,
	mongoDbOptions *api_common.TMongoDbDataSourceOptions,
	_ *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sink paging.Sink[any],
	conn *mongo.Client,
) error {
	collection := conn.Database(dsi.Database).Collection(split.Select.From.Table)

	filter := bson.D{}
	opts := options.Find()

	var cursor *mongo.Cursor

	err := ds.retrierSet.Query.Run(
		ctx,
		logger,
		func() error {
			var queryErr error

			cursor, queryErr = collection.Find(ctx, filter, opts)
			if queryErr != nil {
				return fmt.Errorf("find in collection: %w", queryErr)
			}

			return nil
		},
	)
	if err != nil {
		return err
	}

	defer cursor.Close(ctx)

	arrowSchema, err := common.SelectWhatToArrowSchema(split.Select.What)
	if err != nil {
		return fmt.Errorf("select what to Arrow schema: %w", err)
	}

	ydbSchema, err := common.SelectWhatToYDBTypes(split.Select.What)
	if err != nil {
		return fmt.Errorf("select what to YDB schema: %w", err)
	}

	reader, err := makeDocumentReader(mongoDbOptions.ReadingMode, mongoDbOptions.UnexpectedTypeDisplayMode, arrowSchema, ydbSchema, ds.cc)
	if err != nil {
		return fmt.Errorf("make document reader: %w", err)
	}

	for cursor.Next(ctx) {
		var doc bson.M

		if err = cursor.Decode(&doc); err != nil {
			return fmt.Errorf("decode: %w", err)
		}

		if err = reader.accept(doc); err != nil {
			return fmt.Errorf("accept document: %w", err)
		}

		if err = sink.AddRow(reader.transformer); err != nil {
			return fmt.Errorf("add row to sink: %w", err)
		}
	}

	if err = cursor.Err(); err != nil {
		return fmt.Errorf("cursor: %w", err)
	}

	sink.Finish()

	return nil
}
