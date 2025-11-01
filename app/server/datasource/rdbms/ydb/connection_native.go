package ydb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_sdk_query "github.com/ydb-platform/ydb-go-sdk/v3/query"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Rows = (*rowsNative)(nil)

type rowsNative struct {
	ctx context.Context
	err error

	streamResult  ydb_sdk_query.Result
	lastResultSet ydb_sdk_query.ResultSet
	lastRow       ydb_sdk_query.Row

	closeChan chan struct{}
}

func (r *rowsNative) Next() bool {
	var err error

	r.lastRow, err = r.lastResultSet.NextRow(r.ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.err = nil
		} else {
			r.err = fmt.Errorf("next row: %w", err)
		}

		return false
	}

	return true
}

func (r *rowsNative) NextResultSet() bool {
	var err error

	r.lastResultSet, err = r.streamResult.NextResultSet(r.ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.err = nil
		} else {
			r.err = fmt.Errorf("next result set: %w", err)
		}

		return false
	}

	return true
}

func (r *rowsNative) Scan(dest ...any) error {
	if err := r.lastRow.Scan(dest...); err != nil {
		return fmt.Errorf("last row scan: %w", err)
	}

	return nil
}

func (r *rowsNative) MakeTransformer(ydbColumns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error) {
	if r.lastResultSet == nil {
		return nil, errors.New("last result set is not ready yet")
	}

	columnTypes := r.lastResultSet.ColumnTypes()
	typeNames := make([]string, 0, len(columnTypes))

	for _, columnType := range columnTypes {
		typeNames = append(typeNames, columnType.Yql())
	}

	transformer, err := transformerFromSQLTypes(typeNames, common.YDBColumnsToYDBTypes(ydbColumns), cc)
	if err != nil {
		return nil, fmt.Errorf("transformer from sql types: %w", err)
	}

	return transformer, nil
}

func (r *rowsNative) Err() error {
	return r.err
}

func (r *rowsNative) Close() error {
	close(r.closeChan)

	return nil
}

var _ rdbms_utils.Columns = (*columnsNative)(nil)

type columnsNative struct {
	ctx         context.Context
	err         error
	arrowResult ydb_sdk_query.ArrowResult
	currentPart io.Reader
	reader      *ipc.Reader
	record      arrow.Record
	closeChan   chan struct{}
}

func (c *columnsNative) Close() error {
	if err := c.arrowResult.Close(c.ctx); err != nil {
		return fmt.Errorf("arrow result close: %w", err)
	}

	return nil
}

func (c *columnsNative) Err() error {
	return c.err
}

func (c *columnsNative) Next() bool {
	// If we have a reader and it has more records, get the next one
	if c.reader != nil && c.reader.Next() {
		c.record = c.reader.Record()

		return true
	}

	// Try to get the next part
	var part io.Reader

	var err error

	for p, e := range c.arrowResult.Parts(c.ctx) {
		if e != nil {
			if errors.Is(e, io.EOF) {
				c.err = nil
			} else {
				c.err = fmt.Errorf("next part: %w", e)
			}

			return false
		}

		part = p

		break
	}

	if part == nil {
		return false
	}

	// Create a new reader for this part
	c.currentPart = part

	reader, err := ipc.NewReader(part)
	if err != nil {
		c.err = fmt.Errorf("create arrow reader: %w", err)

		return false
	}

	c.reader = reader

	// Get the first record from this part
	if !c.reader.Next() {
		c.err = errors.New("no records in arrow part")

		return false
	}

	c.record = c.reader.Record()

	return true
}

func (c *columnsNative) Record() arrow.Record {
	return c.record
}

var _ rdbms_utils.Connection = (*connectionNative)(nil)

type connectionNative struct {
	dsi                *api_common.TGenericDataSourceInstance
	logger             *zap.Logger
	queryLoggerFactory common.QueryLoggerFactory
	driver             *ydb_sdk.Driver
	tableName          string
	formatter          rdbms_utils.SQLFormatter
	resourcePool       string
	queryDataFormat    api_common.TYdbDataSourceOptions_EQueryDataFormat
}

// nolint: gocyclo,funlen
func (c *connectionNative) Query(params *rdbms_utils.QueryParams) (*rdbms_utils.QueryResult, error) {
	paramsBuilder := ydb_sdk.ParamsBuilder()

	// modify query with args
	queryRewritten, err := c.rewriteQuery(params)
	if err != nil {
		return nil, fmt.Errorf("rewrite query: %w", err)
	}

	for i, arg := range params.QueryArgs.Values() {
		placeholder := c.formatter.GetPlaceholder(i)

		switch t := arg.(type) {
		case bool:
			paramsBuilder = paramsBuilder.Param(placeholder).Bool(t)
		case *bool:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Bool(t).EndOptional()
		case int8:
			paramsBuilder = paramsBuilder.Param(placeholder).Int8(t)
		case *int8:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Int8(t).EndOptional()
		case int16:
			paramsBuilder = paramsBuilder.Param(placeholder).Int16(t)
		case *int16:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Int16(t).EndOptional()
		case int32:
			paramsBuilder = paramsBuilder.Param(placeholder).Int32(t)
		case *int32:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Int32(t).EndOptional()
		case int64:
			paramsBuilder = paramsBuilder.Param(placeholder).Int64(t)
		case *int64:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Int64(t).EndOptional()
		case uint8:
			paramsBuilder = paramsBuilder.Param(placeholder).Uint8(t)
		case *uint8:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Uint8(t).EndOptional()
		case uint16:
			paramsBuilder = paramsBuilder.Param(placeholder).Uint16(t)
		case *uint16:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Uint16(t).EndOptional()
		case uint32:
			paramsBuilder = paramsBuilder.Param(placeholder).Uint32(t)
		case *uint32:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Uint32(t).EndOptional()
		case uint64:
			paramsBuilder = paramsBuilder.Param(placeholder).Uint64(t)
		case *uint64:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Uint64(t).EndOptional()
		case float32:
			paramsBuilder = paramsBuilder.Param(placeholder).Float(t)
		case *float32:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Float(t).EndOptional()
		case float64:
			paramsBuilder = paramsBuilder.Param(placeholder).Double(t)
		case *float64:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Double(t).EndOptional()
		case string:
			paramsBuilder = paramsBuilder.Param(placeholder).Text(t)
		case *string:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Text(t).EndOptional()
		case []byte:
			paramsBuilder = paramsBuilder.Param(placeholder).Bytes(t)
		case *[]byte:
			paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Bytes(t).EndOptional()
		case time.Time:
			switch params.QueryArgs.Get(i).YdbType.GetTypeId() {
			case Ydb.Type_TIMESTAMP:
				paramsBuilder = paramsBuilder.Param(placeholder).Timestamp(t)
			default:
				return nil, fmt.Errorf("unsupported type: %v (%T): %w", arg, arg, common.ErrUnimplementedPredicateType)
			}
		case *time.Time:
			switch params.QueryArgs.Get(i).YdbType.GetOptionalType().GetItem().GetTypeId() {
			case Ydb.Type_TIMESTAMP:
				paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Timestamp(t).EndOptional()
			default:
				return nil, fmt.Errorf("unsupported type: %v (%T): %w", arg, arg, common.ErrUnimplementedPredicateType)
			}
		default:
			return nil, fmt.Errorf("unsupported type: %v (%T): %w", arg, arg, common.ErrUnimplementedPredicateType)
		}
	}

	type result struct {
		result *rdbms_utils.QueryResult
		err    error
	}

	// We cannot use the results of a query from outside of the SDK callback.
	// See https://github.com/ydb-platform/ydb-go-sdk/issues/1862 for details.
	resultChan := make(chan result)

	// context coming from the client (the federated YDB)
	parentCtx := params.Ctx

	go func() {
		finalErr := c.driver.Query().Do(
			parentCtx,
			func(ctx context.Context, session ydb_sdk_query.Session) (err error) {
				queryLogger := c.queryLoggerFactory.Make(params.Logger, zap.String("resource_pool", c.resourcePool))
				queryLogger.Dump(queryRewritten, params.QueryArgs.Values()...)

				var queryResult *rdbms_utils.QueryResult

				switch c.queryDataFormat {
				case api_common.TYdbDataSourceOptions_QUERY_DATA_FORMAT_UNSPECIFIED:
					// execute query
					streamResult, err := session.Query(
						ctx,
						queryRewritten,
						ydb_sdk_query.WithParameters(paramsBuilder.Build()),
						ydb_sdk_query.WithResourcePool(c.resourcePool),
					)
					if err != nil {
						return fmt.Errorf("session query: %w", err)
					}

					defer func() {
						if closeErr := streamResult.Close(ctx); closeErr != nil {
							params.Logger.Error("close stream result", zap.Error(closeErr))
						}
					}()

					// obtain first result set because it's necessary
					// to create type transformers
					resultSet, err := streamResult.NextResultSet(ctx)
					if err != nil {
						return fmt.Errorf("next result set: %w", err)
					}

					queryResult = &rdbms_utils.QueryResult{
						Rows: &rowsNative{
							ctx:           parentCtx,
							streamResult:  streamResult,
							lastResultSet: resultSet,
							closeChan:     make(chan struct{}),
						},
					}
				case api_common.TYdbDataSourceOptions_ARROW:
					// execute query
					arrowResult, err := session.QueryArrow(
						ctx,
						queryRewritten,
						ydb_sdk_query.WithParameters(paramsBuilder.Build()),
						ydb_sdk_query.WithResourcePool(c.resourcePool),
					)
					if err != nil {
						return fmt.Errorf("session query: %w", err)
					}

					defer func() {
						if closeErr := arrowResult.Close(ctx); closeErr != nil {
							params.Logger.Error("close stream result", zap.Error(closeErr))
						}
					}()

					queryResult = &rdbms_utils.QueryResult{
						Columns: &columnsNative{
							ctx:         parentCtx,
							arrowResult: arrowResult,
							closeChan:   make(chan struct{}),
						},
					}
				default:
					return fmt.Errorf("unsupported query data format: %v", c.queryDataFormat)
				}

				// push iterator over GRPC stream into the outer space
				select {
				case resultChan <- result{result: queryResult}:
				case <-ctx.Done():
					return ctx.Err()
				}

				// Keep waiting until the rowsNative/columnsNative object is closed by a caller.
				// The context (and the objects) will be otherwise invalidated by the SDK.
				select {
				case <-extractCloseChan(queryResult):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			ydb_sdk_query.WithIdempotent(),
		)

		// If the error is not nil, that means that callback didn't return the result via channel,
		// so we need to write the error into the channel here.
		if finalErr != nil {
			select {
			case resultChan <- result{err: fmt.Errorf("query do: %w", finalErr)}:
			case <-parentCtx.Done():
			}
		}
	}()

	select {
	case r := <-resultChan:
		if r.err != nil {
			return nil, r.err
		}

		return r.result, nil
	case <-parentCtx.Done():
		return nil, parentCtx.Err()
	}
}

func extractCloseChan(queryResult *rdbms_utils.QueryResult) <-chan struct{} {
	if queryResult.Rows != nil {
		return queryResult.Rows.(*rowsNative).closeChan
	}

	return queryResult.Columns.(*columnsNative).closeChan
}

func (c *connectionNative) Driver() *ydb_sdk.Driver {
	return c.driver
}

func (c *connectionNative) DataSourceInstance() *api_common.TGenericDataSourceInstance {
	return c.dsi
}

func (c *connectionNative) TableName() string {
	return c.tableName
}

func (c *connectionNative) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.driver.Close(ctx); err != nil {
		return fmt.Errorf("driver close: %w", err)
	}

	return nil
}

func (c *connectionNative) rewriteQuery(params *rdbms_utils.QueryParams) (string, error) {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("PRAGMA TablePathPrefix(\"%s\");\n", c.dsi.Database)) //nolint:revive

	for i, arg := range params.QueryArgs.GetAll() {
		var primitiveTypeID Ydb.Type_PrimitiveTypeId

		if arg.YdbType.GetOptionalType() != nil {
			internalType := arg.YdbType.GetOptionalType().GetItem()

			switch t := internalType.GetType().(type) {
			case *Ydb.Type_TypeId:
				primitiveTypeID = t.TypeId
			default:
				return "", fmt.Errorf("optional type contains no primitive type: %v", arg.YdbType)
			}
		} else {
			primitiveTypeID = arg.YdbType.GetTypeId()
		}

		typeName, err := primitiveYqlTypeName(primitiveTypeID)
		if err != nil {
			return "", fmt.Errorf("get YQL type name from value %v: %w", arg, err)
		}

		if arg.YdbType.GetOptionalType() != nil {
			typeName = fmt.Sprintf("%s?", typeName)
		}

		buf.WriteString(fmt.Sprintf("DECLARE $p%d AS %s;\n", i, typeName)) //nolint:revive
	}

	buf.WriteString(params.QueryText) //nolint:revive

	return buf.String(), nil
}

func (c *connectionNative) Logger() *zap.Logger {
	return c.logger
}

func newConnectionNative(
	logger *zap.Logger,
	queryLoggerFactory common.QueryLoggerFactory,
	dsi *api_common.TGenericDataSourceInstance,
	tableName string,
	driver *ydb_sdk.Driver,
	formatter rdbms_utils.SQLFormatter,
	resourcePool string,
	queryDataFormat api_common.TYdbDataSourceOptions_EQueryDataFormat,
) Connection {
	return &connectionNative{
		driver:             driver,
		logger:             logger,
		queryLoggerFactory: queryLoggerFactory,
		dsi:                dsi,
		tableName:          tableName,
		formatter:          formatter,
		resourcePool:       resourcePool,
		queryDataFormat:    queryDataFormat,
	}
}
