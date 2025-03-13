package ydb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_sdk_query "github.com/ydb-platform/ydb-go-sdk/v3/query"
	"go.uber.org/zap"

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
		return fmt.Errorf("rows scan: %w", err)
	}

	return nil
}

func (r *rowsNative) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	if r.lastResultSet == nil {
		return nil, fmt.Errorf("last result set is not ready yet")
	}

	columnTypes := r.lastResultSet.ColumnTypes()
	typeNames := make([]string, 0, len(columnTypes))

	for _, columnType := range columnTypes {
		typeNames = append(typeNames, columnType.Yql())
	}

	transformer, err := transformerFromSQLTypes(typeNames, ydbTypes, cc)
	if err != nil {
		return nil, fmt.Errorf("transformer from sql types: %w", err)
	}

	return transformer, nil
}

func (r *rowsNative) Err() error {
	return r.err
}

func (r *rowsNative) Close() error {
	if err := r.streamResult.Close(r.ctx); err != nil {
		return fmt.Errorf("stream result close: %w", err)
	}

	return nil
}

var _ rdbms_utils.Connection = (*connectionNative)(nil)

type connectionNative struct {
	dsi         *api_common.TGenericDataSourceInstance
	queryLogger common.QueryLogger
	ctx         context.Context
	driver      *ydb_sdk.Driver
	tableName   string
	formatter   rdbms_utils.SQLFormatter
}

// nolint: gocyclo
func (c *connectionNative) Query(params *rdbms_utils.QueryParams) (rdbms_utils.Rows, error) {
	rowsChan := make(chan rdbms_utils.Rows, 1)

	finalErr := c.driver.Query().Do(
		params.Ctx,
		func(ctx context.Context, session ydb_sdk_query.Session) (err error) {
			// modify query with args
			queryRewritten, err := c.rewriteQuery(params)
			if err != nil {
				return fmt.Errorf("rewrite query: %w", err)
			}

			// prepare parameter list
			paramsBuilder := ydb_sdk.ParamsBuilder()

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
						return fmt.Errorf("unsupported type: %v (%T): %w", arg, arg, common.ErrUnimplementedPredicateType)
					}
				case *time.Time:
					switch params.QueryArgs.Get(i).YdbType.GetOptionalType().GetItem().GetTypeId() {
					case Ydb.Type_TIMESTAMP:
						paramsBuilder = paramsBuilder.Param(placeholder).BeginOptional().Timestamp(t).EndOptional()
					default:
						return fmt.Errorf("unsupported type: %v (%T): %w", arg, arg, common.ErrUnimplementedPredicateType)
					}
				default:
					return fmt.Errorf("unsupported type: %v (%T): %w", arg, arg, common.ErrUnimplementedPredicateType)
				}
			}

			c.queryLogger.Dump(queryRewritten, params.QueryArgs.Values()...)

			// execute query
			streamResult, err := session.Query(
				ctx,
				queryRewritten,
				ydb_sdk_query.WithParameters(paramsBuilder.Build()))
			if err != nil {
				return fmt.Errorf("session query: %w", err)
			}

			// obtain first result set because it's necessary
			// to create type transformers
			resultSet, err := streamResult.NextResultSet(ctx)
			if err != nil {
				if closeErr := streamResult.Close(context.Background()); closeErr != nil {
					params.Logger.Error("close stream result", zap.Error(closeErr))
				}

				return fmt.Errorf("next result set: %w", err)
			}

			rows := &rowsNative{
				ctx:           c.ctx,
				streamResult:  streamResult,
				lastResultSet: resultSet,
			}

			select {
			case rowsChan <- rows:
				return nil
			case <-ctx.Done():
				if closeErr := streamResult.Close(ctx); closeErr != nil {
					params.Logger.Error("close stream result", zap.Error(closeErr))
				}

				return ctx.Err()
			}
		},
		ydb_sdk_query.WithIdempotent(),
	)

	if finalErr != nil {
		return nil, fmt.Errorf("query do: %w", finalErr)
	}

	select {
	case rows := <-rowsChan:
		return rows, nil
	case <-params.Ctx.Done():
		return nil, params.Ctx.Err()
	}
}

func (c *connectionNative) Driver() *ydb_sdk.Driver {
	return c.driver
}

func (c *connectionNative) From() (datbaseName, tableName string) {
	return c.dsi.Database, c.tableName
}

func (c *connectionNative) Close() error {
	if err := c.driver.Close(c.ctx); err != nil {
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
	return c.queryLogger.Logger
}

func newConnectionNative(
	ctx context.Context,
	queryLogger common.QueryLogger,
	dsi *api_common.TGenericDataSourceInstance,
	tableName string,
	driver *ydb_sdk.Driver,
	formatter rdbms_utils.SQLFormatter,
) Connection {
	return &connectionNative{
		ctx:         ctx,
		driver:      driver,
		queryLogger: queryLogger,
		dsi:         dsi,
		tableName:   tableName,
		formatter:   formatter,
	}
}
