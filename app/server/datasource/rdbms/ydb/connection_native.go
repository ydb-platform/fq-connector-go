package ydb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_sdk_query "github.com/ydb-platform/ydb-go-sdk/v3/query"
	"go.uber.org/zap"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/fq-connector-go/app/config"
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
	dsi         *api_common.TDataSourceInstance
	queryLogger common.QueryLogger
	ctx         context.Context
	driver      *ydb_sdk.Driver
}

//nolint: gocyclo
func (c *connectionNative) Query(ctx context.Context, logger *zap.Logger, query string, args ...any) (rdbms_utils.Rows, error) {
	rowsChan := make(chan rdbms_utils.Rows, 1)

	finalErr := c.driver.Query().Do(
		ctx,
		func(ctx context.Context, session ydb_sdk_query.Session) (err error) {
			// modify query with args
			queryRewritten, err := c.rewriteQuery(query, args...)
			if err != nil {
				return fmt.Errorf("rewrite query: %w", err)
			}

			// prepare parameter list
			formatter := NewSQLFormatter(config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE)
			paramsBuilder := ydb_sdk.ParamsBuilder()
			for i, arg := range args {
				switch t := arg.(type) {
				case int8:
					paramsBuilder = paramsBuilder.Param(formatter.GetPlaceholder(i)).Int8(t)
				case int16:
					paramsBuilder = paramsBuilder.Param(formatter.GetPlaceholder(i)).Int16(t)
				case int32:
					paramsBuilder = paramsBuilder.Param(formatter.GetPlaceholder(i)).Int32(t)
				case int64:
					paramsBuilder = paramsBuilder.Param(formatter.GetPlaceholder(i)).Int64(t)
				case uint8:
					paramsBuilder = paramsBuilder.Param(formatter.GetPlaceholder(i)).Uint8(t)
				case uint16:
					paramsBuilder = paramsBuilder.Param(formatter.GetPlaceholder(i)).Uint16(t)
				case uint32:
					paramsBuilder = paramsBuilder.Param(formatter.GetPlaceholder(i)).Uint32(t)
				case uint64:
					paramsBuilder = paramsBuilder.Param(formatter.GetPlaceholder(i)).Uint64(t)
				case float32:
					paramsBuilder = paramsBuilder.Param(formatter.GetPlaceholder(i)).Float(t)
				case float64:
					paramsBuilder = paramsBuilder.Param(formatter.GetPlaceholder(i)).Double(t)
				case string:
					paramsBuilder = paramsBuilder.Param(formatter.GetPlaceholder(i)).Text(t)
				case []byte:
					paramsBuilder = paramsBuilder.Param(formatter.GetPlaceholder(i)).Bytes(t)
				default:
					return fmt.Errorf("unsupported type: %T", common.ErrUnimplementedPredicateType)
				}
			}

			c.queryLogger.Dump(queryRewritten, args)

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
				if closeErr := streamResult.Close(ctx); closeErr != nil {
					logger.Error("close stream result", zap.Error(closeErr))
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
					logger.Error("close stream result", zap.Error(closeErr))
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
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *connectionNative) getDriver() *ydb_sdk.Driver {
	return c.driver
}

func (c *connectionNative) Close() error {
	if err := c.driver.Close(c.ctx); err != nil {
		return fmt.Errorf("driver close: %w", err)
	}

	return nil
}

func newConnectionNative(
	ctx context.Context,
	queryLogger common.QueryLogger,
	dsi *api_common.TDataSourceInstance,
	driver *ydb_sdk.Driver,
) ydbConnection {
	return &connectionNative{
		ctx:         ctx,
		driver:      driver,
		queryLogger: queryLogger,
		dsi:         dsi,
	}
}

func (c *connectionNative) rewriteQuery(query string, args ...any) (string, error) {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("PRAGMA TablePathPrefix(\"%s\");", c.dsi.Database)) //nolint:revive

	for i, arg := range args {
		typeName, err := getYQLTypeNameFromValue(arg)
		if err != nil {
			return "", fmt.Errorf("get YQL type name from value %v: %w", arg, err)
		}

		buf.WriteString(fmt.Sprintf("DECLARE $p%d AS %s;", i, typeName)) //nolint:revive
	}

	buf.WriteString(query) //nolint:revive

	return buf.String(), nil
}
