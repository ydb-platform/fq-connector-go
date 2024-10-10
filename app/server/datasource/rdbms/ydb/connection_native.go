package ydb

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_sdk_query "github.com/ydb-platform/ydb-go-sdk/v3/query"
	"go.uber.org/zap"
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
	ctx    context.Context
	driver *ydb_sdk.Driver
}

func (c *connectionNative) Query(ctx context.Context, logger *zap.Logger, query string, args ...any) (rdbms_utils.Rows, error) {
	rowsChan := make(chan rdbms_utils.Rows, 1)

	c.driver.Query().Do(
		ctx,
		func(ctx context.Context, session ydb_sdk_query.Session) (err error) {
			streamResult, err := session.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("session query: %w", err)
			}

			rows := &rowsNative{
				ctx:          ctx,
				streamResult: streamResult,
			}

			select {
			case rowsChan <- rows:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	)

	select {
	case rows := <-rowsChan:
		return rows, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *connectionNative) Close() error {
	if err := c.driver.Close(c.ctx); err != nil {
		return fmt.Errorf("driver close: %w", err)
	}

	return nil
}