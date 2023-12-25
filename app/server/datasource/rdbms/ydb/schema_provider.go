package ydb

import (
	"context"
	"fmt"
	"path"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"go.uber.org/zap"
)

type schemaProvider struct {
	typeMapper utils.TypeMapper
}

var _ rdbms_utils.SchemaProvider = (*schemaProvider)(nil)

func (f *schemaProvider) GetSchema(
	ctx context.Context,
	logger *zap.Logger,
	conn rdbms_utils.Connection,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TSchema, error) {
	ydbConn := conn.(*Connection)

	db, err := ydb.Unwrap(ydbConn.DB)
	if err != nil {
		return nil, fmt.Errorf("unwrap connection: %w", err)
	}

	desc := options.Description{}
	prefix := path.Join(db.Name(), request.Table)
	cl := db.Table()

	err = cl.Do(
		ctx,
		func(ctx context.Context, s table.Session) error {
			desc, err = s.DescribeTable(ctx, prefix)
			if err != nil {
				return fmt.Errorf("describe table: %w", err)
			}
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("get table description: %w", err)
	}

	sb := rdbms_utils.NewSchemaBuilder(f.typeMapper, request.TypeMappingSettings)
	for _, column := range desc.Columns {
		if err = sb.AddColumn(column.Name, column.Type.String()); err != nil {
			return nil, fmt.Errorf("add column to schema builder: %w", err)
		}
	}

	schema, err := sb.Build(logger)
	if err != nil {
		return nil, fmt.Errorf("build schema: %w", err)
	}

	return schema, nil
}

func NewSchemaProvider(
	typeMapper utils.TypeMapper,
) rdbms_utils.SchemaProvider {
	return &schemaProvider{
		typeMapper: typeMapper,
	}
}
