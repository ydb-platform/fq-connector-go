package ms_sql_server

import (
	"database/sql"
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ rdbms_utils.Rows = (*rows)(nil)

type rows struct {
	rows *sql.Rows
}

func (r rows) NextResultSet() bool {
	return r.rows.NextResultSet()
}

func (r rows) Next() bool {
	return r.rows.Next()
}

func (r rows) Err() error {
	return r.rows.Err()
}

func (r rows) ColumnTypes() ([]*sql.ColumnType, error) {
	return r.rows.ColumnTypes()
}

func (r rows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r rows) Close() error {
	return r.rows.Close()
}

func (r rows) MakeTransformer(ydbColumns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error) {
	columns, err := r.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("column types: %w", err)
	}

	typeNames := make([]string, 0, len(columns))
	for _, column := range columns {
		typeNames = append(typeNames, column.DatabaseTypeName())
	}

	transformer, err := transformerFromSQLTypes(typeNames, common.YDBColumnsToYDBTypes(ydbColumns), cc)
	if err != nil {
		return nil, fmt.Errorf("transformer from sql types: %w", err)
	}

	return transformer, nil
}
