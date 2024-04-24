package ms_sql_server

import (
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

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

func (r rows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r rows) Close() error {
	return r.rows.Close()
}

func (r rows) MakeTransformer(ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	// fields := r.FieldDescriptions()

	// oids := make([]uint32, 0, len(fields))
	// for _, field := range fields {
	// 	oids = append(oids, field.DataTypeOID)
	// }

	// return transformerFromOIDs(oids, ydbTypes, cc)
	return nil, nil
}
