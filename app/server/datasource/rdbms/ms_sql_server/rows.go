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

func (rows) NextResultSet() bool {
	return false
}

func (r rows) Next() bool {
	return false
}

func (r rows) Err() error {
	return nil
}

func (r rows) Scan(dest ...any) error {
	return nil
}

func (r rows) Close() error {
	// r.Rows.Close()

	return nil
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
