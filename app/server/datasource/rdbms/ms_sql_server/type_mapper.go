package ms_sql_server

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	_ "github.com/denisenkom/go-mssqldb"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.TypeMapper = typeMapper{}

type typeMapper struct{}

func (typeMapper) SQLTypeToYDBColumn(columnName, typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {

	return &Ydb.Column{
		// Name: columnName,
		// Type: ydbType,
	}, nil
}

//nolint:gocyclo
func transformerFromOIDs(oids []uint32, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {

	return paging.NewRowTransformer[any](nil, nil, nil), nil
}

func appendValueToArrowBuilder[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](
	value any,
	builder array.Builder,
	valid bool,
	conv conversion.ValueConverter[IN, OUT],
) error {

	return nil
}

func appendValuePtrToArrowBuilder[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](
	value any,
	builder array.Builder,
	valid bool,
	conv conversion.ValuePtrConverter[IN, OUT],
) error {

	return nil
}

func NewTypeMapper() datasource.TypeMapper { return typeMapper{} }
