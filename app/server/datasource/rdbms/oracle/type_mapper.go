package oracle

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/common"
)

var _ datasource.TypeMapper = typeMapper{}

type typeMapper struct{}

func (typeMapper) SQLTypeToYDBColumn(columnName, typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {
	var (
		ydbType *Ydb.Type
		err     error
	)

	_ = rules

	// Oracle Data Types https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-7B72E154-677A-4342-A1EA-C74C1EA928E6
	// Reference table: https://github.com/ydb-platform/fq-connector-go/blob/main/docs/type_mapping_table.md
	switch typeName {
	case "NUMBER":
		// TODO: NUMBER(p, s) can be float. Should convert to Decimal
		// 	Note: NUMBER can be from 1 to 22 bytes. Has wider range than Int64 or Decimal. Possible representation - string
		ydbType = common.MakePrimitiveType(Ydb.Type_INT64)
	//// godror
	// case "VARCHAR", "VARCHAR2":
	// 	ydbType = common.MakePrimitiveType(Ydb.Type_UTF8)
	//// go-ora
	// for some reason go-ora driver doesnot disringuish VARCHAR and NCHAR from time to time. go-ora valueTypes:
	// https://github.com/sijms/go-ora/blob/78d53fdf18c31d74e7fc9e0ebe49ee1c6af0abda/parameter.go#L30-L77
	case "NCHAR", "VARCHAR", "VARCHAR2":
		ydbType = common.MakePrimitiveType(Ydb.Type_UTF8)
	default:
		return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
	}

	if err != nil {
		return nil, fmt.Errorf("convert type '%s': %w", typeName, err)
	}

	ydbType = common.MakeOptionalType(ydbType)

	return &Ydb.Column{
		Name: columnName,
		Type: ydbType,
	}, nil
}

func transformerFromSQLTypes(types []string, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	_ = ydbTypes
	acceptors := make([]any, 0, len(types))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(types))

	fmt.Printf("TFST: typeNames: %v\n", types)
	for _, typeName := range types {
		switch typeName {
		case "NUMBER":
			acceptors = append(acceptors, new(*int64))
			appenders = append(appenders, makeAppender[int64, int64, *array.Int64Builder](cc.Int64()))
		case "NCHAR", "VARCHAR", "VARCHAR2":
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, makeAppender[string, string, *array.StringBuilder](cc.String()))
		default:
			return nil, fmt.Errorf("convert type '%s': %w", typeName, common.ErrDataTypeNotSupported)
		}
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
}

func makeAppender[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](conv conversion.ValuePtrConverter[IN, OUT]) func(acceptor any, builder array.Builder) error {
	return func(acceptor any, builder array.Builder) error {
		return appendValueToArrowBuilder[IN, OUT, AB](acceptor, builder, conv)
	}
}

func appendValueToArrowBuilder[IN common.ValueType, OUT common.ValueType, AB common.ArrowBuilder[OUT]](
	acceptor any,
	builder array.Builder,
	conv conversion.ValuePtrConverter[IN, OUT],
) error {
	cast := acceptor.(**IN)

	if *cast == nil {
		builder.AppendNull()

		return nil
	}

	value := *cast

	out, err := conv.Convert(value)
	if err != nil {
		if errors.Is(err, common.ErrValueOutOfTypeBounds) {
			// TODO: write warning to logger
			builder.AppendNull()

			return nil
		}

		return fmt.Errorf("convert value %v: %w", value, err)
	}

	//nolint:forcetypeassert
	builder.(AB).Append(out)

	// it was copied from ClickHouse, not sure if it is necessary
	*cast = nil

	return nil
}

func NewTypeMapper() datasource.TypeMapper { return typeMapper{} }
