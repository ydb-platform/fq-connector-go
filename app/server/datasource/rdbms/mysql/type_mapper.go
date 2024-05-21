package mysql

import (
	"errors"
	"fmt"
	"strings"

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

func (typeMapper) SQLTypeToYDBColumn(columnName, columnType string, _ *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {
	rawColumnType := strings.Split(columnType, " ")

	nullable := strings.Contains(columnType, "nullable")
	unsigned := strings.Contains(columnType, "unsigned")

	var ydbColumn Ydb.Column

	switch rawColumnType[0] {
	case "int", "mediumint":
		var ydbType Ydb.Type_PrimitiveTypeId

		if unsigned {
			ydbType = Ydb.Type_UINT32
		} else {
			ydbType = Ydb.Type_INT32
		}

		ydbColumn = Ydb.Column{
			Name: columnName,
			Type: common.MakePrimitiveType(ydbType),
		}
	case "float":
		ydbColumn = Ydb.Column{
			Name: columnName,
			Type: common.MakePrimitiveType(Ydb.Type_FLOAT),
		}
	case "double":
		ydbColumn = Ydb.Column{
			Name: columnName,
			Type: common.MakePrimitiveType(Ydb.Type_DOUBLE),
		}
	case "smallint", "tinyint", "bool":
		var ydbType Ydb.Type_PrimitiveTypeId

		if unsigned {
			ydbType = Ydb.Type_UINT16
		} else {
			ydbType = Ydb.Type_INT16
		}

		ydbColumn = Ydb.Column{
			Name: columnName,
			Type: common.MakePrimitiveType(ydbType),
		}
	case "longblob", "blob", "mediumblob", "tinyblob":
		ydbColumn = Ydb.Column{
			Name: columnName,
			Type: common.MakePrimitiveType(Ydb.Type_STRING),
		}
	case "varchar", "string", "text", "longtext", "tinytext", "mediumtext":
		ydbColumn = Ydb.Column{
			Name: columnName,
			Type: common.MakePrimitiveType(Ydb.Type_UTF8),
		}
	default:
		return nil, errors.New("mysql: datatype not implemented yet")
	}

	if nullable {
		ydbColumn.Type = common.MakeOptionalType(ydbColumn.GetType())
	}

	return &ydbColumn, nil
}

func NewTypeMapper() datasource.TypeMapper { return typeMapper{} }

func transformerFromTypeIDs(_ []uint8, ydbTypes []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(ydbTypes))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(ydbTypes))

	for _, ydbType := range ydbTypes {
		var typeId Ydb.Type_PrimitiveTypeId
		// nullable := true

		if opt := ydbType.GetOptionalType(); opt != nil {
			typeId = opt.Item.GetTypeId()
		} else {
			typeId = ydbType.GetTypeId()
			// nullable = false
		}

		switch typeId {
		case Ydb.Type_UINT16:
			acceptors = append(acceptors, new(uint16))
			appenders = append(appenders, makeAppender[uint16, uint16, *array.Uint16Builder](cc.Uint16()))
		case Ydb.Type_INT16, Ydb.Type_BOOL:
			acceptors = append(acceptors, new(int16))
			appenders = append(appenders, makeAppender[int16, int16, *array.Int16Builder](cc.Int16()))
		case Ydb.Type_UINT32:
			acceptors = append(acceptors, new(uint32))
			appenders = append(appenders, makeAppender[uint32, uint32, *array.Uint32Builder](cc.Uint32()))
		case Ydb.Type_INT32:
			acceptors = append(acceptors, new(int32))
			appenders = append(appenders, makeAppender[int32, int32, *array.Int32Builder](cc.Int32()))
		case Ydb.Type_FLOAT:
			acceptors = append(acceptors, new(float32))
			appenders = append(appenders, makeAppender[float32, float32, *array.Float32Builder](cc.Float32()))
		case Ydb.Type_DOUBLE:
			acceptors = append(acceptors, new(float64))
			appenders = append(appenders, makeAppender[float64, float64, *array.Float64Builder](cc.Float64()))
		case Ydb.Type_UTF8:
			acceptors = append(acceptors, new(string))
			appenders = append(appenders, makeAppender[string, string, *array.StringBuilder](cc.String()))
		case Ydb.Type_STRING:
			acceptors = append(acceptors, new([]byte))
			appenders = append(appenders, makeAppender[[]byte, []byte, *array.BinaryBuilder](cc.Bytes()))
		default:
			return nil, errors.New("mysql: datatype not implemented yet")
		}
	}

	return paging.NewRowTransformer(acceptors, appenders, nil), nil
}

func makeAppender[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](conv conversion.ValueConverter[IN, OUT]) func(acceptor any, builder array.Builder) error {
	return func(acceptor any, builder array.Builder) error {
		return appendValueToArrowBuilder[IN, OUT, AB](acceptor, builder, conv)
	}
}

func appendValueToArrowBuilder[IN common.ValueType, OUT common.ValueType, AB common.ArrowBuilder[OUT]](
	acceptor any,
	builder array.Builder,
	conv conversion.ValueConverter[IN, OUT],
) error {
	cast := acceptor.(*IN)

	if cast == nil {
		builder.AppendNull()

		return nil
	}

	value := *cast

	out, err := conv.Convert(value)
	if err != nil {
		if errors.Is(err, common.ErrValueOutOfTypeBounds) {
			builder.AppendNull()

			return nil
		}

		return fmt.Errorf("convert value %v: %w", value, err)
	}

	//nolint:forcetypeassert
	builder.(AB).Append(out)

	cast = nil

	return nil
}
