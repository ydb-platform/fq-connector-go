package mysql

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/go-mysql-org/go-mysql/mysql"
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
	switch columnType {
	case "int", "mediumint":
		return &Ydb.Column{
			Name: columnName,
			Type: common.MakePrimitiveType(Ydb.Type_INT32),
		}, nil
	case "float":
		return &Ydb.Column{
			Name: columnName,
			Type: common.MakePrimitiveType(Ydb.Type_FLOAT),
		}, nil
	case "double":
		return &Ydb.Column{
			Name: columnName,
			Type: common.MakePrimitiveType(Ydb.Type_DOUBLE),
		}, nil
	case "smallint", "tinyint":
		return &Ydb.Column{
			Name: columnName,
			Type: common.MakePrimitiveType(Ydb.Type_INT16),
		}, nil
	case "varchar", "string", "longblob", "blob", "text":
		return &Ydb.Column{
			Name: columnName,
			Type: common.MakePrimitiveType(Ydb.Type_UTF8),
		}, nil
	default:
		return nil, errors.New("mysql: datatype not implemented yet")
	}
}

func NewTypeMapper() datasource.TypeMapper { return typeMapper{} }

func transformerFromTypeIDs(ids []uint8, _ []*Ydb.Type, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(ids))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(ids))

	for _, id := range ids {
		switch id {
		case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_INT24:
			acceptors = append(acceptors, new(*int32))
			appenders = append(appenders, makeAppender[int32, int32, *array.Int32Builder](cc.Int32()))
		case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_TINY:
			acceptors = append(acceptors, new(*int16))
			appenders = append(appenders, makeAppender[int16, int16, *array.Int16Builder](cc.Int16()))
		case mysql.MYSQL_TYPE_FLOAT:
			acceptors = append(acceptors, new(*float32))
			appenders = append(appenders, makeAppender[float32, float32, *array.Float32Builder](cc.Float32()))
		case mysql.MYSQL_TYPE_DOUBLE:
			acceptors = append(acceptors, new(*float64))
			appenders = append(appenders, makeAppender[float64, float64, *array.Float64Builder](cc.Float64()))
		case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_BLOB,
			mysql.MYSQL_TYPE_LONG_BLOB:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, makeAppender[string, string, *array.StringBuilder](cc.String()))
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
	cast := acceptor.(**IN)

	if *cast == nil {
		builder.AppendNull()

		return nil
	}

	value := **cast

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

	*cast = nil

	return nil
}
