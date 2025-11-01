package common //nolint:revive

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

type ValueType interface {
	bool |
		int8 | int16 | int32 | int64 |
		uint8 | uint16 | uint32 | uint64 |
		float32 | float64 |
		string | []byte |
		time.Time
}

type ArrowArrayType[VT ValueType] interface {
	*array.Boolean |
		*array.Int8 | *array.Int16 | *array.Int32 | *array.Int64 |
		*array.Uint8 | *array.Uint16 | *array.Uint32 | *array.Uint64 |
		*array.Float32 | *array.Float64 |
		*array.String | *array.Binary | *array.FixedSizeBinary

	Len() int
	Value(int) VT
	IsNull(int) bool
}

type ArrowBuilder[VT ValueType] interface {
	AppendNull()
	Append(value VT)
}

func SelectWhatToArrowSchema(selectWhat *api_service_protos.TSelect_TWhat) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(selectWhat.Items))

	for i, item := range selectWhat.Items {
		column := item.GetColumn()
		if column == nil {
			return nil, fmt.Errorf("item #%d (%v) is not a column", i, item)
		}

		field, err := ydbTypeToArrowField(column.GetType(), column)
		if err != nil {
			return nil, err
		}

		fields = append(fields, field)
	}

	schema := arrow.NewSchema(fields, nil)

	return schema, nil
}

func YdbTypesToArrowBuilders(ydbTypes []*Ydb.Type, arrowAllocator memory.Allocator) ([]array.Builder, error) {
	var (
		builders []array.Builder
		builder  array.Builder
		err      error
	)

	for _, ydbType := range ydbTypes {
		builder, err = ydbTypeToArrowBuilder(ydbType, arrowAllocator)
		if err != nil {
			return nil, err
		}

		builders = append(builders, builder)
	}

	return builders, nil
}

func ydbTypeToArrowBuilder(ydbType *Ydb.Type, arrowAllocator memory.Allocator) (array.Builder, error) {
	var (
		builder array.Builder
		err     error
	)

	switch t := ydbType.Type.(type) {
	case *Ydb.Type_TypeId:
		builder, err = ydbTypeIdToArrowBuilder(t.TypeId, arrowAllocator)
		if err != nil {
			return nil, fmt.Errorf("primitive YDB type to Arrow builder: %w", err)
		}
	case *Ydb.Type_OptionalType:
		builder, err = ydbTypeToArrowBuilder(t.OptionalType.Item, arrowAllocator)
		if err != nil {
			return nil, fmt.Errorf("optional YDB type to Arrow builder: %w", err)
		}
	case *Ydb.Type_TaggedType:
		builder, err = ydbTypeToArrowBuilder(t.TaggedType.Type, arrowAllocator)
		if err != nil {
			return nil, fmt.Errorf("tagged YDB type to Arrow builder: %w", err)
		}
	case *Ydb.Type_StructType:
		fields := make([]arrow.Field, 0, len(t.StructType.Members))

		for _, member := range t.StructType.Members {
			field, err := ydbTypeToArrowField(member.Type, &Ydb.Column{Name: member.Name})
			if err != nil {
				return nil, fmt.Errorf("map YDB type to Arrow field for struct member %s: %w", member.Name, err)
			}

			field.Nullable = true
			fields = append(fields, field)
		}

		structType := arrow.StructOf(fields...)

		builder = array.NewStructBuilder(arrowAllocator, structType)
	case *Ydb.Type_DecimalType:
		builder = array.NewFixedSizeBinaryBuilder(arrowAllocator, &arrow.FixedSizeBinaryType{ByteWidth: 16})
	default:
		err := fmt.Errorf(
			"only primitive, optional, tagged, struct and decimal types are supported, got '%T' instead: %w",
			t, ErrDataTypeNotSupported,
		)

		return nil, err
	}

	return builder, nil
}

//nolint:gocyclo,revive
func ydbTypeIdToArrowBuilder(typeID Ydb.Type_PrimitiveTypeId, arrowAllocator memory.Allocator) (array.Builder, error) {
	var builder array.Builder

	switch typeID {
	case Ydb.Type_BOOL:
		// NOTE: for some reason YDB bool type is mapped to Arrow uint8
		// https://st.yandex-team.ru/YQL-15332
		builder = array.NewUint8Builder(arrowAllocator)
	case Ydb.Type_INT8:
		builder = array.NewInt8Builder(arrowAllocator)
	case Ydb.Type_UINT8:
		builder = array.NewUint8Builder(arrowAllocator)
	case Ydb.Type_INT16:
		builder = array.NewInt16Builder(arrowAllocator)
	case Ydb.Type_UINT16:
		builder = array.NewUint16Builder(arrowAllocator)
	case Ydb.Type_INT32:
		builder = array.NewInt32Builder(arrowAllocator)
	case Ydb.Type_UINT32:
		builder = array.NewUint32Builder(arrowAllocator)
	case Ydb.Type_INT64:
		builder = array.NewInt64Builder(arrowAllocator)
	case Ydb.Type_UINT64:
		builder = array.NewUint64Builder(arrowAllocator)
	case Ydb.Type_FLOAT:
		builder = array.NewFloat32Builder(arrowAllocator)
	case Ydb.Type_DOUBLE:
		builder = array.NewFloat64Builder(arrowAllocator)
	case Ydb.Type_STRING, Ydb.Type_YSON:
		builder = array.NewBinaryBuilder(arrowAllocator, arrow.BinaryTypes.Binary)
		// TODO: find more reasonable constant, maybe make dependency on paging settings
		builder.(*array.BinaryBuilder).ReserveData(1 << 20)
	case Ydb.Type_UTF8, Ydb.Type_JSON:
		// TODO: what about LargeString?
		// https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type12LARGE_STRINGE
		builder = array.NewStringBuilder(arrowAllocator)
		// TODO: find more reasonable constant, maybe make dependency on paging settings
		builder.(*array.StringBuilder).ReserveData(1 << 20)
	case Ydb.Type_DATE:
		builder = array.NewUint16Builder(arrowAllocator)
	case Ydb.Type_DATETIME:
		builder = array.NewUint32Builder(arrowAllocator)
	case Ydb.Type_TIMESTAMP:
		builder = array.NewUint64Builder(arrowAllocator)
	case Ydb.Type_JSON_DOCUMENT:
		builder = array.NewBinaryBuilder(arrowAllocator, arrow.BinaryTypes.Binary)
	default:
		return nil, fmt.Errorf("register type '%v': %w", typeID, ErrDataTypeNotSupported)
	}

	// TODO: find more reasonable constant, maybe make dependency on paging settings
	builder.Reserve(1 << 15)

	return builder, nil
}

//nolint:gocyclo
func ydbTypeToArrowField(ydbType *Ydb.Type, column *Ydb.Column) (arrow.Field, error) {
	// Reference table: https://github.com/ydb-platform/fq-connector-go/blob/main/docs/type_mapping_table.md
	var (
		field arrow.Field
		err   error
	)

	switch t := ydbType.Type.(type) {
	case *Ydb.Type_TypeId:
		field, err = ydbTypeIdToArrowField(t.TypeId, column)
		if err != nil {
			return arrow.Field{}, fmt.Errorf("primitive YDB type to arrow field: %w", err)
		}
	case *Ydb.Type_OptionalType:
		field, err = ydbTypeToArrowField(t.OptionalType.Item, column)
		if err != nil {
			return arrow.Field{}, fmt.Errorf("optional YDB type to arrow field: %w", err)
		}
	case *Ydb.Type_TaggedType:
		field, err = ydbTypeToArrowField(t.TaggedType.Type, column)
		if err != nil {
			return arrow.Field{}, fmt.Errorf("tagged YDB type to arrow field: %w", err)
		}
	case *Ydb.Type_StructType:
		fields := make([]arrow.Field, 0, len(t.StructType.Members))

		for _, member := range t.StructType.Members {
			innerfield, err := ydbTypeToArrowField(member.Type, &Ydb.Column{Name: member.Name})
			if err != nil {
				return arrow.Field{}, fmt.Errorf("map YDB type to Arrow field for struct member %s: %w", member.Name, err)
			}

			innerfield.Nullable = true
			fields = append(fields, innerfield)
		}

		field = arrow.Field{
			Name:     column.Name,
			Type:     arrow.StructOf(fields...),
			Nullable: true,
		}
	case *Ydb.Type_DecimalType:
		field = arrow.Field{
			Name: column.Name,
			Type: &arrow.FixedSizeBinaryType{ByteWidth: 16},
		}
	default:
		err := fmt.Errorf(
			"only primitive, optional, tagged, struct and decimal types are supported, got '%T' instead: %w",
			t, ErrDataTypeNotSupported,
		)

		return arrow.Field{}, err
	}

	return field, nil
}

//nolint:gocyclo,revive
func ydbTypeIdToArrowField(typeID Ydb.Type_PrimitiveTypeId, column *Ydb.Column) (arrow.Field, error) {
	var field arrow.Field

	switch typeID {
	case Ydb.Type_BOOL:
		// NOTE: for some reason YDB bool type is mapped to Arrow uint8
		// https://st.yandex-team.ru/YQL-15332
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint8}
	case Ydb.Type_INT8:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Int8}
	case Ydb.Type_UINT8:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint8}
	case Ydb.Type_INT16:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Int16}
	case Ydb.Type_UINT16:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint16}
	case Ydb.Type_INT32:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Int32}
	case Ydb.Type_UINT32:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint32}
	case Ydb.Type_INT64:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Int64}
	case Ydb.Type_UINT64:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint64}
	case Ydb.Type_FLOAT:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Float32}
	case Ydb.Type_DOUBLE:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Float64}
	case Ydb.Type_STRING, Ydb.Type_YSON:
		field = arrow.Field{Name: column.Name, Type: arrow.BinaryTypes.Binary}
	case Ydb.Type_UTF8, Ydb.Type_JSON:
		// TODO: what about LargeString?
		// https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type12LARGE_STRINGE
		field = arrow.Field{Name: column.Name, Type: arrow.BinaryTypes.String}
	case Ydb.Type_DATE:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint16}
	case Ydb.Type_DATETIME:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint32}
	case Ydb.Type_TIMESTAMP:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint64}
	case Ydb.Type_JSON_DOCUMENT:
		field = arrow.Field{Name: column.Name, Type: arrow.BinaryTypes.Binary}
	default:
		return arrow.Field{}, fmt.Errorf("register type '%v': %w", typeID, ErrDataTypeNotSupported)
	}

	return field, nil
}
