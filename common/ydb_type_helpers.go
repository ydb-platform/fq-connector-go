package common

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

func MakePrimitiveType(typeId Ydb.Type_PrimitiveTypeId) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: typeId}}
}

func MakeOptionalType(ydbType *Ydb.Type) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: ydbType}}}
}

func MakeListType(ydbType *Ydb.Type) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: ydbType}}}
}

func MakeTypedValue(ydbType *Ydb.Type, value any) *Ydb.TypedValue {
	out := &Ydb.TypedValue{Type: ydbType, Value: &Ydb.Value{}}

	switch v := value.(type) {
	case int32:
		out.Value.Value = &Ydb.Value_Int32Value{Int32Value: v}
	case int64:
		out.Value.Value = &Ydb.Value_Int64Value{Int64Value: v}
	case string:
		out.Value.Value = &Ydb.Value_TextValue{TextValue: v}
	case []byte:
		out.Value.Value = &Ydb.Value_BytesValue{BytesValue: v}
	case nil:
		out.Value.Value = &Ydb.Value_NullFlagValue{}
	default:
		panic(fmt.Sprintf("unexpected type %T", value))
	}

	return out
}

func SelectWhatToYDBTypes(selectWhat *api_service_protos.TSelect_TWhat) ([]*Ydb.Type, error) {
	var ydbTypes []*Ydb.Type

	for i, item := range selectWhat.Items {
		ydbType := item.GetColumn().GetType()
		if ydbType == nil {
			return nil, fmt.Errorf("item #%d (%v) is not a column", i, item)
		}

		ydbTypes = append(ydbTypes, ydbType)
	}

	return ydbTypes, nil
}

func YdbTypeToYdbPrimitiveTypeID(ydbType *Ydb.Type) (Ydb.Type_PrimitiveTypeId, error) {
	switch t := ydbType.Type.(type) {
	case *Ydb.Type_TypeId:
		return t.TypeId, nil
	case *Ydb.Type_OptionalType:
		switch t.OptionalType.Item.Type.(type) {
		case *Ydb.Type_TypeId:
			return t.OptionalType.Item.GetTypeId(), nil
		default:
			return Ydb.Type_PRIMITIVE_TYPE_ID_UNSPECIFIED,
				fmt.Errorf("unexpected type %v: %w", t.OptionalType.Item, ErrDataTypeNotSupported)
		}
	default:
		return Ydb.Type_PRIMITIVE_TYPE_ID_UNSPECIFIED, fmt.Errorf("unexpected type %v: %w", t, ErrDataTypeNotSupported)
	}
}

func MakeYdbDateTimeType(ydbTypeID Ydb.Type_PrimitiveTypeId, format api_service_protos.EDateTimeFormat) (*Ydb.Type, error) {
	switch format {
	case api_service_protos.EDateTimeFormat_YQL_FORMAT:
		return MakePrimitiveType(ydbTypeID), nil
	case api_service_protos.EDateTimeFormat_STRING_FORMAT:
		return MakePrimitiveType(Ydb.Type_UTF8), nil
	default:
		return nil, fmt.Errorf("unexpected datetime format '%s': %w", format, ErrInvalidRequest)
	}
}
