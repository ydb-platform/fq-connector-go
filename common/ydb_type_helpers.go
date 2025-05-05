package common

import (
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

func MakePrimitiveType(typeId Ydb.Type_PrimitiveTypeId) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: typeId}}
}

func MakeOptionalType(ydbType *Ydb.Type) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: ydbType}}}
}

func MakeTaggedType(tag string, ydbType *Ydb.Type) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_TaggedType{TaggedType: &Ydb.TaggedType{Tag: tag, Type: ydbType}}}
}

func MakeListType(ydbType *Ydb.Type) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: ydbType}}}
}

func MakeStructType(ydbTypeMembers []*Ydb.StructMember) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_StructType{StructType: &Ydb.StructType{Members: ydbTypeMembers}}}
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
	case float64:
		out.Value.Value = &Ydb.Value_DoubleValue{DoubleValue: v}
	case bool:
		out.Value.Value = &Ydb.Value_BoolValue{BoolValue: v}
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

func SelectWhatToYDBColumns(selectWhat *api_service_protos.TSelect_TWhat) []*Ydb.Column {
	var ydbColumns []*Ydb.Column

	for _, item := range selectWhat.Items {
		ydbColumns = append(ydbColumns, item.GetColumn())
	}

	return ydbColumns
}

func YDBColumnsToYDBTypes(ydbColumns []*Ydb.Column) []*Ydb.Type {
	var ydbTypes []*Ydb.Type

	for _, column := range ydbColumns {
		ydbTypes = append(ydbTypes, column.Type)
	}

	return ydbTypes
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

//nolint:gocyclo
func TypesEqual(lhs, rhs *Ydb.Type) bool {
	switch lhsType := lhs.Type.(type) {
	case *Ydb.Type_TypeId:
		return lhsType.TypeId == rhs.GetTypeId()
	case *Ydb.Type_NullType:
		return rhs.GetNullType() != structpb.NullValue(0)
	case *Ydb.Type_OptionalType:
		rhsType := rhs.GetOptionalType()
		return rhsType != nil &&
			TypesEqual(rhsType.Item, lhsType.OptionalType.Item)
	case *Ydb.Type_DictType:
		rhsType := rhs.GetDictType()

		return rhsType != nil &&
			TypesEqual(rhsType.Key, lhsType.DictType.Key) &&
			TypesEqual(rhsType.Payload, lhsType.DictType.Payload)
	case *Ydb.Type_ListType:
		rhsType := rhs.GetListType()
		return rhsType != nil &&
			TypesEqual(rhsType.Item, lhsType.ListType.Item)
	case *Ydb.Type_DecimalType:
		rhsType := rhs.GetDecimalType()

		return rhsType != nil &&
			rhsType.Precision == lhsType.DecimalType.Precision &&
			rhsType.Scale == lhsType.DecimalType.Scale
	case *Ydb.Type_TupleType:
		rhsType := rhs.GetTupleType()
		return rhsType != nil && tuplesEqual(rhsType, lhsType.TupleType)
	case *Ydb.Type_StructType:
		rhsType := rhs.GetStructType()
		return rhsType != nil && structsEqual(rhsType, lhsType.StructType)
	case *Ydb.Type_VariantType:
		rhsType := rhs.GetVariantType()
		return rhsType != nil && variantsEqual(rhsType, lhsType.VariantType)
	case *Ydb.Type_TaggedType:
		rhsType := rhs.GetTaggedType()
		return rhsType.Tag == lhsType.TaggedType.Tag &&
			TypesEqual(rhsType.Type, lhsType.TaggedType.Type)
	case *Ydb.Type_VoidType:
		return rhs.GetVoidType() != structpb.NullValue(0)
	case *Ydb.Type_EmptyListType:
		return rhs.GetEmptyListType() != structpb.NullValue(0)
	case *Ydb.Type_EmptyDictType:
		return rhs.GetEmptyDictType() != structpb.NullValue(0)
	case *Ydb.Type_PgType:
		rhsType := rhs.GetPgType()
		return rhsType != nil && rhs.GetPgType().TypeName == lhsType.PgType.TypeName
	}

	panic("unreachable")
}

func tuplesEqual(lhs, rhs *Ydb.TupleType) bool {
	if len(lhs.Elements) != len(rhs.Elements) {
		return false
	}

	for i := range len(rhs.Elements) {
		if !TypesEqual(rhs.Elements[i], lhs.Elements[i]) {
			return false
		}
	}

	return true
}

func structsEqual(lhs, rhs *Ydb.StructType) bool {
	if len(rhs.Members) != len(lhs.Members) {
		return false
	}

	for i := range len(rhs.Members) {
		if rhs.Members[i].Name != lhs.Members[i].Name || !TypesEqual(rhs.Members[i].Type, lhs.Members[i].Type) {
			return false
		}
	}

	return true
}

func variantsEqual(lhs, rhs *Ydb.VariantType) bool {
	switch innerType := lhs.Type.(type) {
	case *Ydb.VariantType_TupleItems:
		return tuplesEqual(innerType.TupleItems, rhs.GetTupleItems())
	case *Ydb.VariantType_StructItems:
		return structsEqual(innerType.StructItems, rhs.GetStructItems())
	}

	panic("unreachable")
}
