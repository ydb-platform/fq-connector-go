package mongodb

import (
	"errors"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"

	"github.com/ydb-platform/fq-connector-go/common"
)

var errEmptyArray = errors.New("can't determine field type for items in an empty array")
var errNull = errors.New("can't determine field type for null")

const idColumn string = "_id"

func typeMap(v bson.RawValue, logger *zap.Logger) (*Ydb.Type, error) {
	switch v.Type {
	case bson.TypeInt32:
		return common.MakePrimitiveType(Ydb.Type_INT32), nil
	case bson.TypeInt64:
		return common.MakePrimitiveType(Ydb.Type_INT64), nil
	case bson.TypeBoolean:
		return common.MakePrimitiveType(Ydb.Type_BOOL), nil
	case bson.TypeDouble:
		return common.MakePrimitiveType(Ydb.Type_DOUBLE), nil
	case bson.TypeString:
		return common.MakePrimitiveType(Ydb.Type_UTF8), nil
	case bson.TypeBinary, bson.TypeObjectID:
		return common.MakePrimitiveType(Ydb.Type_STRING), nil
	case bson.TypeDateTime:
		return common.MakePrimitiveType(Ydb.Type_INTERVAL), nil
	case bson.TypeArray:
		elements, err := v.Array().Elements()
		if err != nil {
			return nil, err
		}

		if len(elements) > 0 {
			innerType, err := typeMap(elements[0].Value(), logger)
			if err != nil {
				return nil, err
			}

			return common.MakeListType(innerType), nil
		}

		return nil, errEmptyArray

	case bson.TypeEmbeddedDocument:
		return common.MakePrimitiveType(Ydb.Type_JSON), nil
	case bson.TypeNull:
		return nil, errNull
	default:
		logger.Debug(fmt.Sprintf("typeMap: skipping unsupported type %v", v.Type.String()))
	}

	return nil, common.ErrDataTypeNotSupported
}

func bsonToYqlColumnSingleDoc(
	doc bson.Raw,
	deducedTypes map[string]*Ydb.Type,
	ambiguousFields, ambiguousArrayFields map[string]struct{},
	doSkipUnsupported bool,
	logger *zap.Logger,
) error {
	elements, err := doc.Elements()
	if err != nil {
		return fmt.Errorf("doc.Elements(): %w", err)
	}

	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return fmt.Errorf("elem.KeyErr(): %w", err)
		}

		prevType, prevTypeExists := deducedTypes[key]

		t, err := typeMap(elem.Value(), logger)
		if err != nil {
			if errors.Is(err, errNull) {
				ambiguousFields[key] = struct{}{}

				continue
			} else if errors.Is(err, errEmptyArray) {
				ambiguousArrayFields[key] = struct{}{}

				if prevTypeExists && prevType.GetListType() == nil {
					deducedTypes[key] = common.MakePrimitiveType(Ydb.Type_UTF8)

					logger.Debug(fmt.Sprintf("bsonToYqlColumnSingleDoc: keeping serialized %v. prev: %v curr: []", key, prevType.String()))
				}

				continue
			} else if errors.Is(err, common.ErrDataTypeNotSupported) {
				logger.Debug(fmt.Sprintf("bsonToYqlColumnSingleDoc: data not supported: %v", key))

				if !doSkipUnsupported {
					deducedTypes[key] = common.MakePrimitiveType(Ydb.Type_UTF8)
				}

				continue
			}

			return err
		}

		tString := t.String()
		_, prevIsArray := ambiguousArrayFields[key]

		if (prevTypeExists && prevType.String() != tString) || (prevIsArray && t.GetListType() == nil) {
			deducedTypes[key] = common.MakePrimitiveType(Ydb.Type_UTF8)

			logger.Debug(fmt.Sprintf("bsonToYqlColumnSingleDoc: keeping serialized %v. prev: %v curr: %v", key, prevType.String(), tString))

			continue
		}

		deducedTypes[key] = t

		logger.Debug(fmt.Sprintf("bsonToYqlColumnSingleDoc: column %v of type %v", key, tString))
	}

	return nil
}

func bsonToYqlColumn(docs []bson.Raw, doSkipUnsupported bool, logger *zap.Logger) ([]*Ydb.Column, error) {
	if len(docs) == 0 {
		return []*Ydb.Column{}, nil
	}

	deducedTypes := make(map[string]*Ydb.Type)
	ambiguousFields := make(map[string]struct{})
	ambiguousArrayFields := make(map[string]struct{})

	for _, doc := range docs {
		err := bsonToYqlColumnSingleDoc(
			doc,
			deducedTypes,
			ambiguousFields,
			ambiguousArrayFields,
			doSkipUnsupported,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("bsonToYqlColumn(): %w", err)
		}
	}

	for field := range ambiguousArrayFields {
		ambiguousFields[field] = struct{}{}
	}

	for field := range ambiguousFields {
		if _, ok := deducedTypes[field]; !ok {
			deducedTypes[field] = common.MakePrimitiveType(Ydb.Type_UTF8)
		}
	}

	columns := make([]*Ydb.Column, 0, len(deducedTypes))

	for columnName, deducedType := range deducedTypes {
		if columnName == idColumn {
			columns = append(columns, &Ydb.Column{Name: columnName, Type: deducedType})
		} else {
			columns = append(columns, &Ydb.Column{Name: columnName, Type: common.MakeOptionalType(deducedType)})
		}
	}

	return columns, nil
}
