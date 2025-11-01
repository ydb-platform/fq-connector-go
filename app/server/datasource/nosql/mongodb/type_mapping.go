package mongodb

import (
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/app/config"
	"github.com/ydb-platform/fq-connector-go/common"
)

type readingMode = api_common.TMongoDbDataSourceOptions_EReadingMode

var errEmptyArray = errors.New("can't determine field type for items in an empty array")
var errNull = errors.New("can't determine field type for null")

const idColumn string = "_id"
const objectIdTag string = "ObjectId"

var objectIdTaggedType *Ydb.Type = common.MakeTaggedType(objectIdTag, common.MakePrimitiveType(Ydb.Type_STRING))

func typeMapObjectId(objectIdType config.TMongoDbConfig_EObjectIdYqlType) (*Ydb.Type, error) {
	asTaggedString := config.TMongoDbConfig_OBJECT_ID_AS_TAGGED_STRING
	asString := config.TMongoDbConfig_OBJECT_ID_AS_STRING

	switch objectIdType {
	case asTaggedString:
		return common.MakeTaggedType(objectIdTag, common.MakePrimitiveType(Ydb.Type_STRING)), nil
	case asString:
		return common.MakePrimitiveType(Ydb.Type_STRING), nil
	default:
		return nil, fmt.Errorf("unsupported ObjectId YQL Type representation: %s", objectIdType.String())
	}
}

func typeMap(logger *zap.Logger, v bson.RawValue, objectIdType *Ydb.Type) (*Ydb.Type, error) {
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
	case bson.TypeBinary:
		return common.MakePrimitiveType(Ydb.Type_STRING), nil
	case bson.TypeObjectID:
		return objectIdType, nil
	case bson.TypeNull:
		return nil, errNull
	default:
		logger.Debug(fmt.Sprintf("typeMap: skipping unsupported type %v", v.Type.String()))
	}

	return nil, common.ErrDataTypeNotSupported
}

func bsonToYqlColumn(
	logger *zap.Logger,
	key string,
	elem bson.RawValue,
	deducedTypes map[string]*Ydb.Type,
	ambiguousFields, ambiguousArrayFields map[string]struct{},
	omitUnsupported bool,
	objectIdType *Ydb.Type,
) error {
	prevType, prevTypeExists := deducedTypes[key]

	t, err := typeMap(logger, elem, objectIdType)
	if err != nil {
		if errors.Is(err, errNull) {
			ambiguousFields[key] = struct{}{}

			return nil
		} else if errors.Is(err, errEmptyArray) {
			ambiguousArrayFields[key] = struct{}{}

			if prevTypeExists && prevType.GetListType() == nil {
				deducedTypes[key] = common.MakePrimitiveType(Ydb.Type_UTF8)

				logger.Debug(fmt.Sprintf("bsonToYqlColumn: keeping serialized %v. prev: %v curr: []", key, prevType.String()))
			}

			return nil
		} else if errors.Is(err, common.ErrDataTypeNotSupported) {
			logger.Debug(fmt.Sprintf("bsonToYqlColumn: data not supported: %v", key))

			if !omitUnsupported {
				deducedTypes[key] = common.MakePrimitiveType(Ydb.Type_UTF8)
			}

			return nil
		}

		return err
	}

	tString := t.String()
	_, prevIsArray := ambiguousArrayFields[key]

	// Leaving fields that have inconsistent types serialized
	// Extra check for arrays because we might have encountered an empty one:
	// we know it is an array, but prevType is not determined yet
	if (prevTypeExists && !common.TypesEqual(prevType, t)) || (prevIsArray && t.GetListType() == nil) {
		deducedTypes[key] = common.MakePrimitiveType(Ydb.Type_UTF8)

		logger.Debug(fmt.Sprintf("bsonToYqlColumn: keeping serialized %v. prev: %v curr: %v", key, prevType.String(), tString))

		return nil
	}

	deducedTypes[key] = t

	logger.Debug(fmt.Sprintf("bsonToYqlColumn: column %v of type %v", key, tString))

	return nil
}

func bsonToYql(logger *zap.Logger, docs []bson.Raw, omitUnsupported, typeMapIdOnly bool, objectIdType *Ydb.Type) ([]*Ydb.Column, error) {
	if len(docs) == 0 {
		return []*Ydb.Column{}, nil
	}

	deducedTypes := make(map[string]*Ydb.Type)
	ambiguousFields := make(map[string]struct{})
	ambiguousArrayFields := make(map[string]struct{})

	for _, doc := range docs {
		if typeMapIdOnly {
			elem := doc.Lookup(idColumn)

			err := bsonToYqlColumn(
				logger,
				idColumn,
				elem,
				deducedTypes,
				ambiguousFields,
				ambiguousArrayFields,
				omitUnsupported,
				objectIdType,
			)
			if err != nil {
				return nil, fmt.Errorf("bsonToYqlColumn: %w", err)
			}

			continue
		}

		elements, err := doc.Elements()
		if err != nil {
			return nil, fmt.Errorf("document elements: %w", err)
		}

		for _, elem := range elements {
			err := bsonToYqlColumn(
				logger,
				elem.Key(),
				elem.Value(),
				deducedTypes,
				ambiguousFields,
				ambiguousArrayFields,
				omitUnsupported,
				objectIdType,
			)
			if err != nil {
				return nil, fmt.Errorf("bsonToYqlColumn: %w", err)
			}
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
		columns = append(columns, &Ydb.Column{Name: columnName, Type: common.MakeOptionalType(deducedType)})
	}

	return columns, nil
}

func getSerializedDocumentSchema(documentName string, idType, documentType *Ydb.Type) *api_service_protos.TSchema {
	return &api_service_protos.TSchema{Columns: []*Ydb.Column{
		{Name: idColumn, Type: idType},
		{Name: documentName, Type: documentType},
	}}
}

func getDocumentType(readingMode readingMode) *Ydb.Type {
	switch readingMode {
	case api_common.TMongoDbDataSourceOptions_JSON:
		return common.MakePrimitiveType(Ydb.Type_JSON)
	case api_common.TMongoDbDataSourceOptions_YSON:
		return common.MakePrimitiveType(Ydb.Type_YSON)
	default:
		return nil
	}
}
