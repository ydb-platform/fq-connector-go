package paging

import (
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	bson_primitive "go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/ydb-platform/fq-connector-go/common"
)

// Some acceptors belong to fixed-size types (integers, floats, booleans),
// while the others contain variable length types (arrays, slices, maps, strings).
// We will save CPU if we calculate the size of acceptors of fixed types only once.
type acceptorKind int8

const (
	unknownSize acceptorKind = iota
	fixedSize
	variableSize
)

type sizePattern[T Acceptor] struct {
	// Ordered numbers of acceptors of variable legnth types.
	// Their size must be estimated every time.
	varyingSizeIx []int
	// We summarize the size of fixed size acceptors here.
	// They never change between the rows.
	fixedSizeTotal uint64
}

func (sp *sizePattern[T]) estimate(acceptors []T) (uint64, error) {
	sizeTotal := sp.fixedSizeTotal

	for _, ix := range sp.varyingSizeIx {
		sizeVariable, _, err := sizeOfValueBloated(acceptors[ix])
		if err != nil {
			return 0, fmt.Errorf("size of value #%d: %w", ix, err)
		}

		sizeTotal += sizeVariable
	}

	return sizeTotal, nil
}

func newSizePattern[T Acceptor](acceptors []T) (*sizePattern[T], error) {
	sp := &sizePattern[T]{}

	for i, acceptor := range acceptors {
		size, kind, err := sizeOfValueBloated(acceptor)
		if err != nil {
			return nil, fmt.Errorf("estimate size of value #%d: %w", i, err)
		}

		switch kind {
		case fixedSize:
			sp.fixedSizeTotal += size
		case variableSize:
			sp.varyingSizeIx = append(sp.varyingSizeIx, i)
		default:
			return nil, fmt.Errorf("unknown type kind: %w", err)
		}
	}

	return sp, nil
}

// TODO: take money for empty []byte and string? at least 24 bytes
//
//nolint:gocyclo
func sizeOfValueReflection(v any) (uint64, acceptorKind, error) {
	reflected := reflect.ValueOf(v)

	// for nil values
	value := reflect.Indirect(reflected)

	if value.Kind() == reflect.Ptr {
		// unwrap double pointer
		value = reflect.Indirect(value)
	}

	if !value.IsValid() {
		return 0, variableSize, nil
	}

	// TODO: in order to support complicated and composite data types
	// one should write reflection code in spite of
	// https://github.com/DmitriyVTitov/size/blob/master/size.go
	switch t := value.Interface().(type) {
	case bool:
		return 1, fixedSize, nil
	case int8, uint8:
		return 1, fixedSize, nil
	case int16, uint16:
		return 2, fixedSize, nil
	case int32, uint32, float32:
		return 4, fixedSize, nil
	case int64, uint64, float64:
		return 8, fixedSize, nil
	case time.Time:
		// time.Time and all its derivatives consist of two 8-byte ints:
		// https://cs.opensource.google/go/go/+/refs/tags/go1.21.4:src/time/time.go;l=141-142
		// Location is ignored.
		return 16, fixedSize, nil
	case []byte:
		return uint64(len(t)), variableSize, nil
	case bson_primitive.Binary:
		return uint64(len(t.Data)), variableSize, nil
	case string:
		return uint64(len(t)), variableSize, nil
	case pgtype.Bool:
		return 1, fixedSize, nil
	case pgtype.Int2:
		return 2, fixedSize, nil
	case pgtype.Int4:
		return 4, fixedSize, nil
	case pgtype.Int8:
		return 8, fixedSize, nil
	case pgtype.Float4:
		return 4, fixedSize, nil
	case pgtype.Float8:
		return 8, fixedSize, nil
	case pgtype.Text:
		return uint64(len(t.String)), variableSize, nil
	case pgtype.Date:
		return 16, fixedSize, nil
	case pgtype.Timestamp:
		return 16, fixedSize, nil
	// https://www.mongodb.com/docs/manual/reference/bson-types/#objectid
	case bson_primitive.ObjectID:
		return 12, fixedSize, nil
	default:
		return 0, 0, fmt.Errorf("value %v of unexpected data type %T: %w", t, t, common.ErrDataTypeNotSupported)
	}
}

// TODO: take money for empty []byte and string? at least 24 bytes
//
//nolint:funlen,gocyclo
func sizeOfValueBloated(v any) (uint64, acceptorKind, error) {
	switch t := v.(type) {
	case bool, *bool, **bool:
		return 1, fixedSize, nil
	case int8, *int8, **int8,
		uint8, *uint8, **uint8:
		return 1, fixedSize, nil
	case int16, *int16, **int16,
		uint16, *uint16, **uint16:
		return 2, fixedSize, nil
	case int32, *int32, **int32,
		uint32, *uint32, **uint32,
		float32, *float32, **float32:
		return 4, fixedSize, nil
	case int64, *int64, **int64,
		uint64, *uint64, **uint64,
		float64, *float64, **float64:
		return 8, fixedSize, nil
	case time.Time, *time.Time, **time.Time:
		// time.Time and all its derivatives consist of two 8-byte ints:
		// https://cs.opensource.google/go/go/+/refs/tags/go1.21.4:src/time/time.go;l=141-142
		// Location is ignored.
		return 16, fixedSize, nil
	case []byte:
		return uint64(len(t)), variableSize, nil
	case *[]byte:
		if t == nil {
			return 0, variableSize, nil
		}

		return uint64(len(*t)), variableSize, nil
	case **[]byte:
		if t == nil || *t == nil {
			return 0, variableSize, nil
		}

		return uint64(len(**t)), variableSize, nil
	case *bson_primitive.Binary:
		if t == nil {
			return 0, variableSize, nil
		}

		return uint64(len((*t).Data)), variableSize, nil
	case **bson_primitive.Binary:
		if t == nil || *t == nil {
			return 0, variableSize, nil
		}

		return uint64(len((**t).Data)), variableSize, nil
	case string:
		return uint64(len(t)), variableSize, nil
	case *string:
		if t == nil {
			return 0, variableSize, nil
		}

		return uint64(len(*t)), variableSize, nil
	case **string:
		if t == nil || *t == nil {
			return 0, variableSize, nil
		}

		return uint64(len(**t)), variableSize, nil
	case *pgtype.Bool:
		return 1, fixedSize, nil
	case *pgtype.Int2:
		return 2, fixedSize, nil
	case *pgtype.Int4:
		return 4, fixedSize, nil
	case *pgtype.Int8:
		return 8, fixedSize, nil
	case *pgtype.Float4:
		return 4, fixedSize, nil
	case *pgtype.Float8:
		return 8, fixedSize, nil
	case *pgtype.Text:
		return uint64(len(t.String)), variableSize, nil
	case *pgtype.Date:
		return 16, fixedSize, nil
	case *pgtype.Timestamp:
		return 16, fixedSize, nil
	case **uuid.UUID:
		return 16, fixedSize, nil
	// https://www.mongodb.com/docs/manual/reference/bson-types/#objectid
	case bson_primitive.ObjectID, *bson_primitive.ObjectID, **bson_primitive.ObjectID:
		return 12, fixedSize, nil
	case map[string]string:
		var size uint64
		for k, v := range t {
			size += uint64(len(k) + len(v))
		}

		return size, variableSize, nil
	case *map[string]string:
		if t == nil {
			return 0, variableSize, nil
		}

		var size uint64

		for k, v := range *t {
			size += uint64(len(k) + len(v))
		}

		return size, variableSize, nil
	case **map[string]string:
		if t == nil || *t == nil {
			return 0, variableSize, nil
		}

		var size uint64

		for k, v := range **t {
			size += uint64(len(k) + len(v))
		}

		return size, variableSize, nil
	case *map[string]any:
		if t == nil {
			return 0, variableSize, nil
		}

		var size uint64

		for k, v := range *t {
			vsize, _, err := sizeOfValueBloated(v)
			if err != nil {
				return 0, 0, fmt.Errorf("value %v of unexpected data type %T: %w", t, t, common.ErrDataTypeNotSupported)
			}

			size += uint64(len(k)) + vsize
		}

		return size, variableSize, nil
	case **map[string]any:
		if t == nil || *t == nil {
			return 0, variableSize, nil
		}

		var size uint64

		for k, v := range **t {
			vsize, _, err := sizeOfValueBloated(v)
			if err != nil {
				return 0, 0, fmt.Errorf("value %v of unexpected data type %T: %w", t, t, common.ErrDataTypeNotSupported)
			}

			size += uint64(len(k)) + vsize
		}

		return size, variableSize, nil
	default:
		return 0, 0, fmt.Errorf("value %v of unexpected data type %T: %w", t, t, common.ErrDataTypeNotSupported)
	}
}
