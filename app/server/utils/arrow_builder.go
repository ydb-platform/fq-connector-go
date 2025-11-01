package utils //nolint:revive

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/array"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/common"
)

func MakeAppender[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](conv conversion.ValuePtrConverter[IN, OUT]) func(acceptor any, builder array.Builder) error {
	return func(acceptor any, builder array.Builder) error {
		return AppendValueToArrowBuilder[IN, OUT, AB](acceptor, builder, conv)
	}
}

func AppendValueToArrowBuilder[IN common.ValueType, OUT common.ValueType, AB common.ArrowBuilder[OUT]](
	acceptor any,
	builder array.Builder,
	conv conversion.ValuePtrConverter[IN, OUT],
) error {
	cast := acceptor.(*IN)

	if cast == nil {
		builder.AppendNull()

		return nil
	}

	out, err := conv.Convert(cast)
	if err != nil {
		if errors.Is(err, common.ErrValueOutOfTypeBounds) {
			// TODO: write warning to logger
			builder.AppendNull()

			return nil
		}

		return fmt.Errorf("convert value %v: %w", *cast, err)
	}

	//nolint:forcetypeassert
	builder.(AB).Append(out)

	return nil
}

func MakeAppenderNullable[
	IN common.ValueType,
	OUT common.ValueType,
	AB common.ArrowBuilder[OUT],
](conv conversion.ValuePtrConverter[IN, OUT]) func(acceptor any, builder array.Builder) error {
	return func(acceptor any, builder array.Builder) error {
		return AppendValueToArrowBuilderNullable[IN, OUT, AB](acceptor, builder, conv)
	}
}

func AppendValueToArrowBuilderNullable[IN common.ValueType, OUT common.ValueType, AB common.ArrowBuilder[OUT]](
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

	// Without that ClickHouse native driver would return invalid values for NULLABLE(bool) columns;
	// TODO: research it.
	*cast = nil

	return nil
}
