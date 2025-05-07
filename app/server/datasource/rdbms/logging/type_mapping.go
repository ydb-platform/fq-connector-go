package logging

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/fq-connector-go/app/server/conversion"
	"github.com/ydb-platform/fq-connector-go/app/server/paging"
	"github.com/ydb-platform/fq-connector-go/app/server/utils"
)

func makeRowTransformer(ydbColumns []*Ydb.Column, cc conversion.Collection) (paging.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(ydbColumns))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(ydbColumns))

	for _, ydbColumn := range ydbColumns {
		switch ydbColumn.Name {
		case levelColumnName:
			acceptors = append(acceptors, new(*int32))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				src, ok := acceptor.(**int32)
				if !ok {
					return fmt.Errorf("unexpected acceptor type %T", src)
				}

				typedBuilder, ok := builder.(*array.StringBuilder)
				if !ok {
					return fmt.Errorf("unexpected builder type %T", builder)
				}

				if *src == nil {
					typedBuilder.AppendNull()
					return nil
				}

				switch **src {
				case 1:
					typedBuilder.Append(levelTraceValue)
				case 2:
					typedBuilder.Append(levelDebugValue)
				case 3:
					typedBuilder.Append(levelInfoValue)
				case 4:
					typedBuilder.Append(levelWarnValue)
				case 5:
					typedBuilder.Append(levelErrorValue)
				case 6:
					typedBuilder.Append(levelFatalValue)
				default:
					return fmt.Errorf("unexpected level value %d", *src)
				}

				return nil
			})
		case messageColumnName:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		case timestampColumnName:
			acceptors = append(acceptors, new(*time.Time))
			appenders = append(appenders, utils.MakeAppenderNullable[time.Time, uint64, *array.Uint64Builder](cc.Timestamp()))
		case metaColumnName:
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, utils.MakeAppenderNullable[string, string, *array.StringBuilder](cc.String()))
		}
	}

	return paging.NewRowTransformer[any](acceptors, appenders, nil), nil
}
