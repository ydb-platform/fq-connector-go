package paging

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/array"
)

type RowTransformerDefault[T Acceptor] struct {
	// The row data itself.
	acceptors []T
	// Collection of functions responsible for appending certain row items to the corresponding columns.
	appenders []func(acceptor T, builder array.Builder) error
	// Sometimes row contains more data than necessary.
	// This array specifies what particular row items to convert into the columnar format.
	wantedColumnIDs []int
}

func (rt *RowTransformerDefault[T]) AppendToArrowBuilders(builders []array.Builder) error {
	if len(rt.wantedColumnIDs) != 0 {
		for i, columnID := range rt.wantedColumnIDs {
			if err := rt.appenders[i](rt.acceptors[columnID], builders[i]); err != nil {
				return fmt.Errorf(
					"append acceptor of type %T (column #%d) to arrow builder of type %T: %w",
					rt.acceptors[columnID], i, builders[i], err)
			}
		}
	} else {
		for i, acceptor := range rt.acceptors {
			if err := rt.appenders[i](acceptor, builders[i]); err != nil {
				return fmt.Errorf(
					"append acceptor of type %T (column #%d) to arrow builder of type %T: %w",
					acceptor, i, builders[i], err)
			}
		}
	}

	return nil
}

func (rt *RowTransformerDefault[T]) SetAcceptors(acceptors []T) { rt.acceptors = acceptors }

func (rt *RowTransformerDefault[T]) GetAcceptors() []T { return rt.acceptors }

func NewRowTransformer[T Acceptor](
	acceptors []T,
	appenders []func(acceptor T, builder array.Builder) error,
	wantedColumnIDs []int,
) RowTransformer[T] {
	return &RowTransformerDefault[T]{
		acceptors:       acceptors,
		appenders:       appenders,
		wantedColumnIDs: wantedColumnIDs,
	}
}
