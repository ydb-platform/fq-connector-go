package paging

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/array"
)

// Acceptor is a fundamental type class that is used during data extraction from the data source
type Acceptor interface {
	any | string
}

// RowTransformer is a container for values taken extracted from a single table row.
// RowTransformer also knows how to convert them into columnar reprsentation with Arrow builders.
type RowTransformer[T Acceptor] interface {
	AppendToArrowBuilders(builders []array.Builder) error
	SetAcceptors(acceptors []T)
	GetAcceptors() []T
}

type transformationRequest struct {
	ix      int
	builder array.Builder
}

type RowTransformerDefault[T Acceptor] struct {
	// The row data itself.
	acceptors []T
	// Collection of functions responsible for appending certain row items to the corresponding columns.
	appenders []func(acceptor T, builder array.Builder) error
	// Sometimes row contains more data than necessary.
	// This array specifies what particular row items to convert into the columnar format.
	wantedColumnIDs []int

	requestChan chan transformationRequest
	errChan     chan error
}

func (rt *RowTransformerDefault[T]) AppendToArrowBuilders(builders []array.Builder) error {
	if len(rt.wantedColumnIDs) != 0 {
		for i, columnID := range rt.wantedColumnIDs {
			if err := rt.appenders[i](rt.acceptors[columnID], builders[i]); err != nil {
				return fmt.Errorf(
					"append acceptor %#v of %d column to arrow builder %#v: %w",
					rt.acceptors[columnID], i, builders[i], err)
			}
		}
	} else {
		// for i, acceptor := range rt.acceptors {
		// 	if err := rt.appenders[i](acceptor, builders[i]); err != nil {
		// 		return fmt.Errorf(
		// 			"append acceptor %#v of %d column to arrow builder %#v: %w",
		// 			acceptor, i, builders[i], err)
		// 	}
		// }
		return rt.enqueue(builders)
	}

	return nil
}

func (rt *RowTransformerDefault[T]) SetAcceptors(acceptors []T) { rt.acceptors = acceptors }

func (rt *RowTransformerDefault[T]) GetAcceptors() []T { return rt.acceptors }

func (rt *RowTransformerDefault[T]) enqueue(builders []array.Builder) error {
	for i := range rt.acceptors {
		req := transformationRequest{ix: i, builder: builders[i]}

		rt.requestChan <- req
	}

	var nonEmptyErr error
	for i := 0; i < len(rt.acceptors); i++ {
		if err := <-rt.errChan; err != nil {
			nonEmptyErr = err
		}
	}

	return nonEmptyErr
}

func (rt *RowTransformerDefault[T]) worker() {
	for {
		req, ok := <-rt.requestChan
		if !ok {
			return
		}

		err := rt.appenders[req.ix](rt.acceptors[req.ix], req.builder)
		if err != nil {
			err = fmt.Errorf(
				"append acceptor %#v of %d column to arrow builder %#v: %w",
				rt.acceptors[req.ix], req.ix, req.builder, err)
		}

		rt.errChan <- err
	}
}

func NewRowTransformer[T Acceptor](
	acceptors []T,
	appenders []func(acceptor T, builder array.Builder) error,
	wantedColumnIDs []int,
) RowTransformer[T] {
	rt := &RowTransformerDefault[T]{
		acceptors:       acceptors,
		appenders:       appenders,
		wantedColumnIDs: wantedColumnIDs,
		requestChan:     make(chan transformationRequest, 1024),
		errChan:         make(chan error, 1024),
	}

	for i := 0; i < 2; i++ {
		go rt.worker()
	}

	return rt
}
