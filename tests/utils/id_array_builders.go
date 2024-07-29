package utils

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

func NewInt32IDArrayBuilder(pool memory.Allocator) *array.Int32Builder {
	return array.NewInt32Builder(pool)
}

func NewInt64IDArrayBuilder(pool memory.Allocator) *array.Int64Builder {
	return array.NewInt64Builder(pool)
}
