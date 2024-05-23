package mysql

import "golang.org/x/exp/constraints"

type number interface {
	constraints.Integer | constraints.Float
}

type stringLike interface {
	[]byte | string
}
