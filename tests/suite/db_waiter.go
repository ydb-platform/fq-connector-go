package suite

import (
	"github.com/ydb-platform/fq-connector-go/tests/suite/waiter"
	test_utils "github.com/ydb-platform/fq-connector-go/tests/utils"
	"golang.org/x/exp/constraints"
)

type DBWaiterFactory[T constraints.Integer, K test_utils.ArrowIDBuilder[T]] interface {
	NewDbWaiter(*Base[T, K]) waiter.DbWaiter
}
