package oracle

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/fq-connector-go/tests/suite"
	"github.com/ydb-platform/fq-connector-go/tests/suite/waiter"
)

var _ suite.DBWaiterFactory[int64, *array.Int64Builder] = (*OracleWaiterFactory)(nil)

type OracleWaiterFactory struct {
}

func (OracleWaiterFactory) NewDbWaiter(b *suite.Base[int64, *array.Int64Builder]) waiter.DbWaiter {
	return waiter.NewDefaultDBWaiter(newOracleRetrierFuncs(b))
}

func NewOracleDbWaiterFactory() *OracleWaiterFactory {
	return &OracleWaiterFactory{}
}
