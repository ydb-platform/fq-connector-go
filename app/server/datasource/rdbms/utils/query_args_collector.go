package utils

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type QueryArg struct {
	YdbType *Ydb.Type
	Value   any
}

type QueryArgs struct {
	args []*QueryArg
}

func (q *QueryArgs) AddTyped(ydbType *Ydb.Type, arg any) *QueryArgs {
	q.args = append(q.args, &QueryArg{ydbType, arg})

	return q
}

func (q *QueryArgs) AddUntyped(arg any) *QueryArgs { return q.AddTyped(nil, arg) }

func (q *QueryArgs) Count() int {
	if q == nil {
		return 0
	}

	return len(q.args)
}

func (q *QueryArgs) Values() []any {
	if q == nil {
		return nil
	}

	args := make([]any, len(q.args))
	for i, arg := range q.args {
		args[i] = arg.Value
	}

	return args
}

func (q *QueryArgs) Get(i int) *QueryArg { return q.args[i] }

func (q *QueryArgs) GetAll() []*QueryArg {
	if q == nil {
		return nil
	}

	return q.args
}
