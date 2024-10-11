package utils

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type QueryArgument struct {
	YdbType *Ydb.Type
	Value   any
}

type QueryArgs struct {
	args []*QueryArgument
}

func (q *QueryArgs) AddTyped(ydbType *Ydb.Type, arg any) *QueryArgs {
	q.args = append(q.args, &QueryArgument{ydbType, arg})

	return q
}

func (q *QueryArgs) AddUntyped(arg any) *QueryArgs { return q.AddTyped(nil, arg) }

func (q *QueryArgs) Count() int { return len(q.args) }

func (q *QueryArgs) Values() []any {
	args := make([]any, len(q.args))
	for i, arg := range q.args {
		args[i] = arg.Value
	}
	return args
}

func (q *QueryArgs) Get(i int) *QueryArgument {
	return q.args[i]
}
