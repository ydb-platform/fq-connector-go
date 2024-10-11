package utils

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type QueryArgument struct {
	YdbType *Ydb.Type
	Value   any
}

type QueryArgsCollection struct {
	args []*QueryArgument
}

func (q *QueryArgsCollection) Add(ydbType *Ydb.Type, arg any) *QueryArgsCollection {
	q.args = append(q.args, &QueryArgument{ydbType, arg})

	return q
}

func (q *QueryArgsCollection) Count() int {
	return len(q.args)
}

func (q *QueryArgsCollection) Args() []any {
	args := make([]any, len(q.args))
	for i, arg := range q.args {
		args[i] = arg.Value
	}
	return args
}

func (q *QueryArgsCollection) Get(i int) *QueryArgument {
	return q.args[i]
}
