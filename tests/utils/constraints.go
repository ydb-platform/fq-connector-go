package utils //nolint:revive

type TableIDTypes interface {
	int32 | int64 | []byte | string
}
