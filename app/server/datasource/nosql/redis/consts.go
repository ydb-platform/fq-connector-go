package redis

//nolint:unused
const (
	TypeNone   = "none"
	TypeString = "string"
	TypeHash   = "hash"
	TypeList   = "list"
	TypeSet    = "set"
	TypeZSet   = "zset"
	TypeStream = "stream"

	KeyColumnName    = "key"
	StringColumnName = "string_values"
	HashColumnName   = "hash_values"

	scanBatchSize = 100
)
