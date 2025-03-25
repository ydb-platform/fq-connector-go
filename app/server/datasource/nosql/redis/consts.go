package redis

const (
	redisTypeNone   = "none"
	redisTypeString = "string"
	redisTypeHash   = "hash"
	redisTypeList   = "list"
	redisTypeSet    = "set"
	redisTypeZSet   = "zset"
	redisTypeStream = "stream"

	KeyColumnName    = "key_"
	StringColumnName = "string_values"
	HashColumnName   = "hash_values"
)
