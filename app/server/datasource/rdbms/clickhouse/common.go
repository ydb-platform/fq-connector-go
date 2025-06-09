package clickhouse

func rewriteQueryArgs(src []any) []any {
	dst := make([]any, len(src))

	for i, arg := range src {
		switch v := arg.(type) {
		case []byte:
			// It's important to convert byte slice into a string
			// in order to distinguish `String` from `Array[Uint8]`.
			// TODO: in case if pushdown for the arrays is necessary, add more complicated logic here.
			dst[i] = string(v)
		default:
			dst[i] = arg
		}
	}

	return dst
}
