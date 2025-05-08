package opensearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

type queryBuilder struct{}

func (*queryBuilder) buildInitialSearchQuery(batchSize uint64) (io.Reader, error) {
	query := map[string]any{
		"size": batchSize,
		"query": map[string]any{
			"match_all": make(map[string]any),
		},
	}

	jsonBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("marshal query: %w", err)
	}

	return bytes.NewReader(jsonBytes), nil
}
