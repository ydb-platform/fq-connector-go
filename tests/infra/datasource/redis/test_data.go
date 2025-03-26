package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// PopulateTestData populates Redis with test data for the given case.
// The caseName should be one of "stringOnly", "hashOnly", "mixed", or "empty".
func PopulateTestData(ctx context.Context, client *redis.Client, caseName string) error {
	// Flush all keys before inserting new test data.
	if err := client.FlushAll(ctx).Err(); err != nil {
		return fmt.Errorf("flushall error: %w", err)
	}

	switch caseName {
	case "stringOnly":
		// Insert keys for stringOnly case.
		if err := client.Set(ctx, "stringOnly:stringKey1", "value1", 0).Err(); err != nil {
			return fmt.Errorf("set stringOnly:stringKey1: %w", err)
		}

		if err := client.Set(ctx, "stringOnly:stringKey2", "value2", 0).Err(); err != nil {
			return fmt.Errorf("set stringOnly:stringKey2: %w", err)
		}

	case "hashOnly":
		// Insert hash keys for hashOnly case.
		if err := client.HSet(ctx, "hashOnly:hashKey1", map[string]any{
			"field1": "hashValue1",
			"field2": "hashValue2",
		}).Err(); err != nil {
			return fmt.Errorf("HSET hashOnly:hashKey1: %w", err)
		}

		if err := client.HSet(ctx, "hashOnly:hashKey2", map[string]any{
			"field1": "hashValue3",
			"field2": "hashValue4",
			"field3": "hashValue5",
		}).Err(); err != nil {
			return fmt.Errorf("HSET hashOnly:hashKey2: %w", err)
		}

	case "mixed":
		// Insert one string key and one hash key for mixed case.
		if err := client.Set(ctx, "mixed:stringKey1", "mixedString", 0).Err(); err != nil {
			return fmt.Errorf("set mixed:stringKey1: %w", err)
		}

		if err := client.HSet(ctx, "mixed:hashKey2", map[string]any{
			"hashField1": "mixedHash1",
			"hashField2": "mixedHash2",
		}).Err(); err != nil {
			return fmt.Errorf("HSET mixed:hashKey2: %w", err)
		}

	case "empty":
		// For empty case, no keys are inserted.

	default:
		return fmt.Errorf("unknown test case: %s", caseName)
	}

	return nil
}
