package common

import (
	"fmt"
	"time"
)

var (
	// According to https://ydb.tech/en/docs/yql/reference/types/primitive#datetime
	minYDBTime = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	maxYDBTime = time.Date(2106, time.January, 1, 0, 0, 0, 0, time.UTC)
)

func TimeToYDBDate(t *time.Time) (uint16, error) {
	if t.Before(minYDBTime) || t.After(maxYDBTime) {
		return 0, fmt.Errorf("convert '%v' to YDB Date: %w", t, ErrValueOutOfTypeBounds)
	}

	days := t.Sub(minYDBTime).Hours() / 24

	return uint16(days), nil
}

func TimeToYDBDatetime(t *time.Time) (uint32, error) {
	if t.Before(minYDBTime) || t.After(maxYDBTime) {
		return 0, fmt.Errorf("convert '%v' to YDB Date: %w", t, ErrValueOutOfTypeBounds)
	}

	seconds := t.Unix()

	return uint32(seconds), nil
}

func TimeToYDBTimestamp(t *time.Time) (uint64, error) {
	if t.Before(minYDBTime) || t.After(maxYDBTime) {
		return 0, fmt.Errorf("convert '%v' to YDB Date: %w", t, ErrValueOutOfTypeBounds)
	}

	seconds := t.UnixMicro()

	return uint64(seconds), nil
}

type ydbTime interface {
	uint16 | uint32 | uint64
}

func MustTimeToYDBType[OUT ydbTime](f func(t *time.Time) (OUT, error), t time.Time) OUT {
	res, err := f(&t)
	if err != nil {
		panic(err)
	}

	return res
}
