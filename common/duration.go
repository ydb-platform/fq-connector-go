package common

import (
	"fmt"
	"time"
)

func DurationFromString(src string) (time.Duration, error) {
	out, err := time.ParseDuration(src)
	if err != nil {
		return 0, fmt.Errorf("parse duration: %v", err)
	}

	return out, nil
}

func MustDurationFromString(src string) time.Duration {
	out, err := DurationFromString(src)
	if err != nil {
		panic(err)
	}

	return out
}
