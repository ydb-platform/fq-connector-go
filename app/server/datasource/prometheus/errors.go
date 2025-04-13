package prometheus

import "errors"

var (
	ErrEmptyTimeSeries = errors.New("empty time series")
)
