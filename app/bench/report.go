package bench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

type jsonTime struct {
	time.Time
}

const layout = "2006-01-02 15:04:05"

func (t *jsonTime) MarshalJSON() ([]byte, error) {
	if t == nil {
		return nil, nil
	}

	return []byte(fmt.Sprintf(`"%s"`, t.Time.Format(layout))), nil
}

type report struct {
	TestCaseConfig *config.TBenchmarkTestCase `json:"test_case_config"`
	StartTime      jsonTime                   `json:"start_time"`
	StopTime       *jsonTime                  `json:"stop_time"`

	BytesInternalTotal uint64  `json:"bytes_internal_total"`
	BytesInternalRate  float32 `json:"bytes_internal_rate"`
	BytesArrowTotal    uint64  `json:"bytes_arrow_total"`
	BytesArrowRate     float32 `json:"bytes_arrow_rate"`
	RowsTotal          uint64  `json:"rows_total"`
	RowsRate           float32 `json:"rows_rate"`
}

func (r *report) String() string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("elapsed time: %v, ", time.Since(r.StartTime.Time)))
	builder.WriteString(fmt.Sprintf("bytes internal total = %s, ", humanize.Bytes(r.BytesInternalTotal)))
	builder.WriteString(fmt.Sprintf("bytes internal rate = %.2f MB/sec, ", r.BytesInternalRate))
	builder.WriteString(fmt.Sprintf("bytes arrow total = %s, ", humanize.Bytes(r.BytesArrowTotal)))
	builder.WriteString(fmt.Sprintf("bytes arrow rate = %.2f MB/sec, ", r.BytesArrowRate))
	builder.WriteString(fmt.Sprintf("rows total = %d, ", r.RowsTotal))
	builder.WriteString(fmt.Sprintf("rows rate = %.2f rows/sec, ", r.RowsRate))
	return builder.String()
}

func (r *report) saveToFile(dir string) error {
	fileName := fmt.Sprintf(
		"bytes_per_page_%s-prefetch_queue_capacity_%d",
		humanize.Bytes(r.TestCaseConfig.GetServerParams().Paging.BytesPerPage),
		r.TestCaseConfig.GetServerParams().Paging.PrefetchQueueCapacity,
	)
	fullPath := filepath.Join(dir, fileName)

	dump, err := json.MarshalIndent(r, "", "    ")
	if err != nil {
		return fmt.Errorf("json marshal indent: %w", err)
	}

	if err := os.WriteFile(fullPath, dump, 0644); err != nil {
		return fmt.Errorf("write file '%s': %w", fullPath, err)
	}

	return nil
}
