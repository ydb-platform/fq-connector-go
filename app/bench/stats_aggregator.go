package bench

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

// statsAggregator is responsible for collecting reading stats
type statsAggregator struct {
	startTime     time.Time
	bytesInternal atomic.Uint64 // total amount of data in internal representation (Go type system)
	bytesArrow    atomic.Uint64 // total amount of data in Arrow format
	rows          atomic.Uint64 // total number of rows read

	exitChan chan struct{}
	wg       sync.WaitGroup
	logger   *zap.Logger
}

func (agg *statsAggregator) start() {
	agg.wg.Add(1)
	go agg.progress()
}

func (agg *statsAggregator) registerResponse(response *api_service_protos.TReadSplitsResponse) {
	agg.bytesInternal.Add(response.Stats.Bytes)
	agg.bytesArrow.Add(uint64(len(response.GetArrowIpcStreaming())))
	agg.rows.Add(response.Stats.Rows)
}

const reportPeriod = 5 * time.Second

func (agg *statsAggregator) progress() {
	defer agg.wg.Done()

	for {
		select {
		case <-time.After(reportPeriod):
			agg.logger.Info("INTERMEDIATE RESULT: " + agg.dumpReport())
		case <-agg.exitChan:
			return
		}
	}
}

const megabyte = 1 << 20

func (agg *statsAggregator) dumpReport() string {
	secondsSinceStart := float32(time.Since(agg.startTime).Seconds())

	bytesInternalRate := float32(agg.bytesInternal.Load()) / secondsSinceStart / megabyte
	bytesArrowRate := float32(agg.bytesArrow.Load()) / secondsSinceStart / megabyte
	rowsRate := float32(agg.rows.Load()) / secondsSinceStart

	msg := fmt.Sprintf(
		"bytes internal rate = %.2f MB/sec, bytes arrow rate = %.2f MB/sec, rows rate = %.2f rows/sec",
		bytesInternalRate, bytesArrowRate, rowsRate,
	)

	return msg

}

func (agg *statsAggregator) stop() {
	close(agg.exitChan)
	agg.logger.Info("FINAL RESULT: " + agg.dumpReport())
}

func newStatsAggregator(logger *zap.Logger) *statsAggregator {
	agg := &statsAggregator{
		startTime: time.Now(),
		exitChan:  make(chan struct{}),
	}

	return agg
}
