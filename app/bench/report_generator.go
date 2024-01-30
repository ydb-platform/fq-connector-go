package bench

import (
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	api_service_protos "github.com/ydb-platform/fq-connector-go/api/service/protos"
)

// reportGenerator is responsible for collecting reading stats
type reportGenerator struct {
	startTime     time.Time
	bytesInternal atomic.Uint64 // total amount of data in internal representation (Go type system)
	bytesArrow    atomic.Uint64 // total amount of data in Arrow format
	rows          atomic.Uint64 // total number of rows read

	exitChan chan struct{}
	wg       sync.WaitGroup
	logger   *zap.Logger
}

func (agg *reportGenerator) start() {
	agg.wg.Add(1)
	go agg.progress()
}

func (agg *reportGenerator) registerResponse(response *api_service_protos.TReadSplitsResponse) {
	agg.bytesInternal.Add(response.Stats.Bytes)
	agg.bytesArrow.Add(uint64(len(response.GetArrowIpcStreaming())))
	agg.rows.Add(response.Stats.Rows)
}

const reportPeriod = 5 * time.Second

func (agg *reportGenerator) progress() {
	defer agg.wg.Done()

	for {
		select {
		case <-time.After(reportPeriod):
			agg.logger.Info("INTERMEDIATE RESULT: " + agg.makeReport().String())
		case <-agg.exitChan:
			return
		}
	}
}

const megabyte = 1 << 20

func (agg *reportGenerator) makeReport() *report {
	secondsSinceStart := float32(time.Since(agg.startTime).Seconds())

	bytesInternalRate := float32(agg.bytesInternal.Load()) / secondsSinceStart / megabyte
	bytesArrowRate := float32(agg.bytesArrow.Load()) / secondsSinceStart / megabyte
	rowsRate := float32(agg.rows.Load()) / secondsSinceStart

	r := &report{
		StartTime:          jsonTime{agg.startTime},
		BytesInternalTotal: agg.bytesInternal.Load(),
		BytesInternalRate:  bytesInternalRate,
		BytesArrowTotal:    agg.bytesArrow.Load(),
		BytesArrowRate:     bytesArrowRate,
		RowsTotal:          agg.rows.Load(),
		RowsRate:           rowsRate,
	}

	return r
}

func (agg *reportGenerator) stop() *report {
	close(agg.exitChan)
	agg.wg.Wait()

	finalReport := agg.makeReport()
	finalReport.StopTime = &jsonTime{time.Now()}

	agg.logger.Info("FINAL RESULT: " + finalReport.String())
	return finalReport
}

func newReportGenerator(logger *zap.Logger) *reportGenerator {
	agg := &reportGenerator{
		startTime: time.Now(),
		exitChan:  make(chan struct{}),
	}

	return agg
}
