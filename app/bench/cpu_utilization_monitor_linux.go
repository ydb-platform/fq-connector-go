//go:build cgo && linux

package bench

// #include <time.h>
import "C"
import "time"

type cpuUtilizationMonitorLinux struct {
	startTime  time.Time
	startTicks C.long
}

func (mon *cpuUtilizationMonitorLinux) getPercentage() float64 {
	clockSeconds := float64(C.clock()-mon.startTicks) / float64(C.CLOCKS_PER_SEC)
	realSeconds := time.Since(mon.startTime).Seconds()

	return clockSeconds / realSeconds * 100
}

func NewCPUUtilizationMonitor() cpuUtilizationMonitor {
	return &cpuUtilizationMonitorLinux{
		startTime:  time.Now(),
		startTicks: C.clock(),
	}
}
