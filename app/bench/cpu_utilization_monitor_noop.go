//go:build !(cgo && (linux || darwin))

package bench

type cpuUtilizationMonitorNoop struct {
}

func (mon cpuUtilizationMonitorNoop) getPercentage() float64 {
	return 0
}

func NewCPUUtilizationMonitor() cpuUtilizationMonitor {
	return cpuUtilizationMonitorNoop{}
}
