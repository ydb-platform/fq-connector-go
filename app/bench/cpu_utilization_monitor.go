package bench

// cpuUtilizationMonitor computes CPU utilization for a current process
// in a platform-dependent way.
type cpuUtilizationMonitor interface {
	// getPercentage returns utilization value in percents (like in tools similar to `top`)
	getPercentage() float64
}
