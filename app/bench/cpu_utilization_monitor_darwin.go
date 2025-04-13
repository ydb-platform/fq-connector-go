//go:build cgo && darwin

package bench

// #include <sys/time.h>
// #include <sys/resource.h>
import "C"
import "time"

// cpuUtilizationMonitorDarwin реализует интерфейс cpuUtilizationMonitor для macOS.
type cpuUtilizationMonitorDarwin struct {
	startTime    time.Time
	startCPUTime float64
}

// getCPUTime получает общее процессорное время (user + sys) для текущего процесса.
func getCPUTime() float64 {
	var usage C.struct_rusage
	if ret := C.getrusage(C.RUSAGE_SELF, &usage); ret != 0 {
		// В случае ошибки возвращаем 0, хотя можно обработать ошибку более детально.
		return 0.0
	}
	userTime := float64(usage.ru_utime.tv_sec) + float64(usage.ru_utime.tv_usec)/1e6
	sysTime := float64(usage.ru_stime.tv_sec) + float64(usage.ru_stime.tv_usec)/1e6
	return userTime + sysTime
}

// getPercentage вычисляет процент использования CPU, сравнивая затраченное процессорное время с реальным временем.
func (mon *cpuUtilizationMonitorDarwin) getPercentage() float64 {
	currentCPUTime := getCPUTime()
	cpuTimeDiff := currentCPUTime - mon.startCPUTime
	realSeconds := time.Since(mon.startTime).Seconds()
	return (cpuTimeDiff / realSeconds) * 100
}

// NewCPUUtilizationMonitor возвращает новый экземпляр мониторинга использования CPU.
func NewCPUUtilizationMonitor() cpuUtilizationMonitor {
	return &cpuUtilizationMonitorDarwin{
		startTime:    time.Now(),
		startCPUTime: getCPUTime(),
	}
}
