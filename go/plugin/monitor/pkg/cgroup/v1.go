package cgroup

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/cgroups"
	v1 "github.com/containerd/cgroups/stats/v1"
	logrus "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/monitor/pkg/types"
)

/*
#include <unistd.h>
*/
import "C"

var log = logrus.WithField("component", "CgroupV1")

const (
	MB        = 1024 * 1024
	NS_TO_SEC = 1e9
)

func getClockTicks() int64 {
	ticks := C.sysconf(C._SC_CLK_TCK)
	if ticks == -1 {
		log.Warnf("Failed to get CLK_TCK, using default value 100")
		return 100
	}
	return int64(ticks)
}

type V1Reader struct {
	cgroupName   string
	control      cgroups.Cgroup
	startTime    time.Time
	startCPUTime uint64

	clockTicks int64
}

var _ CgroupReader = (*V1Reader)(nil)

func NewV1Reader(cgroupName string) *V1Reader {
	if cgroupName == "" {
		return nil
	}

	control, err := cgroups.Load(cgroups.V1, cgroups.StaticPath(cgroupName))
	if err != nil {
		return nil
	}

	startCPUTime, err := readTotalCPUTime()
	if err != nil {
		log.Errorf("Failed to read initial CPU time: %v", err)
	}

	reader := V1Reader{
		cgroupName:   cgroupName,
		control:      control,
		startCPUTime: startCPUTime,
		startTime:    time.Now(),
		clockTicks:   getClockTicks(),
	}

	log.Infof("Successfully loaded cgroup: %s", cgroupName)
	for _, subsystem := range control.Subsystems() {
		log.Infof("Subsystem: %s", subsystem.Name())
	}

	return &reader
}

func (r *V1Reader) GetCgroupStats() (types.CgroupStats, error) {
	currentTime := time.Now()
	stats := types.CgroupStats{}

	v1Metrics, err := r.control.Stat()
	if err != nil || v1Metrics == nil {
		log.Infof("Cgroup appears to be removed: metrics unavailable")
		return stats, fmt.Errorf("cgroup no longer exists")
	}

	duration := currentTime.Sub(r.startTime).Seconds()

	cpuStats := r.calculateCPUStats(v1Metrics)
	memoryStats := calculateMemoryStats(v1Metrics)
	ioStats := calculateIOStats(v1Metrics, duration)

	stats = types.CgroupStats{
		CPUStats:    cpuStats,
		MemoryStats: memoryStats,
		IOStats:     ioStats,
	}

	r.logCgroupStats(stats)
	return stats, nil
}

func (r *V1Reader) calculateCPUStats(v1Metrics *v1.Metrics) types.CPUStats {
	if v1Metrics == nil || v1Metrics.CPU == nil {
		log.Warn("CPU metrics not available")
		return types.CPUStats{}
	}

	stats := types.CPUStats{
		UsageSeconds: float64(v1Metrics.CPU.Usage.Total) / NS_TO_SEC,
	}

	currentCPUTime, err := readTotalCPUTime()
	if err != nil {
		log.Warnf("Failed to read current CPU time: %v", err)
		return stats
	}

	if r.startCPUTime > 0 && currentCPUTime > r.startCPUTime {
		systemCPUDiff := currentCPUTime - r.startCPUTime

		// convert nanoseconds to jiffies
		nsPerJiffy := NS_TO_SEC / r.clockTicks
		CPUJiffies := v1Metrics.CPU.Usage.Total / uint64(nsPerJiffy)

		numCPUs := float64(len(v1Metrics.CPU.Usage.PerCPU))
		if numCPUs == 0 {
			numCPUs = 1
		}

		cpuUtil := (float64(CPUJiffies) / float64(systemCPUDiff)) * 100
		if cpuUtil > 100 {
			log.Warnf("CPU utilization exceeds 100%% (%.2f%%), capping at 100%%", cpuUtil)
			cpuUtil = 100
		}
		stats.Utilization = cpuUtil
	}

	return stats
}

func calculateMemoryStats(v1Metrics *v1.Metrics) types.MemoryStats {
	if v1Metrics == nil || v1Metrics.Memory == nil {
		log.Warn("Memory metrics not available")
		return types.MemoryStats{}
	}

	stats := types.MemoryStats{
		UsageMB: float64(v1Metrics.Memory.Usage.Usage) / MB,
	}

	limit := v1Metrics.Memory.Usage.Limit
	if limit == 0 || limit == math.MaxUint64 {
		var si syscall.Sysinfo_t
		if err := syscall.Sysinfo(&si); err != nil {
			log.Warnf("Failed to get system memory info: %v", err)
			return stats
		}
		limit = si.Totalram
	}

	if limit > 0 {
		memUtil := (float64(v1Metrics.Memory.Usage.Usage) / float64(limit)) * 100
		if memUtil > 100 {
			log.Warnf("Memory utilization exceeds 100%% (%.2f%%), capping at 100%%", memUtil)
			memUtil = 100
		}
		stats.Utilization = memUtil
	}

	return stats
}

func calculateIOStats(v1Metrics *v1.Metrics, duration float64) types.IOStats {
	var stats types.IOStats

	if v1Metrics == nil || v1Metrics.Blkio == nil {
		log.Warn("IO metrics not available")
		return stats
	}

	var (
		totalReadBytes  uint64
		totalWriteBytes uint64
		totalReadOps    uint64
		totalWriteOps   uint64
	)

	for _, entry := range v1Metrics.Blkio.IoServiceBytesRecursive {
		switch entry.Op {
		case "Read":
			totalReadBytes += entry.Value
		case "Write":
			totalWriteBytes += entry.Value
		}
	}

	for _, entry := range v1Metrics.Blkio.IoServicedRecursive {
		switch entry.Op {
		case "Read":
			totalReadOps += entry.Value
		case "Write":
			totalWriteOps += entry.Value
		}
	}

	stats.ReadMB = float64(totalReadBytes) / MB
	stats.WriteMB = float64(totalWriteBytes) / MB
	stats.ReadOperations = totalReadOps
	stats.WriteOperations = totalWriteOps

	if duration > 0 {
		stats.ReadMBPS = stats.ReadMB / duration
		stats.WriteMBPS = stats.WriteMB / duration
		stats.ReadOpsPerSec = float64(totalReadOps) / duration
		stats.WriteOpsPerSec = float64(totalWriteOps) / duration
	}

	return stats
}

func readTotalCPUTime() (uint64, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, fmt.Errorf("failed to read /proc/stat: %v", err)
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)[1:] // skip "cpu" field
			var total uint64
			for _, field := range fields {
				val, err := strconv.ParseUint(field, 10, 64)
				if err != nil {
					return 0, fmt.Errorf("failed to parse CPU time: %v", err)
				}
				total += val
			}
			return total, nil
		}
	}

	return 0, fmt.Errorf("CPU stats not found in /proc/stat")
}

func (r *V1Reader) logCgroupStats(usage types.CgroupStats) {
	log.Infof("%s Cgroup Metrics:", r.cgroupName)
	log.Infof("CPU Usage: %.2f s, CPU Utilization: %.2f%%", usage.CPUStats.UsageSeconds, usage.CPUStats.Utilization)
	log.Infof("Memory Usage: %.2f MB, Memory Utilization: %.2f%%", usage.MemoryStats.UsageMB, usage.MemoryStats.Utilization)
	log.Infof("IO Read: %.2f MB, IO Write: %.2f MB, IO Read Speed: %.2f MB/s, IO Write Speed: %.2f MB/s", usage.IOStats.ReadMB, usage.IOStats.WriteMB, usage.IOStats.ReadMBPS, usage.IOStats.WriteMBPS)
	log.Infof("IO Read Operations count: %d, IO Write Operations count: %d, IO Read Operations per second: %.2f, IO Write Operations per second: %.2f", usage.IOStats.ReadOperations, usage.IOStats.WriteOperations, usage.IOStats.ReadOpsPerSec, usage.IOStats.WriteOpsPerSec)
}
