package types

import (
	"time"
)

type NodeData struct {
	NodeID    string
	Timestamp time.Time

	RAPL       RAPLMetrics
	IPMI       IPMIMetrics
	GPU        GPUMetrics
	SystemLoad SystemLoadMetrics
	JobMetrics *JobMetrics
}

type RAPLMetrics struct {
	Package float64 // unit: J
	Core    float64 // unit: J
	Uncore  float64 // unit: J
	DRAM    float64 // unit: J
	GT      float64 // unit: J
}

type IPMIMetrics struct {
	Power    float64 // unit: W
	CPUPower float64 // unit: W

	Energy    float64 // unit: J
	CPUEnergy float64 // unit: J
}

type GPUMetrics struct {
	Energy  float64 // unit: J
	Power   float64 // unit: W
	Util    float64 // GPU utilization(%)
	MemUtil float64 // GPU VRAM utilization(%)
	Temp    float64 // unit: C
}

type SystemLoadMetrics struct {
	CPUUtil        float64 // CPU utilization(%)
	CPULoad1       float64 // 1 minute load average
	CPULoad5       float64 // 5 minute load average
	CPULoad15      float64 // 15 minute load average
	Frequencies    float64 // CPU average frequency(MHz)
	CPUTemperature float64 // CPU temperature(C)

	MemoryUtil  float64 // memory utilization(%)
	MemoryUsed  float64 // used memory(GB)
	MemoryTotal float64 // total memory(GB)

	DiskUtil float64 // disk utilization(%)
	DiskIO   float64 // disk IO(MB/s)

	NetworkIO float64 // network IO(MB/s)
	NetworkRx float64 // network receive rate(MB/s)
	NetworkTx float64 // network transmit rate(MB/s)
}

type JobMetrics struct {
	JobCount             int     // Number of running jobs on this node
	ReqCPURate           float64 // Rate of CPU cores requested by jobs to total CPU cores
	ReqMemoryRate        float64 // Rate of memory requested by jobs to total memory
	AvgReqCPUPerJob      float64 // Average CPU cores requested per job
	AvgReqMemoryGBPerJob float64 // Average memory requested per job
	AvgJobRuntime        float64 // Average runtime of running jobs in seconds
}

// Record the total energy consumption and average indicators after the job is completed.
type JobData struct {
	JobID  uint32
	NodeID string

	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration

	TotalEnergy  float64 // unit: J
	CPUEnergy    float64 // unit: J
	GPUEnergy    float64 // unit: J
	AveragePower float64 // unit: W

	CgroupStats CgroupStats
}

type CgroupStats struct {
	CPUStats    CPUStats
	GPUStats    GPUStats
	MemoryStats MemoryStats
	IOStats     IOStats
}

type CPUStats struct {
	UsageSeconds float64 // unit: s
	Utilization  float64 // unit: %
}

type GPUStats struct {
	Utilization float64 // unit: %
	MemoryUtil  float64 // unit: %
}

type MemoryStats struct {
	UsageMB     float64 // unit: MB
	Utilization float64 // unit: %
}

type IOStats struct {
	ReadMB    float64 // unit: MB
	WriteMB   float64 // unit: MB
	ReadMBPS  float64 // unit: MB/s
	WriteMBPS float64 // unit: MB/s

	ReadOperations  uint64  // read IO count
	WriteOperations uint64  // write IO count
	ReadOpsPerSec   float64 // read IO count/s
	WriteOpsPerSec  float64 // write IO count/s
}
