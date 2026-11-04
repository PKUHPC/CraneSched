package monitor

import (
	"sync"
	"time"

	logrus "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/monitor/pkg/config"
	"CraneFrontEnd/plugin/monitor/pkg/gpu"
	"CraneFrontEnd/plugin/monitor/pkg/ipmi"
	"CraneFrontEnd/plugin/monitor/pkg/rapl"
	"CraneFrontEnd/plugin/monitor/pkg/sysload"
)

var log = logrus.WithField("component", "Monitor")

type Monitor struct {
	NodeMonitor *NodeMonitor
	JobMonitor  *JobMonitor
}

func NewMonitor(config config.MonitorConfig) *Monitor {
	duration, err := time.ParseDuration(config.SamplePeriod)
	if err != nil {
		log.Errorf("invalid sample period: %v", err)
		return nil
	}

	var raplReader *rapl.RAPLReader
	var ipmiReader *ipmi.IPMIReader
	var gpuReader *gpu.Reader
	var sysLoadReader *sysload.SystemLoadReader

	if config.Enabled.RAPL {
		raplReader = rapl.NewRAPLReader()
	}
	if config.Enabled.IPMI {
		ipmiReader = ipmi.NewIPMIReader()
	}
	if config.Enabled.GPU {
		gpuReader = gpu.NewGPUReader(config.GPUType)
	}
	if config.Enabled.System {
		sysLoadReader = sysload.NewSystemLoadReader()
	}

	jobMonitor := &JobMonitor{
		samplePeriod: duration,
		config:       &config,
		jobs:         make(map[uint32]*Job),
		mutex:        sync.RWMutex{},
	}

	return &Monitor{
		NodeMonitor: &NodeMonitor{
			samplePeriod:  duration,
			config:        &config,
			raplReader:    raplReader,
			ipmiReader:    ipmiReader,
			gpuReader:     gpuReader,
			sysLoadReader: sysLoadReader,
			jobMonitor:    jobMonitor,
			stopCh:        make(chan struct{}),
		},
		JobMonitor: jobMonitor,
	}
}

func (sm *Monitor) Close() {
	sm.NodeMonitor.Close()
	sm.JobMonitor.Close()
}
