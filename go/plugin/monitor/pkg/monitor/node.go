package monitor

import (
	"time"

	"CraneFrontEnd/plugin/monitor/pkg/config"
	"CraneFrontEnd/plugin/monitor/pkg/db"
	"CraneFrontEnd/plugin/monitor/pkg/gpu"
	"CraneFrontEnd/plugin/monitor/pkg/ipmi"
	"CraneFrontEnd/plugin/monitor/pkg/rapl"
	"CraneFrontEnd/plugin/monitor/pkg/sysload"
	"CraneFrontEnd/plugin/monitor/pkg/types"
)

type NodeMonitor struct {
	samplePeriod time.Duration
	config       *config.MonitorConfig

	raplReader    *rapl.RAPLReader
	ipmiReader    *ipmi.IPMIReader
	gpuReader     *gpu.Reader
	sysLoadReader *sysload.SystemLoadReader
	jobMonitor    *JobMonitor

	stopCh chan struct{}
}

func (r *NodeMonitor) Start() {
	go r.collectNodeEnergy()
}

func (r *NodeMonitor) collectNodeEnergy() {
	ticker := time.NewTicker(r.samplePeriod)
	defer ticker.Stop()

	metricsCollector := func() *types.NodeData {
		data := &types.NodeData{
			Timestamp: time.Now(),
			NodeID:    getNodeID(),
		}

		if r.raplReader != nil {
			if raplData := r.raplReader.GetMetrics(); raplData != nil {
				data.RAPL = *raplData
			}
		} else {
			r.config.Enabled.RAPL = false
		}

		if r.ipmiReader != nil {
			if ipmiData := r.ipmiReader.GetMetrics(); ipmiData != nil {
				data.IPMI = *ipmiData
			}
		} else {
			r.config.Enabled.IPMI = false
		}

		if r.gpuReader != nil {
			if gpuData := r.gpuReader.GetMetrics(); gpuData != nil {
				data.GPU = *gpuData
			}
		} else {
			r.config.Enabled.GPU = false
		}

		if r.sysLoadReader != nil {
			if sysData := r.sysLoadReader.GetMetrics(); sysData != nil {
				data.SystemLoad = *sysData
			}
		} else {
			r.config.Enabled.System = false
		}

		if r.jobMonitor != nil {
			data.JobMetrics = r.jobMonitor.GetJobMetrics()
		}

		if r.raplReader != nil {
			r.raplReader.LogMetrics(&data.RAPL)
		}
		if r.ipmiReader != nil {
			r.ipmiReader.LogMetrics(&data.IPMI)
		}
		if r.gpuReader != nil {
			r.gpuReader.LogMetrics(&data.GPU)
		}
		if r.sysLoadReader != nil {
			r.sysLoadReader.LogMetrics(&data.SystemLoad)
		}

		return data
	}

	for {
		select {
		case <-ticker.C:
			data := metricsCollector()
			r.broadcastNodeData(data)
			if err := db.GetInstance().SaveNodeEnergy(data); err != nil {
				log.Errorf("Error saving node energy data: %v", err)
			}
		case <-r.stopCh:
			return
		}
	}
}

func (r *NodeMonitor) broadcastNodeData(data *types.NodeData) {
	subscriberManager := GetSubscriberManagerInstance()
	subscriberManager.subscribers.Range(func(key, value interface{}) bool {
		if sub, ok := value.(*NodeDataSubscriber); ok {
			if data.Timestamp.After(sub.StartTime) {
				select {
				case sub.Ch <- data:
				default:
					log.Warnf("job %v channel full, skipping data", key)
				}
			}
		}
		return true
	})
}

func (r *NodeMonitor) Close() {
	if r.raplReader != nil {
		if err := r.raplReader.Close(); err != nil {
			log.Errorf("error closing RAPL reader: %v", err)
		}
	}

	if r.ipmiReader != nil {
		if err := r.ipmiReader.Close(); err != nil {
			log.Errorf("error closing IPMI reader: %v", err)
		}
	}

	if r.gpuReader != nil {
		if err := r.gpuReader.Close(); err != nil {
			log.Errorf("error closing GPU reader: %v", err)
		}
	}

	if r.sysLoadReader != nil {
		r.sysLoadReader.Close()
	}

	subscriberManager := GetSubscriberManagerInstance()
	subscriberManager.Close()

	close(r.stopCh)
}
