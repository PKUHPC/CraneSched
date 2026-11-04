package gpu

import (
	"fmt"
	"strings"
	"time"

	logrus "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/monitor/pkg/types"
)

var log = logrus.WithField("component", "GPU")

type GPUType string

const (
	NVIDIA GPUType = "nvidia"
	AMD    GPUType = "amd"
	ASCEND GPUType = "ascend"
)

type deviceState struct {
	index     int
	lastPower float64
	lastTime  time.Time
}

type Reader struct {
	hasGPU      bool
	manager     GPUManager
	deviceCount int
	devices     []*deviceState
}

func NewGPUReader(gpuType string) *Reader {
	var manager GPUManager

	switch GPUType(strings.ToLower(gpuType)) {
	case NVIDIA:
		manager = &NVMLManager{}
		if !withNVML {
			log.Info("Using NVIDIA GPU without NVML")
		} else {
			log.Info("Using NVIDIA GPU with NVML")
		}
	default:
		log.Warnf("Unsupported GPU type: %s, falling back to nvidia", gpuType)
		manager = &NVMLManager{}
	}

	reader := &Reader{
		manager: manager,
	}

	if err := reader.init(); err != nil {
		log.WithError(err).Info("Init failed, will run in no GPU mode")
	}

	return reader
}

func (r *Reader) init() error {
	if err := r.manager.Init(); err != nil {
		return fmt.Errorf("GPU manager init failed: %v", err)
	}

	count, err := r.manager.GetDeviceCount()
	if err != nil {
		r.Close()
		return fmt.Errorf("failed to get GPU count: %v", err)
	}

	if count > 0 {
		r.hasGPU = true
		r.deviceCount = count
		r.devices = make([]*deviceState, count)
		for i := 0; i < count; i++ {
			r.devices[i] = &deviceState{index: i}
		}

		log.Infof("detected %d GPUs", count)
	}
	return nil
}

func (r *Reader) GetMetrics() *types.GPUMetrics {
	if !r.hasGPU {
		log.Warnf("no GPU available")
		return &types.GPUMetrics{}
	}

	metrics := &types.GPUMetrics{}
	activeDevices := 0

	for i := 0; i < r.deviceCount; i++ {
		deviceMetrics, err := r.GetDeviceMetrics(i)
		if err != nil {
			log.Errorf("Failed to collect metrics: %v", err)
			continue
		}

		metrics.Power += deviceMetrics.Power
		metrics.Energy += deviceMetrics.Energy
		metrics.Util += deviceMetrics.Util
		metrics.MemUtil += deviceMetrics.MemUtil
		metrics.Temp += deviceMetrics.Temp
		activeDevices++
	}

	if activeDevices > 0 {
		metrics.Util /= float64(activeDevices)
		metrics.MemUtil /= float64(activeDevices)
		metrics.Temp /= float64(activeDevices)
	}

	return metrics
}

func (r *Reader) GetDeviceMetrics(index int) (*types.GPUMetrics, error) {
	metrics := &types.GPUMetrics{}

	device, err := r.manager.GetDevice(index)
	if err != nil {
		return nil, fmt.Errorf("failed to get device: %v", err)
	}

	power, err := device.GetPowerUsage()
	if err != nil {
		return nil, fmt.Errorf("power metrics: %v", err)
	}
	metrics.Power = float64(power) / 1000.0 // mW -> W

	gpuUtil, memUtil, err := device.GetUtilization()
	if err != nil {
		return nil, fmt.Errorf("utilization metrics: %v", err)
	}
	metrics.Util = float64(gpuUtil)
	metrics.MemUtil = float64(memUtil)

	temp, err := device.GetTemperature()
	if err != nil {
		return nil, fmt.Errorf("temperature metrics: %v", err)
	}
	metrics.Temp = float64(temp)

	currentTime := time.Now()
	if state := r.devices[index]; !state.lastTime.IsZero() {
		duration := currentTime.Sub(state.lastTime).Seconds()
		averagePower := (metrics.Power + state.lastPower) / 2
		metrics.Energy = averagePower * duration
	}

	r.devices[index].lastPower = metrics.Power
	r.devices[index].lastTime = currentTime

	return metrics, nil
}

func (r *Reader) GetBoundGpuMetrics(indices []int) *types.GPUMetrics {
	if !r.hasGPU {
		return &types.GPUMetrics{}
	}

	metrics := &types.GPUMetrics{}
	activeDevices := 0

	if len(indices) == 1 && indices[0] == -1 {
		metrics := r.GetMetrics()
		if metrics == nil {
			log.Errorf("Failed to get all GPU metrics")
			return nil
		}
		return metrics
	}

	for _, idx := range indices {
		if idx < 0 || idx >= r.deviceCount {
			log.Errorf("invalid device index: %d, total devices: %d", idx, r.deviceCount)
			continue
		}

		deviceMetrics, err := r.GetDeviceMetrics(idx)
		if err != nil {
			log.Errorf("Failed to collect metrics: %v", err)
			continue
		}

		metrics.Power += deviceMetrics.Power
		metrics.Energy += deviceMetrics.Energy
		metrics.Util += deviceMetrics.Util
		metrics.MemUtil += deviceMetrics.MemUtil
		metrics.Temp += deviceMetrics.Temp
		activeDevices++
	}

	if activeDevices > 0 {
		metrics.Util /= float64(activeDevices)
		metrics.MemUtil /= float64(activeDevices)
		metrics.Temp /= float64(activeDevices)
	}

	return metrics
}

func (r *Reader) GetDeviceCount() int {
	return r.deviceCount
}

func (r *Reader) LogMetrics(metrics *types.GPUMetrics) {
	log.Debugf("GPU Metrics: power=%.2f W, energy=%.2f J, util=%.2f%%", metrics.Power, metrics.Energy, metrics.Util)
	log.Debugf("GPU Metrics: mem_util=%.2f%%, temp=%.1f°C", metrics.MemUtil, metrics.Temp)
}

func (r *Reader) Close() error {
	if r.hasGPU {
		if err := r.manager.Close(); err != nil {
			return fmt.Errorf("failed to cleanup GPU manager: %v", err)
		}
	}
	return nil
}
