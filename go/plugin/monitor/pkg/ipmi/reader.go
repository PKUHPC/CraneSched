package ipmi

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	logrus "github.com/sirupsen/logrus"
	"github.com/u-root/u-root/pkg/ipmi"

	"CraneFrontEnd/plugin/monitor/pkg/types"
)

var log = logrus.WithField("component", "IPMI")

const (
	MetricsCacheDuration   = 500 * time.Millisecond
	CMD_GET_SENSOR_READING = 0x2D
	NETFN_SENSOR           = 0x04
)

type IPMIReader struct {
	ipmi    *ipmi.IPMI
	HasIPMI bool

	LastPowerTime  time.Time
	LastTotalPower float64
	LastCPUPower   float64
	metricsCache   *types.IPMIMetrics
	mutex          sync.Mutex
}

type SensorConfig struct {
	Name      string
	SensorNum uint8
	Scale     float64
	MaxValue  float64
}

var powerSensors = []SensorConfig{
	{
		Name:      "Total_Power",
		SensorNum: 0xdb,
		Scale:     8.0,
		MaxValue:  1000.0,
	},
	{
		Name:      "CPU_Power",
		SensorNum: 0xdc,
		Scale:     2.0,
		MaxValue:  500.0,
	},
}

// Sensor configuration for Huawei openEuler systems
var openEulerPowerSensors = []SensorConfig{
	{
		Name:      "Total_Power",
		SensorNum: 0x25, // Power sensor ID from ipmitool (0x25)
		Scale:     7.0,  // No scaling needed, direct watt value
		MaxValue:  1000.0,
	},
	{
		Name:      "CPU_Power",
		SensorNum: 0x8, // CPU Power sensor ID from ipmitool (0x8)
		Scale:     2.0, // No scaling needed, direct watt value
		MaxValue:  500.0,
	},
}

func NewIPMIReader() *IPMIReader {
	reader := &IPMIReader{}

	i, err := ipmi.Open(0)
	if err != nil {
		log.Warnf("Failed to open IPMI device: %v", err)
		return nil
	}

	reader.ipmi = i
	reader.HasIPMI = true

	// Detect system type and select appropriate sensor configuration
	if isOpenEulerSystem() {
		log.Infof("Detected openEuler system, using openEuler IPMI sensor configuration")
		powerSensors = openEulerPowerSensors
	}

	return reader
}

func isOpenEulerSystem() bool {
	osReleaseContent, err := os.ReadFile("/etc/os-release")
	if err == nil && strings.Contains(string(osReleaseContent), "openEuler") {
		return true
	}

	return false
}

func (r *IPMIReader) getPowerUsage(sensor SensorConfig) (float64, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if !r.HasIPMI {
		return 0, fmt.Errorf("IPMI not available")
	}

	cmd := []byte{NETFN_SENSOR, CMD_GET_SENSOR_READING, sensor.SensorNum}
	resp, err := r.ipmi.RawCmd(cmd)
	if err != nil {
		return 0, fmt.Errorf("failed to get sensor reading: %v", err)
	}

	if len(resp) < 4 {
		return 0, fmt.Errorf("invalid response length")
	}

	if resp[0] != 0x00 {
		return 0, fmt.Errorf("command failed with code: 0x%02x", resp[0])
	}

	if resp[2] != 0xC0 || resp[3] != 0xC0 {
		return 0, fmt.Errorf("invalid reading flags: %02x %02x", resp[2], resp[3])
	}

	rawReading := float64(resp[1])
	power := rawReading * sensor.Scale

	// verify if the reading is within the reasonable range
	if power < 0 || power > sensor.MaxValue {
		log.Warnf("%s reading out of range: %.2f W (raw: 0x%02x)",
			sensor.Name, power, uint8(rawReading))
		return 0, fmt.Errorf("reading out of range")
	}

	return power, nil
}

func (r *IPMIReader) GetMetrics() *types.IPMIMetrics {
	if !r.HasIPMI {
		log.Warnf("IPMI not available")
		return &types.IPMIMetrics{}
	}

	if time.Since(r.LastPowerTime) < MetricsCacheDuration && r.metricsCache != nil {
		return r.metricsCache
	}

	currentTime := time.Now()
	metrics := &types.IPMIMetrics{}

	for _, sensor := range powerSensors {
		if reading, err := r.getPowerUsage(sensor); err == nil {
			switch sensor.Name {
			case "Total_Power":
				metrics.Power = reading
			case "CPU_Power":
				metrics.CPUPower = reading
			}
		} else {
			log.Warnf("Failed to get %s: %v", sensor.Name, err)
		}
	}

	if !r.LastPowerTime.IsZero() {
		duration := currentTime.Sub(r.LastPowerTime).Seconds()
		metrics.Energy = (metrics.Power + r.LastTotalPower) * duration / 2
		metrics.CPUEnergy = (metrics.CPUPower + r.LastCPUPower) * duration / 2
	} else {
		metrics.Energy = 0
	}

	r.LastPowerTime = currentTime
	r.LastTotalPower = metrics.Power
	r.LastCPUPower = metrics.CPUPower
	r.metricsCache = metrics

	return metrics
}

func (r *IPMIReader) LogMetrics(metrics *types.IPMIMetrics) {
	log.Debugf("IPMI Metrics:")
	log.Debugf("Power: %.2f W, Energy: %.2f J", metrics.Power, metrics.Energy)
	log.Debugf("CPU Power: %.2f W, CPU Energy: %.2f J", metrics.CPUPower, metrics.CPUEnergy)
}

func (r *IPMIReader) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.ipmi != nil {
		return r.ipmi.Close()
	}
	return nil
}
