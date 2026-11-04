//go:build !with_nvml

package gpu

const (
	withNVML = false
)

type NVMLDevice struct{}

func (d *NVMLDevice) GetPowerUsage() (uint, error)        { return 0, nil }
func (d *NVMLDevice) GetTemperature() (uint, error)       { return 0, nil }
func (d *NVMLDevice) GetUtilization() (uint, uint, error) { return 0, 0, nil }
func (d *NVMLDevice) GetUUID() (string, error)            { return "", nil }
func (d *NVMLDevice) GetPciInfo() (string, error)         { return "", nil }

type NVMLManager struct{}

func (m *NVMLManager) Init() error                            { return nil }
func (m *NVMLManager) Close() error                           { return nil }
func (m *NVMLManager) GetDeviceCount() (int, error)           { return 0, nil }
func (m *NVMLManager) GetDevice(index int) (GPUDevice, error) { return &NVMLDevice{}, nil }
