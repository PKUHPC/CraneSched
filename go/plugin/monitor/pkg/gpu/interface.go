package gpu

type GPUDevice interface {
	GetPowerUsage() (uint, error)
	GetTemperature() (uint, error)
	GetUtilization() (uint, uint, error)
	GetUUID() (string, error)
	GetPciInfo() (string, error)
}

type GPUManager interface {
	Init() error
	Close() error
	GetDeviceCount() (int, error)
	GetDevice(index int) (GPUDevice, error)
}
