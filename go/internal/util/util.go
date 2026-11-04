/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package util

import (
	"CraneFrontEnd/api"
	"time"
)

type Config struct {
	ClusterName                    string            `yaml:"ClusterName"`
	ControlMachine                 string            `yaml:"ControlMachine"`
	ControlMachineAddr             string            `yaml:"ControlMachineAddr"`
	CraneCtldListenPort            string            `yaml:"CraneCtldListenPort"`
	CraneCtldForInternalListenPort string            `yaml:"CraneCtldForInternalListenPort"`
	CranedNodeList                 []ConfigNodesList `yaml:"Nodes"`

	TlsConfig TLSConfig `yaml:"TLS"`

	CraneBaseDir         string  `yaml:"CraneBaseDir"`
	CforedLogDir         string  `yaml:"CforedLogDir"`
	CforedDebugLevel     *string `yaml:"CforedDebugLevel"`
	CranedCforedSockPath string  `yaml:"CranedCforedSockPath"`

	JobLifecycleHook JobLifecycleHookConfig `yaml:"JobLifecycleHook"`

	Container ContainerConfig `yaml:"Container"`
	Cfored    CforedConfig    `yaml:"Cfored"`
}

func (c *Config) ControlMachineConnectAddr() string {
	if c.ControlMachineAddr != "" {
		return c.ControlMachineAddr
	}
	return c.ControlMachine
}

type TLSConfig struct {
	Enabled              bool   `yaml:"Enabled"`
	InternalCertFilePath string `yaml:"InternalCertFilePath"`
	InternalKeyFilePath  string `yaml:"InternalKeyFilePath"`
	ExternalCertFilePath string `yaml:"ExternalCertFilePath"`
	CaFilePath           string `yaml:"CaFilePath"`
	DomainSuffix         string `yaml:"DomainSuffix"`
	UserTlsCertPath      string `yaml:"UserTlsCertPath"`
}

type CforedConfig struct {
	DebugLevel  string `yaml:"DebugLevel"`
	PidFilePath string `yaml:"PidFilePath"`
}

type PluginConfig struct {
	Enabled       bool             `yaml:"Enabled"`
	SockPath      string           `yaml:"PlugindSockPath"`
	ListenAddress string           `yaml:"PlugindListenAddress"`
	ListenPort    string           `yaml:"PlugindListenPort"`
	LogLevel      string           `yaml:"PlugindDebugLevel"`
	Plugins       []api.PluginMeta `yaml:"Plugins"`
}

type JobLifecycleHookConfig struct {
	CrunProlog          string `yaml:"CrunProlog"`
	CrunEpilog          string `yaml:"CrunEpilog"`
	PrologTimeout       uint64 `yaml:"PrologTimeout"`
	EpilogTimeout       uint64 `yaml:"EpilogTimeout"`
	PrologEpilogTimeout uint64 `yaml:"PrologEpilogTimeout"`
}

type ContainerConfig struct {
	UserNsEnabledByDefault bool `yaml:"UserNsEnabledByDefault"`
}

// InfluxDB Config represents the structure of the database configuration
type InfluxDbConfig struct {
	Username    string `yaml:"Username"`
	Bucket      string `yaml:"Bucket"`
	Org         string `yaml:"Org"`
	Token       string `yaml:"Token"`
	Measurement string `yaml:"Measurement"`
	Url         string `yaml:"Url"`
}

type ConfigNodesList struct {
	Name         string `yaml:"name"`
	NodeHostname string `yaml:"NodeHostname"`
	CPU          int    `yaml:"cpu"`
	Memory       string `yaml:"memory"`
}

// Path = BaseDir + Dir + Name
const (
	DefaultConfigPath       = "/etc/crane/config.yaml"
	DefaultPluginConfigPath = "/etc/crane/plugin.yaml"
	DefaultCraneBaseDir     = "/var/crane/"
	DefaultCforedLogDir     = "cfored/"

	DefaultPlugindSocketPath = "cplugind/cplugind.sock"

	DefaultUserConfigPrefix = ".config/crane"

	DefaultCforedSocketPath          = "craned/cfored.sock"
	DefaultCforedPidFilePath         = "cfored/cfored.pid"
	DefaultCforedServerListenAddress = "0.0.0.0"
	DefaultCforedServerListenPort    = "10012"

	DefaultWrappedJobName                 = "Wrap"
	DefaultUserNsEnabledByDefault         = true
	DefaultCacctMaxLines           uint32 = 20
	DefaultCqueueMaxLines          uint32 = 10000
	GresGpuName                           = "gpu"
	MaxJobNameLength                      = 60
	MaxJobFileNameLength                  = 127
	MaxJobFilePathLengthForWindows        = 260 - MaxJobFileNameLength
	MaxJobFilePathLengthForUnix           = 4096 - MaxJobFileNameLength

	MaxJobTimeLimit = 315576000000 // 10000 years
	MaxJobTimeStamp = 253402300799 // 9999-12-31 23:59:59

	MaxEntityNameLength = 30

	MaxJobMemoryBytes = 10737418240000 // 10000GB
	MaxArrayTaskCount = 10000          // Keep in sync with backend kMaxArrayTaskCount.

	// UnlimitedCpuThreshold is used to detect "unlimited" CPU values from backend.
	// Backend kUnlimitedCpu raw = (1<<53)-1, as double ≈ 3.5e13.
	// Any value above 1e13 is treated as unlimited.
	UnlimitedCpuThreshold float64 = 1e13

	// UnlimitedCpuValue is the default "unlimited" CPU value sent to backend
	// when creating QoS without specifying a CPU limit.
	// Matches backend's kUnlimitedCpu: cpu_t::from_raw_value(1<<53).
	UnlimitedCpuValue float64 = float64(int64(1)<<53) / 256.0
)

// Param Options
const (
	OpenModeAppend   = "append"
	OpenModeTruncate = "truncate"
)

const (
	QosFlagNone        uint32 = 0
	QOSFlagDenyOnLimit uint32 = 1
)

var QoSFlagNameMap = map[string]uint32{
	"none":        QosFlagNone,
	"denyonlimit": QOSFlagDenyOnLimit,
}

var (
	InfiniteFuture = time.Unix(1<<63-1, 0)
)
