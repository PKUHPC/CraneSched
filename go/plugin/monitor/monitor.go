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

package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/plugin/monitor/pkg/config"
	"CraneFrontEnd/plugin/monitor/pkg/db"
	"CraneFrontEnd/plugin/monitor/pkg/monitor"

	nested "github.com/antonfisher/nested-logrus-formatter"
	logrus "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var log = logrus.WithField("component", "MonitorPlugin")

func init() {
	logrus.SetFormatter(&nested.Formatter{
		HideKeys:        true,
		TimestampFormat: "2006-01-02 15:04:05",
		ShowFullLevel:   true,
		NoColors:        false,
		NoFieldsColors:  false,
		NoFieldsSpace:   true,
		FieldsOrder:     []string{"caller", "component"},
		CustomCallerFormatter: func(f *runtime.Frame) string {
			filename := path.Base(f.File)
			return fmt.Sprintf(" [%s:%d]", filename, f.Line)
		},
	})

	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.DebugLevel)
}

var _ api.Plugin = MonitorPlugin{}
var _ api.CgroupLifecycleHooks = MonitorPlugin{}
var _ api.NodeEventHooks = MonitorPlugin{}
var _ api.GrpcServiceRegistrar = MonitorPlugin{}
var _ api.HostConfigAware = MonitorPlugin{}
var _ api.ResourceHooks = MonitorPlugin{}

var PluginInstance = MonitorPlugin{}

var gpuConfigs = map[string]struct {
	pattern    *regexp.Regexp
	validRange func(int) bool
}{
	"nvidia": {
		pattern:    regexp.MustCompile(`/dev/nvidia(\d+)`),
		validRange: func(num int) bool { return num >= 0 && num < 128 },
	},
}

type GlobalMonitor struct {
	config         *config.Config
	monitor        *monitor.Monitor
	queryService   *QueryService
	hostConfigPath string
}

var globalMonitor GlobalMonitor

type MonitorPlugin struct{}

func (p MonitorPlugin) Name() string {
	return "monitor"
}

func (p MonitorPlugin) Version() string {
	return "1.0.0"
}

func (p MonitorPlugin) SetHostConfigPath(path string) {
	globalMonitor.hostConfigPath = path
	if globalMonitor.queryService != nil {
		globalMonitor.queryService.SetHostConfigPath(path)
	}
}

func (p MonitorPlugin) Load(meta api.PluginMeta) error {
	log.Info("Initializing unified monitor plugin")

	cfg, err := config.LoadConfig(meta.Config)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if err := setupLogging(cfg.Monitor.LogPath); err != nil {
		log.Warnf("Failed to setup logging: %v, using default stderr", err)
	}

	globalMonitor.config = cfg
	config.PrintConfig(cfg)

	if err := p.ensureInitialized(); err != nil {
		return fmt.Errorf("failed to initialize resources: %w", err)
	}

	globalMonitor.monitor.NodeMonitor.Start()

	log.Info("Unified monitor plugin initialized successfully")
	return nil
}

func (p MonitorPlugin) Unload(meta api.PluginMeta) error {
	log.Info("Unloading monitor plugin")

	if globalMonitor.monitor != nil {
		globalMonitor.monitor.Close()
		globalMonitor.monitor = nil
	}

	if db.GetInstance() != nil {
		if err := db.GetInstance().Close(); err != nil {
			return fmt.Errorf("error closing database: %v", err)
		}
	}

	globalMonitor = GlobalMonitor{}

	log.Info("Monitor plugin gracefully unloaded")
	return nil
}

func (p MonitorPlugin) CreateCgroupHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.CreateCgroupHookRequest)
	if !ok {
		log.Error("Invalid request type, expected CreateCgroupHookRequest")
		return
	}

	log.Infof("CreateCgroupHook received for cgroup: %s", req.Cgroup)

	requestCpu := req.Resource.CpuCount
	requestMemory := req.Resource.MemoryBytes
	boundGPUs := getBoundGPUs(req.Resource.Gres, globalMonitor.config.Monitor.GPUType)
	resourceRequest := monitor.ResourceRequest{
		ReqCPU:    requestCpu,
		ReqMemory: requestMemory,
		ReqGPUs:   boundGPUs,
	}

	globalMonitor.monitor.JobMonitor.Start(req.JobId, req.Cgroup, resourceRequest)
}

func (p MonitorPlugin) DestroyCgroupHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.DestroyCgroupHookRequest)
	if !ok {
		log.Error("Invalid request type, expected DestroyCgroupHookRequest")
		return
	}

	log.Infof("DestroyCgroupHook received for cgroup: %s", req.Cgroup)
	globalMonitor.monitor.JobMonitor.Stop(req.JobId)
}

func (p MonitorPlugin) NodeEventHook(ctx *api.PluginContext) {
	if !globalMonitor.config.Monitor.Enabled.Event {
		log.Debug("Event monitoring is disabled, skipping NodeEventHook")
		return
	}

	req, ok := ctx.Request().(*protos.NodeEventHookRequest)
	if !ok {
		log.Error("Invalid request type, expected NodeEventHookRequest")
		return
	}

	log.Infof("NodeEventHook received for %d events", len(req.GetEventInfoList()))

	if err := db.GetInstance().SaveNodeEvents(req.GetEventInfoList()); err != nil {
		log.Errorf("Failed to save node events: %v", err)
	}
}

func (p MonitorPlugin) UpdateLicensesHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.UpdateLicensesHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected UpdateLicensesHookRequest.")
		return
	}

	log.Infof("UpdateLicensesHook received for %d licenses", len(req.GetLicenseInfo()))

	if err := db.GetInstance().SaveLicenseUsage(req.GetLicenseInfo()); err != nil {
		log.Errorf("Failed to save license usage: %v", err)
	}
}

func (p MonitorPlugin) RegisterGrpcServices(server grpc.ServiceRegistrar) error {
	if globalMonitor.queryService == nil {
		return fmt.Errorf("monitor query service is not initialized")
	}

	protos.RegisterPluginQueryServiceServer(server, globalMonitor.queryService)
	log.Info("Monitor plugin registered PluginQueryService gRPC endpoints")
	return nil
}

func (p MonitorPlugin) ensureInitialized() error {
	if db.GetInstance() == nil {
		err := db.InitDB(globalMonitor.config)
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
	}

	if globalMonitor.monitor == nil {
		globalMonitor.monitor = monitor.NewMonitor(globalMonitor.config.Monitor)
	}

	if globalMonitor.queryService == nil {
		globalMonitor.queryService = NewQueryService(globalMonitor.config)
	}

	if globalMonitor.hostConfigPath != "" {
		globalMonitor.queryService.SetHostConfigPath(globalMonitor.hostConfigPath)
	}

	return nil
}

func setupLogging(logPath string) error {
	logDir := path.Dir(logPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	logFile, err := os.OpenFile(logPath,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	logrus.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.Infof("Successfully set up logging to file %s", logPath)
	return nil
}

func getBoundGPUs(res *protos.DedicatedResourceInNode, gpuType string) []int {
	boundGPUs := make([]int, 0)

	gpuConfig, exists := gpuConfigs[gpuType]
	if !exists {
		log.Errorf("Unsupported GPU type: %s", gpuType)
		return boundGPUs
	}

	for deviceName, typeSlotMap := range res.GetNameTypeMap() {
		if !strings.Contains(strings.ToLower(deviceName), "gpu") {
			continue
		}

		for typeName, slots := range typeSlotMap.TypeSlotsMap {
			log.Infof("Device type: %s", typeName)

			for _, slot := range slots.Slots {
				matches := gpuConfig.pattern.FindStringSubmatch(slot)
				if len(matches) != 2 {
					log.Errorf("Invalid %s GPU device path format: %s", gpuType, slot)
					continue
				}

				deviceNum, err := strconv.Atoi(matches[1])
				if err != nil {
					log.Errorf("Failed to parse GPU number: %v", err)
					continue
				}

				if gpuConfig.validRange(deviceNum) {
					log.Infof("Bound %s GPU device number: %d", gpuType, deviceNum)
					boundGPUs = append(boundGPUs, deviceNum)
				} else {
					log.Warnf("Invalid %s GPU device number: %d", gpuType, deviceNum)
				}
			}
		}
	}

	return boundGPUs
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
