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
	"runtime"

	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"

	nested "github.com/antonfisher/nested-logrus-formatter"
	logrus "github.com/sirupsen/logrus"
)

var log = logrus.WithField("component", "TracePlugin")

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

var _ api.Plugin = TracePlugin{}
var _ api.TraceHooks = TracePlugin{}

var PluginInstance = TracePlugin{}

type GlobalTrace struct {
	config *Config
	store  TraceStore
	writer *TraceWriter
}

var globalTrace GlobalTrace

type TracePlugin struct{}

func (p TracePlugin) Name() string {
	return "trace"
}

func (p TracePlugin) Version() string {
	return "1.0.0"
}

func (p TracePlugin) Load(meta api.PluginMeta) error {
	log.Info("Initializing trace plugin")

	cfg, err := LoadConfig(meta.Config)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if err := setupLogging(cfg.Tracing.LogPath); err != nil {
		log.Warnf("Failed to setup logging: %v, using default stderr", err)
	}

	store, err := NewTraceStore(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize trace store: %w", err)
	}

	globalTrace.config = cfg
	globalTrace.store = store
	globalTrace.writer = NewTraceWriter(store, cfg.DB.TraceWriter)
	PrintConfig(cfg)

	log.Info("Trace plugin initialized successfully")
	return nil
}

func (p TracePlugin) Unload(meta api.PluginMeta) error {
	log.Info("Unloading trace plugin")

	if globalTrace.writer != nil {
		globalTrace.writer.Close()
		globalTrace.writer = nil
	}

	if globalTrace.store != nil {
		if err := globalTrace.store.Close(); err != nil {
			return fmt.Errorf("error closing trace store: %w", err)
		}
		globalTrace.store = nil
	}

	globalTrace = GlobalTrace{}

	log.Info("Trace plugin gracefully unloaded")
	return nil
}

func (p TracePlugin) TraceHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.TraceHookRequest)
	if !ok {
		log.Error("Invalid request type, expected TraceHookRequest")
		return
	}

	if globalTrace.writer == nil {
		log.Error("Trace writer is not initialized")
		return
	}

	if err := globalTrace.writer.Enqueue(ctx.GrpcCtx, req.GetSpans()); err != nil {
		log.Errorf("Failed to enqueue trace spans: %v", err)
	}
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

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
