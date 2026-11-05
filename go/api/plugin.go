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

package api

import "google.golang.org/grpc"

type HookType uint8

const (
	StartHook HookType = iota
	EndHook
	CreateCgroupHook
	DestroyCgroupHook
	NodeEventHook

	UpdatePowerStateHook
	RegisterCranedHook
	UpdateLicensesHook
	TraceHook
)

type PluginHandler func(*PluginContext)

type PluginMeta struct {
	Name   string `yaml:"Name"`
	Path   string `yaml:"Path"`
	Config string `yaml:"Config"`
}

// A plugin is a shared object that implements the Plugin interface
type Plugin interface {
	// Get the plugin name, should be consistent with the config file
	Name() string

	// Get the plugin version, could be used for simple version control
	Version() string

	// Load the plugin with the metadata, e.g., read the config file
	Load(meta PluginMeta) error

	// Unload the plugin, e.g., close the file descriptor
	Unload(meta PluginMeta) error
}

// Hook interfaces: plugins can optionally implement these interfaces
// to subscribe to specific hook events.

// JobLifecycleHooks handles job start and end events
type JobLifecycleHooks interface {
	/*
		Hook processing functions:
			@param ctx: The context of the plugin. Request and other data could
						be accessed from the context.
			            The output of the plugin should be stored in the context.
		See PluginContext for details.
	*/
	StartHook(ctx *PluginContext)
	EndHook(ctx *PluginContext)
}

// CgroupLifecycleHooks handles cgroup creation and destruction events
type CgroupLifecycleHooks interface {
	CreateCgroupHook(ctx *PluginContext)
	DestroyCgroupHook(ctx *PluginContext)
}

// NodeEventHooks handles node-related events
type NodeEventHooks interface {
	NodeEventHook(ctx *PluginContext)
}

// PowerManagementHooks handles power state changes
type PowerManagementHooks interface {
	UpdatePowerStateHook(ctx *PluginContext)
}

// CranedLifecycleHooks handles craned daemon lifecycle events
type CranedLifecycleHooks interface {
	RegisterCranedHook(ctx *PluginContext)
}

// GrpcServiceRegistrar allows a plugin to register its own gRPC services
// onto the shared plugin daemon server instance.
type GrpcServiceRegistrar interface {
	RegisterGrpcServices(server grpc.ServiceRegistrar) error
}

// HostConfigAware allows a plugin to receive the absolute path to the
// CraneSched host configuration file before initialization.
type HostConfigAware interface {
	SetHostConfigPath(path string)
}

// ResourceHooks handles ctld virtual resource change
type ResourceHooks interface {
	UpdateLicensesHook(ctx *PluginContext)
}

// TraceHooks handles trace events
type TraceHooks interface {
	TraceHook(ctx *PluginContext)
}
