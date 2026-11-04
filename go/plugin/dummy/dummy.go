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

// This is a dummy plugin for testing and demonstration purposes.
package main

import (
	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"

	log "github.com/sirupsen/logrus"
)

// Compile-time check to ensure DummyPlugin implements required interfaces
var _ api.Plugin = DummyPlugin{}
var _ api.JobLifecycleHooks = DummyPlugin{}
var _ api.CgroupLifecycleHooks = DummyPlugin{}
var _ api.NodeEventHooks = DummyPlugin{}

// PluginD will call plugin's method thru this variable
var PluginInstance = DummyPlugin{}

type DummyPlugin struct{}

func (dp DummyPlugin) Name() string {
	return "Dummy"
}

func (dp DummyPlugin) Version() string {
	return "v0.0.1"
}

func (dp DummyPlugin) Load(meta api.PluginMeta) error {
	log.Infof("Dummy plugin is loaded.")
	log.Tracef("Metadata: %v", meta)
	return nil
}

func (dp DummyPlugin) Unload(meta api.PluginMeta) error {
	log.Infof("Dummy plugin is unloaded.")
	log.Tracef("Metadata: %v", meta)
	return nil
}

func (dp DummyPlugin) StartHook(ctx *api.PluginContext) {
	log.Infoln("StartHook is called!")

	req, ok := ctx.Request().(*protos.StartHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected StartHookRequest.")
		return
	}

	log.Tracef("StartHookReq: \n%v", req.String())
}

func (dp DummyPlugin) EndHook(ctx *api.PluginContext) {
	log.Infoln("EndHook is called!")

	req, ok := ctx.Request().(*protos.EndHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected EndHookRequest.")
		return
	}

	log.Tracef("EndHookReq: \n%v", req.String())
}

func (dp DummyPlugin) CreateCgroupHook(ctx *api.PluginContext) {
	log.Infoln("CreateCgroupHook is called!")

	req, ok := ctx.Request().(*protos.CreateCgroupHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected CreateCgroupHookRequest.")
		return
	}

	log.Tracef("CreateCgroupHookReq: \n%v", req.String())
}

func (dp DummyPlugin) DestroyCgroupHook(ctx *api.PluginContext) {
	log.Infoln("DestroyCgroupHook is called!")

	req, ok := ctx.Request().(*protos.DestroyCgroupHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected DestroyCgroupHookRequest.")
		return
	}

	log.Tracef("DestroyCgroupHookReq: \n%v", req.String())
}

func (dp DummyPlugin) NodeEventHook(ctx *api.PluginContext) {
	log.Infoln("NodeEventHook is called!")

	req, ok := ctx.Request().(*protos.NodeEventHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected NodeEventHook.")
		return
	}

	log.Tracef("InsetEventHookReq: \n%v", req.String())
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
