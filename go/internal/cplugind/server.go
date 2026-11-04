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

package cplugind

import (
	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"errors"
	"fmt"
	"net"
	"os"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type PluginDaemon struct {
	protos.UnimplementedCranePluginDServer
	Server *grpc.Server
}

func NewPluginD(opts []grpc.ServerOption) *PluginDaemon {
	p := &PluginDaemon{
		Server: grpc.NewServer(opts...),
	}

	protos.RegisterCranePluginDServer(p.Server, p)

	return p
}

func (pd *PluginDaemon) Launch(listeners ...net.Listener) error {
	if len(listeners) == 0 {
		return fmt.Errorf("no listeners provided")
	}

	for _, listener := range listeners {
		go func(l net.Listener) {
			if err := pd.Server.Serve(l); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				log.Errorf("Failed to serve on %s: %v", l.Addr(), err)
				os.Exit(util.ErrorGeneric)
			}
		}(listener)
	}

	return nil
}

func (pd *PluginDaemon) Stop() {
	pd.Server.Stop()
}

func (pd *PluginDaemon) GracefulStop() {
	pd.Server.GracefulStop()
}

func (pd *PluginDaemon) StartHook(ctx context.Context, req *protos.StartHookRequest) (*protos.StartHookReply, error) {
	log.Tracef("StartHook request received: %v", req)
	reply := &protos.StartHookReply{}
	hs := make([]api.PluginHandler, 0)

	// Only call plugins that implement JobLifecycleHooks
	for _, p := range gPluginMap {
		if handler, ok := p.Plugin.(api.JobLifecycleHooks); ok {
			hs = append(hs, handler.StartHook)
		}
	}

	c := api.NewContext(ctx, req, api.StartHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) EndHook(ctx context.Context, req *protos.EndHookRequest) (*protos.EndHookReply, error) {
	log.Tracef("EndHook request received: %v", req)
	reply := &protos.EndHookReply{}
	hs := make([]api.PluginHandler, 0)

	// Only call plugins that implement JobLifecycleHooks
	for _, p := range gPluginMap {
		if handler, ok := p.Plugin.(api.JobLifecycleHooks); ok {
			hs = append(hs, handler.EndHook)
		}
	}

	c := api.NewContext(ctx, req, api.EndHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) CreateCgroupHook(ctx context.Context, req *protos.CreateCgroupHookRequest) (*protos.CreateCgroupHookReply, error) {
	log.Tracef("CreateCgroupHook request received: %v", req)
	reply := &protos.CreateCgroupHookReply{}
	hs := make([]api.PluginHandler, 0)

	// Only call plugins that implement CgroupLifecycleHooks
	for _, p := range gPluginMap {
		if handler, ok := p.Plugin.(api.CgroupLifecycleHooks); ok {
			hs = append(hs, handler.CreateCgroupHook)
		}
	}

	c := api.NewContext(ctx, req, api.CreateCgroupHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) DestroyCgroupHook(ctx context.Context, req *protos.DestroyCgroupHookRequest) (*protos.DestroyCgroupHookReply, error) {
	log.Tracef("DestroyCgroupHook request received: %v", req)
	reply := &protos.DestroyCgroupHookReply{}
	hs := make([]api.PluginHandler, 0)

	// Only call plugins that implement CgroupLifecycleHooks
	for _, p := range gPluginMap {
		if handler, ok := p.Plugin.(api.CgroupLifecycleHooks); ok {
			hs = append(hs, handler.DestroyCgroupHook)
		}
	}

	c := api.NewContext(ctx, req, api.DestroyCgroupHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) NodeEventHook(ctx context.Context, req *protos.NodeEventHookRequest) (*protos.NodeEventHookReply, error) {
	log.Tracef("NodeEventHook request received: %v", req)
	reply := &protos.NodeEventHookReply{}
	hs := make([]api.PluginHandler, 0)

	// Only call plugins that implement NodeEventHooks
	for _, p := range gPluginMap {
		if handler, ok := p.Plugin.(api.NodeEventHooks); ok {
			hs = append(hs, handler.NodeEventHook)
		}
	}

	c := api.NewContext(ctx, req, api.NodeEventHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) UpdatePowerStateHook(ctx context.Context, req *protos.UpdatePowerStateHookRequest) (*protos.UpdatePowerStateHookReply, error) {
	log.Info("Received UpdatePowerStateHook request for node: ", req.CranedId)
	reply := &protos.UpdatePowerStateHookReply{}
	hs := make([]api.PluginHandler, 0)

	// Only call plugins that implement PowerManagementHooks
	for _, p := range gPluginMap {
		if handler, ok := p.Plugin.(api.PowerManagementHooks); ok {
			hs = append(hs, handler.UpdatePowerStateHook)
		}
	}

	c := api.NewContext(ctx, req, api.UpdatePowerStateHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) RegisterCranedHook(ctx context.Context, req *protos.RegisterCranedHookRequest) (*protos.RegisterCranedHookReply, error) {
	log.Info("Received RegisterCranedHook request for node: ", req.CranedId)
	reply := &protos.RegisterCranedHookReply{}
	hs := make([]api.PluginHandler, 0)

	// Only call plugins that implement CranedLifecycleHooks
	for _, p := range gPluginMap {
		if handler, ok := p.Plugin.(api.CranedLifecycleHooks); ok {
			hs = append(hs, handler.RegisterCranedHook)
		}
	}

	c := api.NewContext(ctx, req, api.RegisterCranedHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) UpdateLicensesHook(ctx context.Context, req *protos.UpdateLicensesHookRequest) (*protos.UpdateLicensesHookReply, error) {
	log.Tracef("UpdateLicensesHook request received")
	reply := &protos.UpdateLicensesHookReply{}
	hs := make([]api.PluginHandler, 0)

	for _, p := range gPluginMap {
		if handler, ok := p.Plugin.(api.ResourceHooks); ok {
			hs = append(hs, handler.UpdateLicensesHook)
		}
	}

	c := api.NewContext(ctx, req, api.UpdateLicensesHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) TraceHook(ctx context.Context, req *protos.TraceHookRequest) (*protos.TraceHookReply, error) {
	// log.Tracef("TraceHook request received")
	reply := &protos.TraceHookReply{}
	hs := make([]api.PluginHandler, 0)

	for _, p := range gPluginMap {
		if handler, ok := p.Plugin.(api.TraceHooks); ok {
			hs = append(hs, handler.TraceHook)
		}
	}

	c := api.NewContext(ctx, req, api.TraceHook, &hs)
	c.Start()

	return reply, nil
}
