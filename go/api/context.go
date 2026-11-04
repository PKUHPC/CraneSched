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

// This file should be refactored for a better public/private separation,
// after we have a separate repo for plugind.

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"
)

type PluginContext struct {
	GrpcCtx context.Context
	Type    HookType
	Keys    map[string]any

	request  proto.Message
	index    uint8
	handlers []PluginHandler
	mu       sync.RWMutex
}

func (c *PluginContext) Set(key string, value any) {
	c.mu.Lock()
	c.Keys[key] = value
	c.mu.Unlock()
}

func (c *PluginContext) Get(key string) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Keys[key]
}

func (c *PluginContext) Request() proto.Message {
	return c.request
}

// This should only be called by the plugin daemon
func (c *PluginContext) Start() {
	c.index = 0
	for c.index < uint8(len(c.handlers)) {
		if c.handlers[c.index] == nil {
			// This shouldn't happen
			c.Abort()
			continue
		}
		c.handlers[c.index](c)
		c.index++
	}
}

// Plugin could call this to hand over the control to the next plugin.
// When this returned, the caller may continue.
func (c *PluginContext) Next() {
	c.index++
	for c.index < uint8(len(c.handlers)) {
		if c.handlers[c.index] == nil {
			// This shouldn't happen
			c.Abort()
			continue
		}
		c.handlers[c.index](c)
		c.index++
	}
}

// Plugin could call this to prevent the following plugins from being called.
func (c *PluginContext) Abort() {
	c.index = uint8(len(c.handlers))
}

func NewContext(ctx context.Context, req proto.Message, t HookType, hs *[]PluginHandler) *PluginContext {
	return &PluginContext{
		GrpcCtx:  ctx,
		Type:     t,
		Keys:     make(map[string]any),
		request:  req,
		index:    0,
		handlers: *hs,
		mu:       sync.RWMutex{},
	}
}
