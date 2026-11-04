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
	"CraneFrontEnd/internal/util"
	"fmt"
	"path/filepath"
	"plugin"

	log "github.com/sirupsen/logrus"
)

type PluginLoaded struct {
	api.Plugin

	Meta api.PluginMeta
}

var (
	gPluginConfig util.PluginConfig
	gPluginMap    map[string]*PluginLoaded
	// As gPluginMap is only write once, it's safe to read it without lock
)

func ParsePluginConfig(basedir string, path string) error {
	config, err := util.ParsePluginConfig(path)
	if err != nil {
		return err
	}

	gPluginConfig = *config
	if gPluginConfig.LogLevel == "" {
		gPluginConfig.LogLevel = "info"
	}

	if gPluginConfig.SockPath == "" {
		gPluginConfig.SockPath = filepath.Join(basedir, util.DefaultPlugindSocketPath)
	} else {
		gPluginConfig.SockPath = filepath.Join(basedir, gPluginConfig.SockPath)
	}

	return nil
}

func LoadPluginsByConfig(pl []api.PluginMeta) error {
	gPluginMap = make(map[string]*PluginLoaded)

	for _, p := range pl {
		log.Infof("Loading plugin %s from %s", p.Name, p.Path)

		// Load by path
		plg, err := plugin.Open(p.Path)
		if err != nil {
			log.Warn(err)
			continue
		}

		// Search for variable
		v, err := plg.Lookup("PluginInstance")
		if err != nil {
			return err
		}

		castV, ok := v.(api.Plugin)
		if !ok {
			return fmt.Errorf("failed to cast plugin instance %v", p.Name)
		}

		gPluginMap[p.Name] = &PluginLoaded{
			Plugin: castV,
			Meta:   p,
		}
	}

	return nil
}

func UnloadPlugins() error {
	var errs []error
	for _, p := range gPluginMap {
		if err := (*p).Unload(p.Meta); err != nil {
			errs = append(errs, fmt.Errorf("(%s: %v)", p.Meta.Name, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%v", errs)
	}

	return nil
}
