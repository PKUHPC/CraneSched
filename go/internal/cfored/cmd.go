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

package cfored

import (
	"CraneFrontEnd/internal/util"
	"os"
	"strconv"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath string
	FlagDebugLevel     string
	FlagHostname       string
	FlagReload         bool
)

func ParseCmdArgs() {
	rootCmd := &cobra.Command{
		Use:     "cfored",
		Short:   "Daemon for interactive job management",
		Version: util.Version(),
		Run: func(cmd *cobra.Command, args []string) {
			if FlagReload {
				SendReloadSignal()
				return
			}
			StartCfored(cmd)
		},
	}

	rootCmd.SetVersionTemplate(util.VersionTemplate())
	rootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	rootCmd.PersistentFlags().StringVarP(&FlagDebugLevel, "debug-level", "D",
		"info", "Available debug level: trace,debug,info")
	rootCmd.PersistentFlags().StringVar(&FlagHostname, "hostname", "",
		"Hostname used to register cfored")
	rootCmd.PersistentFlags().BoolVar(&FlagReload, "reload", false, "Reload configuration")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
	}
}

func SendReloadSignal() {
	config := util.ParseConfig(FlagConfigFilePath)
	pidFilePath := config.Cfored.PidFilePath
	if pidFilePath == "" {
		log.Errorf("Pid file path is not configured")
		os.Exit(1)
	}

	out, err := os.ReadFile(pidFilePath)
	if err != nil {
		log.Errorf("Failed to read pid file %s. reason: %v", pidFilePath, err)
		os.Exit(1)
	}

	pidStr := strings.TrimSpace(string(out))
	if pidStr == "" {
		log.Errorf("The pid stored in %s is empty", pidFilePath)
		os.Exit(1)
	}

	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		log.Errorf("Unable to parse pid_string %q. reason: %v", pidStr, err)
		os.Exit(1)
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		log.Errorf("Unable to find process: %d", pid)
		os.Exit(1)
	}

	if err = process.Signal(syscall.Signal(0)); err != nil {
		log.Errorf("Cfored process %d in pid file is not running: %v", pid, err)
		os.Exit(1)
	}

	if err = process.Signal(gReloadSignal); err != nil {
		log.Errorf("Fail when sending signal to the process. reason: %v", err)
		os.Exit(1)
	}
}
