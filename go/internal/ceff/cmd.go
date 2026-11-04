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

package ceff

import (
	"CraneFrontEnd/internal/util"
	"fmt"

	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath       string
	FlagPluginConfigFilePath string
	FlagJson                 bool

	RootCmd = &cobra.Command{
		Use:           "ceff [flags] [job_id, ...]",
		Short:         "Display the status and details of the job",
		Long:          "",
		Version:       util.Version(),
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.MaximumNArgs(1),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			util.DetectNetworkProxy()
			config := util.ParseConfig(FlagConfigFilePath)
			pluginConfig, err := util.ParsePluginConfig(FlagPluginConfigFilePath)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg,
					fmt.Sprintf("Failed to parse plugin config from %s: %v", FlagPluginConfigFilePath, err))
			}
			stub = util.GetStubToCtldByConfig(config)
			client, conn, err := GetPlugindClient(config, pluginConfig)
			if err != nil {
				return err
			}
			pluginClient = client
			pluginConn = conn
			return nil
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			CleanupPlugindClient()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			jobIds := ""
			if len(args) == 0 {
				jobIds = ""
			} else {
				jobIds = args[0]
			}
			return QueryJobsInfoByIds(jobIds)
		},
	}
)

func ParseCmdArgs() {
	util.RunEWrapperForLeafCommand(RootCmd)
	util.RunAndHandleExit(RootCmd)
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.PersistentFlags().StringVarP(&FlagPluginConfigFilePath, "plugin-config", "p",
		util.DefaultPluginConfigPath, "Path to plugin configuration file")
	RootCmd.PersistentFlags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
	util.InitCraneLogger()
}
