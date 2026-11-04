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

package ccancel

import (
	"CraneFrontEnd/internal/util"
	"regexp"

	"github.com/spf13/cobra"
)

var (
	FlagJobName        string
	FlagPartition      string
	FlagState          string
	FlagAccount        string
	FlagUserName       string
	FlagNodes          []string
	FlagConfigFilePath string
	FlagJson           bool

	RootCmd = &cobra.Command{
		Use:           "ccancel [flags] JOBID.STEPID[,JOBID.STEPID...]",
		Short:         "Cancel pending, running or suspended jobs/steps",
		Long:          "",
		Version:       util.Version(),
		SilenceErrors: true,
		SilenceUsage:  true,
		Args: func(cmd *cobra.Command, args []string) error {
			err := cobra.MaximumNArgs(1)(cmd, args)
			if err != nil {
				return err
			}

			if len(args) == 0 &&
				FlagJobName == "" &&
				FlagPartition == "" &&
				FlagState == "" &&
				FlagAccount == "" &&
				FlagUserName == "" &&
				FlagNodes == nil {
				return util.NewCraneErr(util.ErrorCmdArg, "at least one condition should be given.")
			}

			if len(args) > 0 {
				matched, _ := regexp.MatchString(`^([1-9][0-9]*(_[0-9]+)?(\.[1-9][0-9]*)?)(,[1-9][0-9]*(_[0-9]+)?(\.[1-9][0-9]*)?)*$`, args[0])
				if !matched {
					return util.NewCraneErr(util.ErrorCmdArg, "job id list must follow the format "+
						"<job_id>[_<array_task_id>][.<step_id>] or comma-separated list")
				}
			}

			return nil
		},
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return CancelJob(args)
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
	RootCmd.Flags().StringVarP(&FlagJobName, "name", "n", "",
		"Cancel jobs with the specified job name")
	RootCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "",
		"Cancel jobs in the specified partition")
	RootCmd.Flags().StringVarP(&FlagState, "state", "t", "",
		"Cancel jobs of the specified states"+
			"Valid job states are PENDING(P), RUNNING(R), SUSPENDED(S), ALL. "+
			"job states are case-insensitive")
	RootCmd.Flags().StringVarP(&FlagAccount, "account", "A", "",
		"Cancel jobs under the specified account")
	RootCmd.Flags().StringVarP(&FlagUserName, "user", "u", "",
		"Cancel jobs submitted by the specified user")
	RootCmd.Flags().StringSliceVarP(&FlagNodes, "nodes", "w", nil,
		"Cancel jobs running or suspended on the specified nodes")
	RootCmd.Flags().BoolVar(&FlagJson, "json", false,
		"Output in JSON format")
	util.InitCraneLogger()
}
