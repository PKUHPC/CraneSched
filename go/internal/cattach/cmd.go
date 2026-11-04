/**
 * Copyright (c) 2026 Peking University and Peking University
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

package cattach

import (
	"CraneFrontEnd/internal/util"

	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath string
	FlagPty            bool
	FlagErrorFilter    int32 // -1 means "not set" (show all); >= 0 selects a specific task id
	FlagInputFilter    int32 // -1 means "not set" (broadcast); >= 0 routes stdin to a specific task id
	FlagOutputFilter   int32 // -1 means "not set" (show all); >= 0 selects a specific task id
	FlagLabel          bool
	FlagLayout         bool
	FlagQuiet          bool
	// FlagReadOnly is set automatically when the attached step has exclusive stdin routing
	// (i.e., crun was started with --input=<task_id>).  In read-only mode cattach displays
	// output but does not forward any stdin to the running tasks.
	FlagReadOnly bool
	RootCmd      = &cobra.Command{
		Use:           "cattach [flags] jobid.stepid",
		Short:         "Attach to a crane job step",
		Version:       util.Version(),
		SilenceErrors: true,
		SilenceUsage:  true,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return MainCattach(args)
		},
	}
)

func ParseCmdArgs() {
	util.RunEWrapperForLeafCommand(RootCmd)
	util.RunAndHandleExit(RootCmd)
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath, "Path to configuration file")
	RootCmd.PersistentFlags().Int32Var(&FlagErrorFilter, "error-filter", -1, "only print stderr from the specified task (0-based task id, default: show all)")
	RootCmd.PersistentFlags().Int32Var(&FlagOutputFilter, "output-filter", -1, "only print stdout from the specified task (0-based task id, default: show all)")
	RootCmd.PersistentFlags().Int32Var(&FlagInputFilter, "input-filter", -1, "send stdin to only the specified task (0-based task id, default: broadcast to all)")
	RootCmd.PersistentFlags().BoolVar(&FlagLabel, "label", false, "prepend task number to lines of stdout & stderr")
	RootCmd.PersistentFlags().BoolVar(&FlagLayout, "layout", false, "print task layout info and exit (does not attach to tasks)")
	RootCmd.PersistentFlags().BoolVar(&FlagQuiet, "quiet", false, "quiet mode (suppress informational messages)")
	util.InitCraneLogger()
}
