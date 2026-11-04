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

package calloc

import (
	"CraneFrontEnd/internal/util"

	"github.com/spf13/cobra"
)

var (
	FlagNodes         uint32
	FlagCpuPerTask    float64
	FlagNtasks        uint32
	FlagNtasksPerNode uint32
	FlagTime          string
	FlagMem           string
	FlagPartition     string
	FlagJob           string
	FlagAccount       string
	FlagQos           string
	FlagCwd           string
	FlagGres          string
	FlagNodelist      string
	FlagExcludes      string
	FlagGetUserEnv    bool
	FlagExport        string
	FlagExclusive     bool
	FlagWckey         string
	FlagGpusPerNode   string
	FlagMemPerCpu     string

	FlagExtraAttr  string
	FlagMailType   string
	FlagMailUser   string
	FlagComment    string
	FlagDependency string
	FlagMpi        string

	FlagConfigFilePath string
	FlagDebugLevel     string

	FlagReservation string
	FlagSignal      string

	FlagHold         bool
	FlagQuiet        bool
	FlagDeadlineTime string

	FlagLicenses string
	// not implement feature:
	FlagNTasks  string
	FlagNoKill  bool
	FlagVerbose bool

	RootCmd = &cobra.Command{
		Use:           "calloc",
		Short:         "Allocate resource and create terminal",
		Version:       util.Version(),
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.ExactArgs(0),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return MainCalloc(cmd, args)
		},
	}
)

func ParseCmdArgs() {
	util.RunEWrapperForLeafCommand(RootCmd)
	util.RunAndHandleExit(RootCmd)
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath, "Path to configuration file")
	RootCmd.PersistentFlags().StringVarP(&FlagDebugLevel, "debug-level", "",
		"info", "Available debug level: trace, debug, info")
	RootCmd.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 0, "Number of nodes requested")
	RootCmd.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 1, "Number of cpus required per task")
	RootCmd.Flags().Uint32VarP(&FlagNtasks, "ntasks", "n", 0, "Total number of tasks")
	RootCmd.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 0, "Maximum number of tasks per node (default: unlimited)")
	RootCmd.Flags().StringVarP(&FlagTime, "time", "t", "", "Time limit, format: \"day-hours:minutes:seconds\" 5-0:0:1 for 5 days, 1 second or \"hours:minutes:seconds\" 10:1:2 for 10 hours, 1 minute, 2 seconds")
	RootCmd.Flags().StringVar(&FlagMem, "mem", "", "Maximum amount of real memory, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB")
	RootCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "Partition requested")
	RootCmd.Flags().StringVarP(&FlagJob, "job-name", "J", "", "Name of job")
	RootCmd.Flags().StringVarP(&FlagAccount, "account", "A", "", "Account used for the job")
	RootCmd.Flags().StringVarP(&FlagCwd, "chdir", "D", "", "Working directory of the job")
	RootCmd.Flags().StringVarP(&FlagQos, "qos", "q", "", "QoS used for the job")
	RootCmd.Flags().StringVar(&FlagGres, "gres", "", "Gres required per node, format: \"gpu:a100:1\" or \"gpu:1\"")
	RootCmd.Flags().StringVarP(&FlagNodelist, "nodelist", "w", "", "Nodes to be allocated to the job (comma-separated list or hostlist expression)")
	RootCmd.Flags().StringVarP(&FlagExcludes, "exclude", "x", "", "Exclude specific nodes from allocating (comma-separated list or hostlist expression)")
	RootCmd.Flags().BoolVar(&FlagGetUserEnv, "get-user-env", false, "Load login environment variables of the user")
	RootCmd.Flags().StringVar(&FlagExport, "export", "", "Propagate environment variables")
	RootCmd.Flags().StringVar(&FlagExtraAttr, "extra-attr", "", "Extra attributes of the job (in JSON format)")
	RootCmd.Flags().StringVar(&FlagMailType, "mail-type", "", "Notify user by mail when certain events occur, supported values: NONE, BEGIN, END, FAIL, TIMELIMIT, ALL (default is NONE)")
	RootCmd.Flags().StringVar(&FlagMailUser, "mail-user", "", "Mail address of the notification receiver")
	RootCmd.Flags().StringVar(&FlagComment, "comment", "", "Comment of the job")
	RootCmd.Flags().StringVarP(&FlagReservation, "reservation", "r", "", "Use reserved resources")
	RootCmd.Flags().BoolVar(&FlagExclusive, "exclusive", false, "Exclusive node resources")
	RootCmd.Flags().BoolVarP(&FlagHold, "hold", "H", false, "Hold the job until it is released")
	RootCmd.Flags().StringVar(&FlagDeadlineTime, "deadline", "", "Cancel the job if it reaches its deadline.")
	RootCmd.Flags().StringVarP(&FlagLicenses, "licenses", "L", "", "Licenses used for the job")
	RootCmd.Flags().StringVar(&FlagWckey, "wckey", "", "Wckey of the job")
	RootCmd.Flags().StringVar(&FlagGpusPerNode, "gpus-per-node", "", "Gpus required per node, format: [type:]<number>[,[type:]<number>...]. eg: \"4\" or \"a100:1,volta:1\"")
	RootCmd.Flags().StringVarP(&FlagMemPerCpu, "mem-per-cpu", "", "", "Maximum amount of real memory per CPU, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB")
	RootCmd.MarkFlagsMutuallyExclusive("mem", "mem-per-cpu")
	RootCmd.Flags().BoolVarP(&FlagQuiet, "quiet", "Q", false, "Quiet mode (suppress informational messages)")
	RootCmd.Flags().StringVarP(&FlagDependency, "dependency", "d", "", "Conditions for job to execute")
	RootCmd.Flags().StringVarP(&FlagSignal, "signal", "s", "", "Send signal when time limit within time seconds, format: [{R}:]<sig_num>[@sig_time]")
	RootCmd.Flags().BoolVarP(&FlagNoKill, "no-kill", "k", false, "")
	RootCmd.Flags().BoolVar(&FlagVerbose, "verbose", false, "")
}
