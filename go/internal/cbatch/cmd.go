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

package cbatch

import (
	"CraneFrontEnd/internal/util"
	"errors"
	"os"
	"strings"

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
	FlagLicenses      string
	FlagCwd           string
	FlagRepeat        uint32
	FlagArray         string
	FlagNodelist      string
	FlagExcludes      string
	FlagGres          string
	FlagGetUserEnv    bool
	FlagExport        string
	FlagStdinPath     string
	FlagStdoutPath    string
	FlagStderrPath    string
	FlagOpenMode      string
	FlagExclusive     bool
	FlagWckey         string
	FlagGpusPerNode   string
	FlagMemPerCpu     string

	FlagInterpreter   string
	FlagWrappedScript string

	FlagExtraAttr string
	FlagMailType  string
	FlagMailUser  string
	FlagComment   string

	FlagConfigFilePath string
	FlagJson           bool
	FlagDependency     string

	FlagReservation string
	FlagSignal      string

	FlagHold         bool
	FlagBeginTime    string
	FlagDns          []string
	FlagDeadlineTime string

	FlagRequeue   bool
	FlagNoRequeue bool
	FlagParsable  bool

	// not implement feature:
	FlagNTasks          string
	FlagVerbose         bool
	FlagNTasksPerSocket string
	FlagCpuFreq         string
	FlagPriority        string
	FlagThreadsPerCore  string
	FlagDistribution    string
	FlagInput           string
	FlagSocketsPerNode  string
	FlagCoresPerSocket  string
	FlagWait            string

	RootCmd = &cobra.Command{
		Use:           "cbatch [flags] file",
		Short:         "Submit batch job",
		Version:       util.Version(),
		SilenceErrors: true,
		SilenceUsage:  true,
		Args: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("wrap") {
				if len(args) != 0 {
					return errors.New("--wrap is exclusive with file name argument")
				}
			} else if len(args) != 1 {
				return errors.New("invalid number of arguments")
			}
			return nil
		},
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if FlagRepeat == 0 {
				return util.NewCraneErr(util.ErrorCmdArg, "--repeat should be greater than 0")
			}

			config := util.ParseConfig(FlagConfigFilePath)
			job, err := BuildCbatchJob(cmd, args, config)
			if err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%v", err)
			}
			if job.ArraySpec != nil && FlagRepeat != 1 {
				return util.NewCraneErr(util.ErrorCmdArg, "--repeat is incompatible with --array")
			}

			job.CmdLine = strings.Join(os.Args, " ")

			// Process the content of --get-user-env
			util.SetPropagatedEnviron(&job.Env, &job.GetUserEnv)

			if job.Cwd == "" {
				job.Cwd, _ = os.Getwd()
			}
			job.SubmitDir, err = os.Getwd()
			if err != nil {
				return util.WrapCraneErr(util.ErrorSystem, "Get submit dir err: %s.", err)
			}

			if FlagRepeat == 1 {
				return SendRequest(config, job)
			} else {
				return SendMultipleRequests(config, job, FlagRepeat)
			}
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
	RootCmd.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 0, "Number of nodes requested")
	RootCmd.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 1, "Number of cpus required per task")
	RootCmd.Flags().StringVar(&FlagGres, "gres", "", "Gres required per node, format: \"gpu:a100:1\" or \"gpu:1\"")
	RootCmd.Flags().Uint32VarP(&FlagNtasks, "ntasks", "n", 0, "Total number of tasks")
	RootCmd.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 0, "Maximum number of tasks per node (default: unlimited)")
	RootCmd.Flags().StringVarP(&FlagTime, "time", "t", "", "Time limit, format: \"day-hours:minutes:seconds\" 5-0:0:1 for 5 days, 1 second or \"hours:minutes:seconds\" 10:1:2 for 10 hours, 1 minute, 2 seconds")
	RootCmd.Flags().StringVar(&FlagMem, "mem", "", "Maximum amount of real memory, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB")
	RootCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "Partition requested")
	RootCmd.Flags().StringVarP(&FlagJob, "job-name", "J", "", "Name of job")
	RootCmd.Flags().StringVarP(&FlagAccount, "account", "A", "", "Account used for the job")
	RootCmd.Flags().StringVarP(&FlagCwd, "chdir", "D", "", "Working directory of the job")
	RootCmd.Flags().StringVarP(&FlagQos, "qos", "q", "", "QoS used for the job")
	RootCmd.Flags().StringVarP(&FlagLicenses, "licenses", "L", "", "Licenses used for the job")
	RootCmd.Flags().Uint32Var(&FlagRepeat, "repeat", 1, "Submit the job multiple times")
	RootCmd.Flags().StringVarP(&FlagArray, "array", "a", "", "Submit an array job using index range start-end")
	RootCmd.Flags().StringVarP(&FlagNodelist, "nodelist", "w", "", "Nodes to be allocated to the job (comma-separated list or hostlist expression)")
	RootCmd.Flags().StringVarP(&FlagExcludes, "exclude", "x", "", "Exclude specific nodes from allocating (comma-separated list or hostlist expression)")
	RootCmd.Flags().BoolVar(&FlagGetUserEnv, "get-user-env", false, "Load login environment variables of the user")
	RootCmd.Flags().StringVar(&FlagExport, "export", "", "Propagate environment variables")
	RootCmd.Flags().StringVarP(&FlagStdinPath, "input", "i", "", "Redirection path of standard input of the script")
	RootCmd.Flags().StringVarP(&FlagStdoutPath, "output", "o", "", "Redirection path of standard output of the script")
	RootCmd.Flags().StringVarP(&FlagStderrPath, "error", "e", "", "Redirection path of standard error of the script")
	RootCmd.Flags().StringVar(&FlagWrappedScript, "wrap", "", "Wrap command string in a sh script and submit")
	RootCmd.Flags().StringVar(&FlagInterpreter, "interpreter", "", "Interpreter used to run the script")
	RootCmd.Flags().StringVar(&FlagExtraAttr, "extra-attr", "", "Extra attributes of the job (in JSON format)")
	RootCmd.Flags().StringVar(&FlagMailType, "mail-type", "", "Notify user by mail when certain events occur, supported values: NONE, BEGIN, END, FAIL, TIMELIMIT, ALL (default is NONE)")
	RootCmd.Flags().StringVar(&FlagMailUser, "mail-user", "", "Mail address of the notification receiver")
	RootCmd.Flags().StringVar(&FlagComment, "comment", "", "Comment of the job")
	RootCmd.Flags().StringVar(&FlagWckey, "wckey", "", "Wckey of the job")
	RootCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
	RootCmd.Flags().StringVar(&FlagOpenMode, "open-mode", "", "Set the mode for opening output and error files, supported values: append, truncate (default is truncate) ")
	RootCmd.Flags().StringVarP(&FlagReservation, "reservation", "r", "", "Use reserved resources")
	RootCmd.Flags().BoolVar(&FlagExclusive, "exclusive", false, "Exclusive node resources")
	RootCmd.Flags().BoolVarP(&FlagHold, "hold", "H", false, "Hold the job until it is released")
	RootCmd.Flags().StringVarP(&FlagBeginTime, "begin", "b", "", "Defer job until specified time.")
	RootCmd.Flags().StringVar(&FlagGpusPerNode, "gpus-per-node", "", "Gpus required per node, format: [type:]<number>[,[type:]<number>...]. eg: \"4\" or \"a100:1,volta:1\"")
	RootCmd.Flags().StringVarP(&FlagMemPerCpu, "mem-per-cpu", "", "", "Maximum amount of real memory per CPU, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB")
	RootCmd.MarkFlagsMutuallyExclusive("mem", "mem-per-cpu")
	RootCmd.MarkFlagsMutuallyExclusive("repeat", "array")
	RootCmd.Flags().StringVarP(&FlagDependency, "dependency", "d", "", "Conditions for job to execute")
	RootCmd.Flags().StringVarP(&FlagSignal, "signal", "s", "", "Send signal when time limit within time seconds, format: [{R|B}:]<sig_num>[@sig_time]")
	RootCmd.Flags().BoolVar(&FlagParsable, "parsable", false, "")
	RootCmd.Flags().BoolVar(&FlagVerbose, "verbose", false, "")
	RootCmd.Flags().StringVar(&FlagNTasksPerSocket, "ntasks-per-socket", "", "")
	RootCmd.Flags().StringVar(&FlagCpuFreq, "cpu-freq", "", "")
	RootCmd.Flags().StringVar(&FlagPriority, "priority", "", "")
	RootCmd.Flags().StringVar(&FlagThreadsPerCore, "threads-per-core", "", "")
	RootCmd.Flags().StringVar(&FlagDistribution, "distribution", "", "")
	RootCmd.Flags().StringVar(&FlagSocketsPerNode, "sockets-per-node", "", "")
	RootCmd.Flags().StringVar(&FlagCoresPerSocket, "cores-per-socket", "", "")
	RootCmd.Flags().StringVar(&FlagWait, "wait", "", "")
	RootCmd.Flags().StringVar(&FlagDeadlineTime, "deadline", "", "Remove the job if not started by time.")
	RootCmd.Flags().BoolVar(&FlagRequeue, "requeue", false, "Requeue the job on failure")
	RootCmd.Flags().BoolVar(&FlagNoRequeue, "no-requeue", false, "Do not requeue the job on failure")
	RootCmd.MarkFlagsMutuallyExclusive("requeue", "no-requeue")
	initPodFlags(RootCmd)
	util.InitCraneLogger()
}
