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

package cacct

import (
	"CraneFrontEnd/internal/util"

	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath   string
	FlagFormat           string
	FlagFilterSubmitTime string
	FlagFilterStartTime  string
	FlagFilterEndTime    string
	FlagFilterAccounts   string
	FlagFilterJobIDs     string
	FlagFilterUsers      string
	FlagFilterJobNames   string
	FlagFilterStates     string
	FlagFilterPartitions string
	FlagFilterQos        string
	FlagFilterJobTypes   string
	FlagFilterNodeNames  string
	FlagNumLimit         uint32
	FlagNoHeader         bool
	FlagFull             bool
	FlagJson             bool
	FlagDeadlineTime     bool

	RootCmd = &cobra.Command{
		Use:           "cacct [flags]",
		Short:         "Display the recent job information",
		Version:       util.Version(),
		Long:          "",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.ExactArgs(0),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("max-lines") {
				if FlagNumLimit == 0 {
					return util.NewCraneErr(util.ErrorCmdArg, "Output line number limit must be greater than 0.")
				}
			}

			return QueryJob()
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
	RootCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
		"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
			"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or "+
			"semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)")
	RootCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
		"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
			"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or "+
			"semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)")
	RootCmd.Flags().StringVarP(&FlagFilterSubmitTime, "submit-time", "s",
		"", "Filter jobs with a submit time within a certain time period, which can use closed intervals"+
			"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or "+
			"semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)")
	RootCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
		"Select accounts to view (comma separated list)")
	RootCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
		"Select users to view (comma separated list)")
	RootCmd.Flags().StringVarP(&FlagFilterJobIDs, "job", "j", "",
		"Select job ids to view (comma separated list), default is all. Supports jobid, jobid.stepid, jobid_arraytaskid, and jobid_arraytaskid.stepid")
	RootCmd.Flags().StringVarP(&FlagFilterJobNames, "name", "n", "",
		"Select job names to view (comma separated list), default is all")
	RootCmd.Flags().BoolVarP(&FlagNoHeader, "noheader", "N", false,
		"Do not print header line in the output")
	RootCmd.Flags().StringVarP(&FlagFilterQos, "qos", "q", "",
		"Specify QoS of jobs to view (comma separated list), default is all.")
	RootCmd.Flags().StringVarP(&FlagFilterStates, "state", "t",
		"all", "Specify job states to view, supported states: "+
			"pending(p), running(r), completed(c), failed(f), cancelled(x), time-limit-exceeded(t), all.")
	RootCmd.Flags().StringVarP(&FlagFilterPartitions, "partition", "p", "",
		"Specify partitions to view (comma separated list), default is all")
	RootCmd.Flags().StringVar(&FlagFilterJobTypes, "type", "",
		"Specify job types to view (comma separated list), \nvalid values are 'Interactive', 'Batch', 'Container', default is all types")

	RootCmd.Flags().BoolVar(&FlagDeadlineTime, "deadline", false, "Specify the deadline to view")
	RootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "",
		`Specify the output format.

Fields are identified by a percent sign (%) followed by a character or string.
Format specification: %[[.]size]type
  - Without size: field uses natural width
  - With size only (%5j): field uses minimum width, left-aligned (padding on right)
  - With dot and size (%.5j): field uses minimum width, right-aligned (padding on left)

Supported format identifiers or string, string case insensitive:
	%a/%Account           - Display the account associated with the job.
	%ArrayJobId           - Display the array job id, empty for non-array jobs.
	%ArraySpec            - Display the array range specification, empty for non-array jobs.
	%ArrayTaskId          - Display the array task id, empty for non-array jobs and array parents.
	%C/%ReqCpus           - Display the number of requested CPUs, formatted to two decimal places
	%c/%AllocCpus         - Display the number of allocated CPUs, formatted to two decimal places.
	%D/%ElapsedTime       - Display the elapsed time from the start of the job.
	%deadline/%Deadline   - Display the deadline time of the job.
	%E/%EndTime           - Display the end time of the job.
	%e/%ExitCode          - Display the exit code of the job.
                             If the exit code is based on a specific base (e.g., kCraneExitCodeBase),
                             it formats as "0:<code>" or "<code>:0" based on the condition.
	%h/%Held              - Display the hold status of the job.
	%j/%JobID             - Display the raw job ID; a pending array parent uses jobid_[range]; steps use jobid.stepid.
	%K/%Wckey             - Display the wckey of the job.
	%k/%Comment           - Display the comment of the job.
	%L/%NodeList          - Display the list of nodes the job is running on.
	%l/%TimeLimit         - Display the time limit of the job.
	%M/%ReqMemPerNode     - Display the requested mem per node of the job.
	%m/%AllocMemPerNode   - Display the allocted mem per node of the job.
	%N/%NodeNum           - Display the node num of the job.
	%n/%JobName           - Display the name of the job.
	%P/%Partition         - Display the partition associated with the job.
	%p/%Priority          - Display the priority of the job.
	%q/%Qos               - Display the QoS of the job.
	%R/%Reason            - Display the reason of pending.
	%r/%ReqNodes          - Display the reqnodes of the job.
	%S/%StartTime         - Display the start time of the job.
	%s/%SubmitTime        - Display the submit time num of the job.
	%t/%State             - Display the state of the job.
	%T/%JobType           - Display the job type.
	%U/%UserName          - Display the username of the job.
	%u/%Uid               - Display the uid of the job.
	%x/%ExcludeNodes      - Display the excludenodes of the job.
	%X/%Exclusive         - Display the exclusive status of the job.

Examples:
  --format "%j %n %t"              # Natural width for all fields
  --format "%5j %20n %t"           # Left-aligned: JobID (min 5), JobName (min 20), State
  --format "%.5j %.20n %t"         # Right-aligned: JobID (min 5), JobName (min 20), State
  --format "ID:%8j | Name:%.15n"   # Mixed: left-aligned JobID, right-aligned JobName with prefix

Note: If the format is invalid or unrecognized, the program will terminate with an error message.
`)
	RootCmd.Flags().BoolVarP(&FlagFull, "full", "F", false, "Display full information (If not set, only display 30 characters per cell)")
	RootCmd.Flags().Uint32VarP(&FlagNumLimit, "max-lines", "m", util.DefaultCacctMaxLines,
		"Limit the number of jobs returned")
	RootCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
	RootCmd.Flags().StringVarP(&FlagFilterNodeNames, "nodelist", "w", "",
		"Specify node names to view (comma separated list or patterns like node[1-10]), default is all")
	util.InitCraneLogger()
}
