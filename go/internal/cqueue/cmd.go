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

package cqueue

import (
	"CraneFrontEnd/internal/util"
	"fmt"

	"github.com/spf13/cobra"
)

var DefaultStepIdFilter = "ALL"

var (
	FlagConfigFilePath   string
	FlagFull             bool
	FlagNoHeader         bool
	FlagStartTime        bool
	FlagDeadlineTime     bool
	FlagSelf             bool
	FlagFilterPartitions string
	FlagStep             bool
	FlagFilterStepIDs    string
	FlagFilterJobIDs     string
	FlagFilterJobNames   string
	FlagFilterQos        string
	FlagFilterStates     string
	FlagFilterUsers      string
	FlagFilterAccounts   string
	FlagFilterJobTypes   string
	FlagFilterLicenses   string
	FlagFormat           string
	FlagFilterNodeNames  string
	FlagIterate          uint64
	FlagNumLimit         uint32
	FlagJson             bool
	FlagCount            bool

	RootCmd = &cobra.Command{
		Use:           "cqueue [flags]",
		Short:         "Display the job information and queue status",
		Long:          "",
		Version:       util.Version(),
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.ExactArgs(0),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("max-lines") {
				if FlagNumLimit == 0 {
					return util.NewCraneErr(util.ErrorCmdArg, "Output line number limit must be greater than 0.")
				}
			}

			FlagStep = cmd.Flags().Changed("step")
			if FlagFilterStepIDs == DefaultStepIdFilter {
				FlagFilterStepIDs = ""
			}
			if FlagCount {
				if err := validateCountFlags(cmd); err != nil {
					return err
				}
			}
			if FlagIterate != 0 {
				return loopedQuery(FlagIterate)
			} else {
				return Query()
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
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath,
		"Path to configuration file")
	RootCmd.Flags().StringVarP(&FlagFilterStepIDs, "step", "s", "",
		"Specify step ids to view (comma separated list), default is all")
	RootCmd.Flags().Lookup("step").NoOptDefVal = DefaultStepIdFilter
	RootCmd.Flags().StringVarP(&FlagFilterJobIDs, "job", "j", "",
		"Specify job ids to view (comma separated list), default is all. Supports jobid or jobid_arraytaskid; active array parents are displayed as jobid_[range%maxConcurrent] when limited")
	RootCmd.Flags().StringVarP(&FlagFilterJobNames, "name", "n", "",
		"Specify job names to view (comma separated list), default is all")
	RootCmd.Flags().StringVarP(&FlagFilterQos, "qos", "q", "",
		"Specify QoS of jobs to view (comma separated list), \ndefault is all QoS")
	RootCmd.Flags().StringVarP(&FlagFilterStates, "state", "t", "all",
		"Specify job states to view. Valid value are 'pending(p)', 'running(r)' and 'all'.\n"+
			"By default, 'all' is specified and all pending and running jobs will be reported")
	RootCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
		"Specify users to view (comma separated list), default is all users")
	RootCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
		"Specify accounts to view (comma separated list), \ndefault is all accounts")
	RootCmd.Flags().StringVarP(&FlagFilterPartitions, "partition", "p", "",
		"Specify partitions to view (comma separated list), \ndefault is all partitions")
	RootCmd.Flags().StringVar(&FlagFilterJobTypes, "type", "",
		"Specify job types to view (comma separated list), \nvalid values are 'Interactive', 'Batch', 'Container', default is all types")
	RootCmd.Flags().StringVarP(&FlagFilterLicenses, "licenses", "L", "",
		"Specify licenses to view (comma separated list), default is all licenses")
	RootCmd.Flags().Uint64VarP(&FlagIterate, "iterate", "i", 0,
		"Display at specified intervals (seconds), default is 0 (no iteration)")
	RootCmd.Flags().BoolVarP(&FlagStartTime, "start", "S", false,
		"Display expected start time of pending jobs")
	RootCmd.Flags().BoolVar(&FlagDeadlineTime, "deadline", false, "Display the deadline time of jobs")
	RootCmd.Flags().BoolVarP(&FlagNoHeader, "noheader", "N", false,
		"Do not print header line in the output")
	RootCmd.Flags().BoolVar(&FlagSelf, "self", false,
		"Display only the jobs submitted by current user")
	RootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "",
		`Specify the output format.
	Fields are identified by a percent sign (%) followed by a character or string.
	Format specification: %[[.]size]type
	  - Without size: field uses natural width
	  - With size only (%5j): field uses minimum width, left-aligned (padding on right)
	  - With dot and size (%.5j): field uses minimum width, right-aligned (padding on left)

Supported format identifiers or string, string case insensitive:
	%a/%Account            - Display the account associated with the job/step.
	%C/%ReqCpus            - Display the cpus requested to the job. (For jobs only)
	%c/%AllocCpus          - Display the cpus allocated to the job. (For jobs only)
	%deadline/%Deadline    - Display the deadline time of jobs.
	%e/%ElapsedTime        - Display the elapsed time from the start of the job/step.
	%h/%Held               - Display the hold state of the job. (For jobs only)
	%i/%StepId             - Display the ID of the step (format: jobId.stepId). (For steps only)
                              Materialized array steps use jobId_arrayTaskId.stepId.
	%j/%JobID              - Display the logical job ID for the job/step.
                              Materialized array tasks use jobId_arrayTaskId; active array parents use jobId_[range%maxConcurrent] when limited.
	%k/%Comment            - Display the comment of the job. (For jobs only)
	%K/%Wckey              - Display the wckey of the job.
	%L/%NodeList           - Display the list of nodes the job/step is running on.
	%l/%TimeLimit          - Display the time limit for the job/step.
	%M/%ReqMemPerNode      - Display the requested mem per node of the job. (For jobs only)
	%m/%AllocMemPerNode    - Display the requested mem per node of the job. (For jobs only)
	%N/%NodeNum            - Display the number of nodes requested by the job/step.
	%n/%Name               - Display the name of the job/step.
	%o/%Command            - Display the command line of the job/step.
	%P/%Partition          - Display the partition the job/step is running in.
	%p/%Priority           - Display the priority of the job. (For jobs only)
	%Q/%ReqCpuPerNode      - Display the requested cpu per node of the job. (For jobs only)
	%q/%QoS                - Display the Quality of Service level for the job/step.
	%R/%Reason             - Display the reason of pending. (For jobs only)
	%r/%ReqNodes           - Display the reqnodes of the job. (For jobs only)
	%S/%StartTime          - Display the start time of the job. (For jobs only)
	%s/%SubmitTime         - Display the submission time of the job. (For jobs only)
	%t/%State              - Display the current state of the job/step.
	%T/%JobType            - Display the job type. (For jobs only)
	%U/%Uid                - Display the uid of the job/step.
	%u/%User               - Display the user who submitted the job/step.
	%X/%Exclusive          - Display the exclusive status of the job. (For jobs only)
	%x/%ExcludeNodes       - Display the exclude nodes of the job. (For jobs only)

Examples:
  --format "%j %n %t"              # Natural width for all fields
  --format "%5j %20n %t"           # Left-aligned: JobID (min 5), Name (min 20), State
  --format "%.5j %.20n %t"         # Right-aligned: JobID (min 5), Name (min 20), State
  --format "ID:%8j | Name:%.15n"   # Mixed: left-aligned JobID, right-aligned Name with prefix

Note: If the format is invalid or unrecognized, the program will terminate with an error message.
`)
	RootCmd.Flags().BoolVarP(&FlagFull, "full", "F", false,
		"Display full information (If not set, only display 30 characters per cell)")
	RootCmd.Flags().Uint32VarP(&FlagNumLimit, "max-lines", "m", util.DefaultCqueueMaxLines,
		"Limit the number of lines in the output, 0 means no limit")
	RootCmd.Flags().BoolVar(&FlagJson, "json", false,
		"Output in JSON format")
	RootCmd.Flags().BoolVar(&FlagCount, "count", false,
		"Output lightweight queue state counts only")
	RootCmd.Flags().StringVarP(&FlagFilterNodeNames, "nodelist", "w", "",
		"Specify node names to view (comma separated list or patterns like node[1-10]), default is all")

	util.InitCraneLogger()
}

func validateCountFlags(cmd *cobra.Command) error {
	unsupported := []string{
		"step", "job", "name", "qos", "user", "account", "partition",
		"type", "licenses", "start", "deadline", "self", "format", "full",
		"max-lines", "nodelist",
	}
	for _, name := range unsupported {
		if cmd.Flags().Changed(name) {
			return util.NewCraneErr(util.ErrorCmdArg,
				fmt.Sprintf("--count does not support --%s yet; use --state only", name))
		}
	}
	return nil
}
