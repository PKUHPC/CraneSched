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

package cwrapper

import (
	"CraneFrontEnd/internal/cacct"
	"CraneFrontEnd/internal/cacctmgr"
	"CraneFrontEnd/internal/calloc"
	"CraneFrontEnd/internal/cbatch"
	"CraneFrontEnd/internal/ccancel"
	"CraneFrontEnd/internal/ccontrol"
	"CraneFrontEnd/internal/ceff"
	"CraneFrontEnd/internal/cinfo"
	"CraneFrontEnd/internal/cqueue"
	"CraneFrontEnd/internal/creport"
	"CraneFrontEnd/internal/crun"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

/*
We have two ways to implement the Wrapper:
- Directly use Cobra to reimplement a command, including all its subcommands and flags,
  then call the original command.
- Do not reimplement a new command, but directly call the original command after
  replacing and modifying the args.

For the first, since we have reimplemented a command, we get a realistic help and command line experience.
 However, if the wrapped command is too complex, the amount of code will be relatively large.

For the second, we directly call the original command, so the amount of code will be relatively small.
 However, it requires a series of processing on the args, which is likely to lead to some corner cases
 not being properly handled.

To sum up, ccontrol, cacctmgr, cbatch, calloc, crun, ceff and creport are too complex or very same to their
 slurm counterparts, so we choose the 2nd way. The rest of the commands follow the 1st approach.
*/

type SlurmWrapper struct {
}

var (
	wrapCeffLeafRunEOnce    sync.Once
	wrapCreportLeafRunEOnce sync.Once
)

func (w SlurmWrapper) Group() *cobra.Group {
	return &cobra.Group{
		ID:    "slurm",
		Title: "Slurm Commands:",
	}
}

func (w SlurmWrapper) SubCommands() []*cobra.Command {
	return []*cobra.Command{
		sacct(),
		sacctmgr(),
		salloc(),
		sbatch(),
		scancel(),
		scontrol(),
		seff(),
		sinfo(),
		squeue(),
		sreport(),
		srun(),
	}
}

func (w SlurmWrapper) HasCommand(cmd string) bool {
	return slices.Contains([]string{
		"sacct", "sacctmgr", "salloc", "sbatch", "scancel",
		"scontrol", "seff", "sinfo", "squeue", "sreport", "srun",
	}, cmd)
}

func (w SlurmWrapper) Preprocess() error {
	util.SetOutputMode(util.OutputModeSlurm)
	return nil
}

func sacct() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "sacct",
		Short:              "Display job accounting data",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			cacct.RootCmd.Use = "sacct [flags]"
			// Slurm uses -V for version and -v for verbose.
			cacct.RootCmd.Flags().BoolP("version", "V", false, "version for sacct")
			convertedArgs := make([]string, 0, len(args))
			for i := 0; i < len(args); i++ {
				arg := args[i]
				switch {
				case isSlurmVerboseArg(arg) || arg == "--verbose":
					log.Warning("The feature --verbose/-v is not yet supported by Crane, the use is ignored.")
				case arg == "--jobs":
					convertedArgs = append(convertedArgs, "--job")
				case arg == "--starttime":
					convertedArgs = append(convertedArgs, "--start-time")
				case arg == "--endtime":
					convertedArgs = append(convertedArgs, "--end-time")
				case arg == "-n":
					convertedArgs = append(convertedArgs, "-N")
				case arg == "-o" || arg == "--format":
					convertedArgs = append(convertedArgs, arg)
					if i+1 < len(args) {
						convertedArgs = append(convertedArgs, convertSacctFormat(args[i+1]))
						i++
					}
				case strings.HasPrefix(arg, "--format="):
					convertedArgs = append(convertedArgs,
						"--format="+convertSacctFormat(strings.TrimPrefix(arg, "--format=")))
				default:
					convertedArgs = append(convertedArgs, arg)
				}
			}
			cacct.RootCmd.SetArgs(convertedArgs)
			err := cacct.RootCmd.Execute()
			if err != nil {
				exitWithCommandError("sacct", err)
			} else {
				os.Exit(util.ErrorSuccess)
			}
		},
	}

	addConfigPathFlag(cmd, &cacct.FlagConfigFilePath)
	cmd.Flags().StringVarP(&cacct.FlagFilterAccounts, "account", "A", "",
		"Displays jobs when a comma separated list of accounts are given as the argument.")
	cmd.Flags().StringVarP(&cacct.FlagFilterJobIDs, "jobs", "j", "",
		"Displays information about specified jobs. Supports jobid, jobid.stepid, jobid_arraytaskid, and jobid_arraytaskid.stepid.")
	cmd.Flags().StringVarP(&cacct.FlagFilterUsers, "user", "u", "",
		"Use this comma separated list of user names to select jobs to display.")

	return cmd
}

var sacctFormatSpecRegex = regexp.MustCompile(`%\.?\d*[a-zA-Z]`)

// Field names that Slurm and cacct spell differently.
var sacctFieldAliases = map[string]string{
	"elapsed": "ElapsedTime",
	"user":    "UserName",
	"nnodes":  "NodeNum",
	"start":   "StartTime",
	"end":     "EndTime",
	"submit":  "SubmitTime",
}

// convertSacctFormat converts a Slurm-style comma separated field name list
// (e.g. "JobID,JobName%20,Elapsed") into the %-specifier format accepted by
// cacct. Values already written in the %-specifier format are left unchanged.
func convertSacctFormat(format string) string {
	if sacctFormatSpecRegex.MatchString(format) {
		return format
	}
	fields := strings.Split(format, ",")
	specifiers := make([]string, 0, len(fields))
	for _, field := range fields {
		name, width, _ := strings.Cut(field, "%")
		if alias, ok := sacctFieldAliases[strings.ToLower(name)]; ok {
			name = alias
		}
		specifiers = append(specifiers, "%"+width+name)
	}
	return strings.Join(specifiers, " ")
}

func sacctmgr() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "sacctmgr",
		Short:              "Manage accounting records",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cacctmgr.ParseCmdArgs(normalizeSacctmgrArgs(args))
			return nil
		},
	}

	return cmd
}

func normalizeSacctmgrArgs(args []string) []string {
	convertedArgs := []string{"cacctmgr"}

	action := ""
	entity := ""
	clause := ""
	hasID := false

	for i := 0; i < len(args); i++ {
		arg := args[i]

		if strings.HasPrefix(arg, "-") {
			// Slurm uses -V for version and -v for verbose.
			if isSlurmVerboseArg(arg) || arg == "--verbose" {
				log.Warning("The feature --verbose/-v is not yet supported by Crane, the use is ignored.")
				continue
			}
			if arg == "-V" {
				arg = "--version"
			}
			convertedArgs = append(convertedArgs, arg)
			if wrapperFlagConsumesValue(arg) && i+1 < len(args) {
				convertedArgs = append(convertedArgs, args[i+1])
				i++
			}
			continue
		}

		if key, op, value, ok := splitWrapperAssignment(arg); ok {
			if normalized := normalizeSacctmgrKey(action, entity, clause, key); normalized != "" {
				key = normalized
			}
			convertedArgs = append(convertedArgs, key+op+value)
			continue
		}

		if normalizedAction, ok := normalizeSacctmgrAction(arg); ok && action == "" {
			action = normalizedAction
			convertedArgs = append(convertedArgs, normalizedAction)
			continue
		}

		if normalizedEntity, ok := normalizeSacctmgrEntity(arg); ok && entity == "" {
			entity = normalizedEntity
			convertedArgs = append(convertedArgs, normalizedEntity)
			continue
		}

		lowerArg := strings.ToLower(arg)
		if lowerArg == "where" || lowerArg == "set" {
			clause = lowerArg
			convertedArgs = append(convertedArgs, lowerArg)
			continue
		}

		if lowerArg == "withclusters" {
			convertedArgs = append(convertedArgs, "withclusters")
			continue
		}

		if normalizedKey := normalizeSacctmgrKey(action, entity, clause, arg); normalizedKey != "" {
			convertedArg, nextIdx := normalizeWrapperKeyValueArg(normalizedKey, args, i)
			convertedArgs = append(convertedArgs, convertedArg)
			i = nextIdx
			continue
		}

		if !hasID && sacctmgrActionAllowsID(action) && entity != "" && clause == "" {
			hasID = true
			convertedArgs = append(convertedArgs, arg)
			continue
		}

		convertedArgs = append(convertedArgs, arg)
	}

	return convertedArgs
}

func normalizeSacctmgrAction(arg string) (string, bool) {
	switch strings.ToLower(arg) {
	case "create", "add":
		return "add", true
	case "remove", "delete":
		return "delete", true
	case "update", "modify":
		return "modify", true
	case "list", "show":
		return "show", true
	case "block":
		return "block", true
	case "unblock":
		return "unblock", true
	case "reset":
		return "reset", true
	default:
		return "", false
	}
}

func normalizeSacctmgrEntity(arg string) (string, bool) {
	switch strings.ToLower(arg) {
	case "account", "user", "qos", "transaction", "event", "wckey", "resource":
		return strings.ToLower(arg), true
	default:
		return "", false
	}
}

func normalizeSacctmgrKey(action, entity, clause, key string) string {
	lowerKey := strings.ToLower(key)

	switch action {
	case "add":
		switch entity {
		case "account":
			switch lowerKey {
			case "name", "names":
				return "name"
			case "description", "parent", "defaultqos":
				return lowerKey
			case "partition", "partitions":
				return "partition"
			case "qoslist", "allowedqos", "allowedqoslist":
				return "qoslist"
			}
		case "user":
			switch lowerKey {
			case "account", "accounts":
				return "account"
			case "coordinator", "level", "name", "names":
				if lowerKey == "names" {
					return "name"
				}
				return lowerKey
			case "adminlevel":
				return "level"
			case "partition", "partitions":
				return "partition"
			}
		case "qos":
			return normalizeSacctmgrQosKey(lowerKey)
		case "wckey":
			if lowerKey == "user" {
				return "user"
			}
		case "resource":
			switch lowerKey {
			case "name", "server", "count", "description", "lastconsumed",
				"allocated", "servertype", "type", "allowed", "cluster", "flags":
				return lowerKey
			}
		}
	case "delete":
		switch entity {
		case "account", "qos":
			if lowerKey == "name" || lowerKey == "names" {
				return "name"
			}
		case "user":
			switch lowerKey {
			case "name", "names":
				return "name"
			case "account", "accounts":
				return "account"
			}
		case "wckey":
			if lowerKey == "user" {
				return "user"
			}
		case "resource":
			switch lowerKey {
			case "name", "server", "cluster":
				return lowerKey
			}
		}
	case "block", "unblock":
		if (entity == "account" || entity == "user") &&
			(lowerKey == "account" || lowerKey == "accounts") {
			return "account"
		}
	case "show":
		switch entity {
		case "account", "qos":
			switch lowerKey {
			case "name", "names":
				return "name"
			case "format":
				return "format"
			}
		case "user":
			switch lowerKey {
			case "name", "names":
				return "name"
			case "account", "accounts":
				return "account"
			case "format":
				return "format"
			}
		case "transaction":
			switch lowerKey {
			case "actor", "target", "action", "info", "starttime":
				return lowerKey
			}
		case "event":
			switch lowerKey {
			case "maxlines", "nodes", "state", "starttime", "endtime", "format":
				return lowerKey
			}
		case "resource":
			switch lowerKey {
			case "name", "server", "cluster":
				return lowerKey
			}
		}
	case "modify":
		switch entity {
		case "account":
			switch clause {
			case "where":
				if lowerKey == "name" || lowerKey == "names" {
					return "name"
				}
			case "set":
				switch lowerKey {
				case "description", "defaultqos", "defaultaccount":
					return lowerKey
				case "partition", "partitions", "allowedpartition", "allowedpartitions":
					return "allowedpartition"
				case "qos", "qoslist", "allowedqos", "allowedqoslist":
					return "allowedqos"
				}
			}
		case "user":
			switch clause {
			case "where":
				switch lowerKey {
				case "name", "names":
					return "name"
				case "account", "accounts":
					return "account"
				case "partition", "partitions":
					return "partition"
				}
			case "set":
				switch lowerKey {
				case "defaultaccount", "defaultqos":
					return lowerKey
				case "adminlevel", "level":
					return "adminlevel"
				case "partition", "partitions", "allowedpartition", "allowedpartitions":
					return "allowedpartition"
				case "qos", "qoslist", "allowedqos", "allowedqoslist":
					return "allowedqos"
				}
			}
		case "qos":
			switch clause {
			case "where":
				if lowerKey == "name" || lowerKey == "names" {
					return "name"
				}
			case "set":
				return normalizeSacctmgrQosKey(lowerKey)
			}
		case "wckey":
			switch clause {
			case "where":
				if lowerKey == "user" {
					return "user"
				}
			case "set":
				if lowerKey == "defaultwckey" {
					return "defaultwckey"
				}
			}
		case "resource":
			switch clause {
			case "", "where":
				switch lowerKey {
				case "name", "server", "cluster":
					return lowerKey
				}
			case "set":
				switch lowerKey {
				case "count", "description", "lastconsumed", "allocated",
					"servertype", "type", "allowed", "flags":
					return lowerKey
				}
			}
		}
	case "reset":
		if lowerKey == "name" || lowerKey == "names" {
			return "name"
		}
	}

	return ""
}

func normalizeSacctmgrQosKey(lowerKey string) string {
	switch lowerKey {
	case "name", "names":
		return "name"
	case "description", "priority", "flags",
		"maxtresperuser", "maxtresperaccount", "maxtres",
		"maxjobs", "maxsubmitjobs":
		return lowerKey
	case "maxjobpu", "maxjobspu", "maxjobsperuser":
		return "maxjobsperuser"
	case "maxcpupu", "maxcpuspu", "maxcpuperuser", "maxcpusperuser":
		return "maxcpusperuser"
	case "maxsubmitjobspu", "maxsubmitjobsperuser":
		return "maxsubmitjobsperuser"
	case "maxjobspa", "maxjobsperaccount":
		return "maxjobsperaccount"
	case "maxsubmitjobspa", "maxsubmitjobsperaccount":
		return "maxsubmitjobsperaccount"
	case "maxwall", "maxwalldurationperjob", "maxtimelimitperjob":
		return "maxtimelimitperjob"
	}
	return ""
}

func sacctmgrActionAllowsID(action string) bool {
	return slices.Contains([]string{
		"add", "delete", "block", "unblock", "show", "reset",
	}, action)
}

func splitWrapperAssignment(arg string) (key, op, value string, ok bool) {
	for _, operator := range []string{"+=", "-=", "="} {
		if idx := strings.Index(arg, operator); idx > 0 {
			return arg[:idx], operator, arg[idx+len(operator):], true
		}
	}
	return "", "", "", false
}

func normalizeWrapperKeyValueArg(key string, args []string, idx int) (string, int) {
	if idx+1 >= len(args) {
		return key, idx
	}

	op := "="
	nextArg := args[idx+1]
	if nextArg == "=" || nextArg == "+=" || nextArg == "-=" {
		op = nextArg
		if idx+2 >= len(args) {
			return key + op, idx + 1
		}
		return key + op + args[idx+2], idx + 2
	}

	if strings.HasPrefix(nextArg, "-") {
		return key, idx
	}
	return key + op + nextArg, idx + 1
}

func wrapperFlagConsumesValue(arg string) bool {
	if strings.Contains(arg, "=") {
		return false
	}
	switch arg {
	case "-C", "--config":
		return true
	default:
		return false
	}
}

// isSlurmVerboseArg reports whether arg is Slurm's -v verbose option,
// which allows repeated forms such as -vv.
func isSlurmVerboseArg(arg string) bool {
	return len(arg) > 1 && strings.TrimRight(arg, "v") == "-"
}

func salloc() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "salloc",
		Short:              "Allocate resources for a job",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			calloc.RootCmd.Use = "salloc"
			// Add --help from calloc
			calloc.RootCmd.InitDefaultHelpFlag()
			// Slurm uses -V for version and -v for verbose.
			calloc.RootCmd.Flags().BoolP("version", "V", false, "version for salloc")
			for i, arg := range args {
				if isSlurmVerboseArg(arg) {
					args[i] = "--verbose"
				}
			}

			// Parse flags
			if err := calloc.RootCmd.ParseFlags(args); err != nil {
				log.Errorf("salloc: error: %v", err)
				os.Exit(util.ErrorCmdArg)
			}
			args = calloc.RootCmd.Flags().Args()
			if help, err := calloc.RootCmd.Flags().GetBool("help"); err != nil || help {
				return calloc.RootCmd.Help()
			}
			if version, err := calloc.RootCmd.Flags().GetBool("version"); err == nil && version {
				fmt.Println(util.Version())
				return nil
			}

			PrintSallocIgnoreDummyArgsMessage()

			calloc.RootCmd.PersistentPreRun(cmd, args)
			// Validate the arguments
			if err := Validate(calloc.RootCmd, args); err != nil {
				log.Errorf("salloc: error: %v", err)
				os.Exit(util.ErrorCmdArg)
			}
			return calloc.RootCmd.RunE(calloc.RootCmd, args)
		},
	}
	// not implement feature:
	cmd.Flags().BoolVarP(&calloc.FlagNoKill, "no-kill", "k", false, "")
	cmd.Flags().BoolVarP(&calloc.FlagVerbose, "verbose", "v", false, "")

	return cmd
}

func scancel() *cobra.Command {
	var (
		FlagJobName        string
		FlagPartition      string
		FlagState          string
		FlagAccount        string
		FlagUserName       string
		FlagNodeList       []string
		FlagConfigFilePath string
		FlagJson           bool
		FlagVerbose        bool
	)
	cmd := &cobra.Command{
		Use:     "scancel",
		Short:   "Cancel jobs",
		Long:    "",
		GroupID: "slurm",
		Version: util.Version(),
		Run: func(cmd *cobra.Command, args []string) {
			if FlagVerbose {
				log.Warning("The feature --verbose/-v is not yet supported by Crane, the use is ignored.")
			}
			// scancel uses spaced arguments,
			// we need to convert it into a comma-separated list.
			ccancelArgs := make([]string, 0)
			if FlagJobName != "" {
				ccancelArgs = append(ccancelArgs, "--name", FlagJobName)
			}
			if FlagPartition != "" {
				ccancelArgs = append(ccancelArgs, "--partition", FlagPartition)
			}
			if FlagState != "" {
				ccancelArgs = append(ccancelArgs, "--state", FlagState)
			}
			if FlagAccount != "" {
				ccancelArgs = append(ccancelArgs, "--account", FlagAccount)
			}
			if FlagUserName != "" {
				ccancelArgs = append(ccancelArgs, "--user", FlagUserName)
			}
			if len(FlagNodeList) > 0 {
				ccancelArgs = append(ccancelArgs, "--nodes", strings.Join(FlagNodeList, ","))
			}
			if FlagConfigFilePath != "" {
				ccancelArgs = append(ccancelArgs, "--config", FlagConfigFilePath)
			}
			if FlagJson {
				ccancelArgs = append(ccancelArgs, "--json")
			}
			if len(args) > 0 {
				ccancelArgs = append(ccancelArgs, strings.Join(args, ","))
			}
			ccancel.RootCmd.SetArgs(ccancelArgs)
			err := ccancel.RootCmd.Execute()
			if err != nil {
				exitWithCommandError("scancel", err)
			} else {
				os.Exit(util.ErrorSuccess)
			}
		},
	}
	cmd.SetVersionTemplate(util.VersionTemplate())
	// Slurm uses -V for version and -v for verbose.
	cmd.Flags().BoolP("version", "V", false, "version for scancel")
	cmd.Flags().BoolVar(&FlagVerbose, "verbose", false, "")

	addConfigPathFlag(cmd, &FlagConfigFilePath)
	cmd.Flags().StringVarP(&FlagJobName, "name", "n", "",
		"Cancel jobs with the specified job name")
	cmd.Flags().StringVarP(&FlagPartition, "partition", "p", "",
		"Cancel jobs in the specified partition")
	cmd.Flags().StringVarP(&FlagState, "state", "t", "",
		"Cancel jobs of the specified states"+
			"Valid job states are PENDING(P), RUNNING(R), SUSPENDED(S), ALL. "+
			"job states are case-insensitive")
	cmd.Flags().StringVarP(&FlagAccount, "account", "A", "",
		"Cancel jobs under the specified account")
	cmd.Flags().StringVarP(&FlagUserName, "user", "u", "",
		"Cancel jobs submitted by the specified user")
	cmd.Flags().StringSliceVarP(&FlagNodeList, "nodelist", "w", nil,
		"Cancel jobs running or suspended on the specified nodes")
	cmd.Flags().BoolVar(&FlagJson, "json", false,
		"Output in JSON format")
	return cmd
}

func sbatch() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "sbatch",
		Short:              "Submit a batch job",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cbatch.RootCmd.Use = "sbatch [flags] file"
			cbatch.RootCmd.InitDefaultHelpFlag()
			// Slurm uses -V for version and -v for verbose.
			cbatch.RootCmd.Flags().BoolP("version", "V", false, "version for sbatch")
			for i, arg := range args {
				if isSlurmVerboseArg(arg) {
					args[i] = "--verbose"
				} else if (arg == "-t" || arg == "--time") && i+1 < len(args) {
					args[i+1] = cbatch.ConvertSlurmTimeFormat(args[i+1])
				} else if strings.HasPrefix(arg, "--time=") {
					args[i] = "--time=" + cbatch.ConvertSlurmTimeFormat(strings.TrimPrefix(arg, "--time="))
				}
			}

			if err := cbatch.RootCmd.ParseFlags(args); err != nil {
				log.Errorf("sbatch: error: %v", err)
				os.Exit(util.ErrorCmdArg)
			}
			args = cbatch.RootCmd.Flags().Args()
			if help, err := cbatch.RootCmd.Flags().GetBool("help"); err != nil || help {
				return cbatch.RootCmd.Help()
			}
			if version, err := cbatch.RootCmd.Flags().GetBool("version"); err == nil && version {
				fmt.Println(util.Version())
				return nil
			}
			PrintSbatchIgnoreArgsMessage()
			cbatch.RootCmd.PersistentPreRun(cmd, args)
			if err := Validate(cbatch.RootCmd, args); err != nil {
				log.Errorf("sbatch: error: %v", err)
				os.Exit(util.ErrorCmdArg)
			}
			return cbatch.RootCmd.RunE(cbatch.RootCmd, args)
		},
	}
	// not implement feature:
	cmd.Flags().BoolVar(&cbatch.FlagParsable, "parsable", false, "")
	cmd.Flags().BoolVarP(&cbatch.FlagVerbose, "verbose", "v", false, "")
	cmd.Flags().StringVar(&cbatch.FlagNTasksPerSocket, "ntasks-per-socket", "", "")
	cmd.Flags().StringVar(&cbatch.FlagCpuFreq, "cpu-freq", "", "")
	cmd.Flags().StringVar(&cbatch.FlagPriority, "priority", "", "")
	cmd.Flags().StringVar(&cbatch.FlagThreadsPerCore, "threads-per-core", "", "")
	cmd.Flags().StringVarP(&cbatch.FlagDistribution, "distribution", "m", "", "")
	cmd.Flags().StringVar(&cbatch.FlagSocketsPerNode, "sockets-per-node", "", "")
	cmd.Flags().StringVar(&cbatch.FlagCoresPerSocket, "cores-per-socket", "", "")
	cmd.Flags().BoolVar(&cbatch.FlagRequeue, "requeue", false, "")
	cmd.Flags().StringVarP(&cbatch.FlagWait, "wait", "W", "", "")
	return cmd
}

func scontrol() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "scontrol",
		Short:              "View or modify scheduler state",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			allArgs := normalizeScontrolArgs(args)
			ccontrol.ParseCmdArgs(allArgs)
			return cmd.Help()
		},
	}

	return cmd
}

func normalizeScontrolArgs(args []string) []string {
	convertedArgs := []string{"ccontrol"}
	action := ""
	showEntitySeen := false
	expectValue := false

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			// Slurm uses -V for version and -v for verbose.
			if isSlurmVerboseArg(arg) || arg == "--verbose" {
				log.Warning("The feature --verbose/-v is not yet supported by Crane, the use is ignored.")
				continue
			}
			if arg == "-V" {
				arg = "--version"
			}
			convertedArgs = append(convertedArgs, arg)
			if wrapperFlagConsumesValue(arg) && i+1 < len(args) {
				convertedArgs = append(convertedArgs, args[i+1])
				i++
			}
			continue
		}

		if expectValue {
			convertedArgs = append(convertedArgs, arg)
			expectValue = false
			continue
		}

		lowerArg := strings.ToLower(arg)
		if action == "" {
			switch lowerArg {
			case "show", "update", "hold", "release", "create", "delete", "reset":
				action = lowerArg
				convertedArgs = append(convertedArgs, lowerArg)
				continue
			}
		}

		switch action {
		case "show":
			if !showEntitySeen {
				if key, _, value, ok := splitWrapperAssignment(arg); ok {
					if normalizedEntity, ok := normalizeScontrolShowEntity(key); ok {
						convertedArgs = append(convertedArgs, normalizedEntity, value)
						showEntitySeen = true
						continue
					}
				}

				if normalizedEntity, ok := normalizeScontrolShowEntity(arg); ok {
					convertedArgs = append(convertedArgs, normalizedEntity)
					showEntitySeen = true
					continue
				}
			}
			convertedArgs = append(convertedArgs, arg)
		case "hold", "release":
			if key, op, value, ok := splitWrapperAssignment(arg); ok {
				if strings.EqualFold(key, "timelimit") {
					convertedArgs = append(convertedArgs, "timelimit"+op+value)
					continue
				}
				if strings.EqualFold(key, "job") || strings.EqualFold(key, "jobid") {
					jobIDs, nextIdx, err := collectScontrolJobIDs(args, i, value)
					if err != nil {
						log.Errorf("scontrol: error: Invalid job list specified: %v.", err)
						os.Exit(util.ErrorCmdArg)
					}
					convertedArgs = append(convertedArgs, jobIDs)
					i = nextIdx
					continue
				}
			}
			if strings.EqualFold(arg, "timelimit") {
				convertedArgs = append(convertedArgs, "timelimit")
				expectValue = true
				continue
			}
			if lowerArg == "job" || lowerArg == "jobid" {
				continue
			}
			jobIDs, nextIdx, err := collectScontrolJobIDs(args, i, arg)
			if err != nil {
				log.Errorf("scontrol: error: Invalid job list specified: %v.", err)
				os.Exit(util.ErrorCmdArg)
			}
			convertedArgs = append(convertedArgs, jobIDs)
			i = nextIdx
		default:
			if key, op, value, ok := splitWrapperAssignment(arg); ok {
				if normalized := normalizeScontrolUpdateKey(key); normalized != "" {
					key = normalized
				} else {
					key = strings.ToLower(key)
				}
				convertedArgs = append(convertedArgs, key+op+value)
				continue
			}
			convertedArgs = append(convertedArgs, arg)
		}
	}

	return convertedArgs
}

func collectScontrolJobIDs(args []string, idx int, firstValue string) (string, int, error) {
	jobIDs := make([]string, 0)
	if firstValue = strings.Trim(firstValue, ","); firstValue != "" {
		jobIDs = append(jobIDs, firstValue)
	}

	for idx+1 < len(args) {
		next := args[idx+1]
		if strings.HasPrefix(next, "-") || strings.EqualFold(next, "timelimit") {
			break
		}

		if key, _, value, ok := splitWrapperAssignment(next); ok {
			if strings.EqualFold(key, "timelimit") {
				break
			}
			if strings.EqualFold(key, "job") || strings.EqualFold(key, "jobid") {
				if value = strings.Trim(value, ","); value != "" {
					jobIDs = append(jobIDs, value)
				}
				idx++
				continue
			}
			break
		}

		if strings.EqualFold(next, "job") || strings.EqualFold(next, "jobid") {
			idx++
			continue
		}

		if next = strings.Trim(next, ","); next != "" {
			jobIDs = append(jobIDs, next)
		}
		idx++
	}

	concatedJobIDs := strings.Join(jobIDs, ",")
	if _, err := util.ParseJobIdSelectorList(concatedJobIDs, ","); err != nil {
		return "", idx, err
	}
	return concatedJobIDs, idx, nil
}

func normalizeScontrolShowEntity(arg string) (string, bool) {
	lowerArg := strings.ToLower(arg)
	if len(lowerArg) >= 5 && strings.HasPrefix("hostnames", lowerArg) {
		return "hostnames", true
	}

	switch lowerArg {
	case "jobid":
		return "job", true
	case "job":
		return "job", true
	case "nodename":
		return "node", true
	case "nodes":
		return "node", true
	case "node":
		return "node", true
	case "partitionname":
		return "partition", true
	case "partitions":
		return "partition", true
	case "partition":
		return "partition", true
	case "reservationname":
		return "reservation", true
	case "reservations":
		return "reservation", true
	case "reservation":
		return "reservation", true
	case "licensename", "licenses", "license":
		return "lic", true
	case "lic":
		return "lic", true
	case "config":
		return "config", true
	default:
		return "", false
	}
}

func normalizeScontrolUpdateKey(key string) string {
	switch strings.ToLower(key) {
	case "jobid":
		return "job"
	case "nodename":
		return "node"
	case "partitionname":
		return "partition"
	default:
		return ""
	}
}

func seff() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "seff",
		Short:              "Report job efficiency",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			ceff.RootCmd.Use = "seff [flags] [job_id, ...]"
			ceff.RootCmd.Flags().BoolP("version", "V", false, "version for seff")
			wrapCeffLeafRunEOnce.Do(func() {
				util.RunEWrapperForLeafCommand(ceff.RootCmd)
			})
			ceff.RootCmd.SetArgs(args)
			return ceff.RootCmd.Execute()
		},
	}

	return cmd
}

func sinfo() *cobra.Command {
	var FlagVerbose bool
	cmd := &cobra.Command{
		Use:     "sinfo",
		Short:   "View node and partition information",
		Long:    "",
		GroupID: "slurm",
		Version: util.Version(),
		RunE: func(cmd *cobra.Command, args []string) error {
			if FlagVerbose {
				log.Warning("The feature --verbose/-v is not yet supported by Crane, the use is ignored.")
			}
			cinfo.RootCmd.PersistentPreRun(cmd, args)
			return cinfo.RootCmd.RunE(cmd, args)
		},
	}
	cmd.SetVersionTemplate(util.VersionTemplate())
	// Slurm uses -V for version and -v for verbose.
	cmd.Flags().BoolP("version", "V", false, "version for sinfo")
	cmd.Flags().BoolVar(&FlagVerbose, "verbose", false, "")

	addConfigPathFlag(cmd, &cinfo.FlagConfigFilePath)
	// cmd.Flags().BoolVarP(&cinfo.FlagSummarize, "summarize", "s", false,
	// 	"List only a partition state summary with no node state details.")
	cmd.Flags().BoolVarP(&cinfo.FlagListReason, "list-reasons", "R", false,
		"List reasons nodes are in the down, drained, fail or failing state.")

	cmd.Flags().BoolVarP(&cinfo.FlagFilterDownOnly, "dead", "d", false,
		"If set, only report state information for non-responding (dead) nodes.")
	cmd.Flags().BoolVarP(&cinfo.FlagFilterRespondingOnly, "responding", "r", false,
		"If set only report state information for responding nodes.")
	cmd.Flags().StringSliceVarP(&cinfo.FlagFilterPartitions, "partition", "p", nil,
		"Print information about the node(s) in the specified partition(s). \nMultiple partitions are separated by commas.")
	cmd.Flags().StringSliceVarP(&cinfo.FlagFilterNodes, "nodes", "n", nil,
		"Print information about the specified node(s). \nMultiple nodes are separated by commas.")
	cmd.Flags().StringSliceVarP(&cinfo.FlagFilterCranedStates, "states", "t", nil,
		"List nodes only having the given state(s). Multiple states may be comma separated.")

	cmd.Flags().Uint64VarP(&cinfo.FlagIterate, "iterate", "i", 0,
		"Print the state on a periodic basis. Sleep for the indicated number of seconds between reports.")
	cmd.Flags().BoolVar(&cinfo.FlagJson, "json", false, "Output in JSON format")

	return cmd
}

func squeue() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "squeue",
		Short:              "View job queue information",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			if slices.Contains(args, "--help") {
				if err := cmd.Help(); err != nil {
					log.Error(err)
				}
				return
			}

			cqueue.RootCmd.Use = "squeue [flags]"
			// Slurm uses -V for version and -v for verbose.
			cqueue.RootCmd.Flags().BoolP("version", "V", false, "version for squeue")
			convertedArgs := make([]string, 0, len(args))
			for i := 0; i < len(args); i++ {
				arg := args[i]
				switch {
				case isSlurmVerboseArg(arg) || arg == "--verbose":
					log.Warning("The feature --verbose/-v is not yet supported by Crane, the use is ignored.")
				case arg == "-h":
					convertedArgs = append(convertedArgs, "-N")
				case arg == "--jobs":
					convertedArgs = append(convertedArgs, "--job")
				case arg == "--states":
					convertedArgs = append(convertedArgs, "--state")
				case arg == "-o" || arg == "--format":
					convertedArgs = append(convertedArgs, arg)
					if i+1 < len(args) {
						converted, err := convertSqueueFormat(args[i+1])
						if err != nil {
							log.Errorf("squeue: error: %v", err)
							os.Exit(util.ErrorCmdArg)
						}
						convertedArgs = append(convertedArgs, converted)
						i++
					}
				case strings.HasPrefix(arg, "--format="):
					converted, err := convertSqueueFormat(strings.TrimPrefix(arg, "--format="))
					if err != nil {
						log.Errorf("squeue: error: %v", err)
						os.Exit(util.ErrorCmdArg)
					}
					convertedArgs = append(convertedArgs, "--format="+converted)
				default:
					convertedArgs = append(convertedArgs, arg)
				}
			}
			cqueue.RootCmd.SetArgs(convertedArgs)
			err := cqueue.RootCmd.Execute()
			if err != nil {
				exitWithCommandError("squeue", err)
			} else {
				os.Exit(util.ErrorSuccess)
			}
		},
	}

	addConfigPathFlag(cmd, &cqueue.FlagConfigFilePath)
	// As --noheader will use -h, we need to add the help flag manually
	cmd.Flags().BoolP("help", "", false, "Help for this command.")
	cmd.Flags().BoolVarP(&cqueue.FlagStartTime, "start", "S", false,
		"Report the expected start time and resources to be allocated for pending jobs in order of \nincreasing start time.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterPartitions, "partition", "p", "",
		"Specify the partitions of the jobs or steps to view. Accepts a comma separated list of \npartition names.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterJobIDs, "jobs", "j", "",
		"Specify a comma separated list of job IDs to display (jobid or jobid_arraytaskid). Defaults to all jobs. ")
	cmd.Flags().StringVarP(&cqueue.FlagFilterJobNames, "name", "n", "",
		"Request jobs or job steps having one of the specified names. The list consists of a comma \nseparated list of job names.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterQos, "qos", "q", "",
		"Specify the qos(s) of the jobs or steps to view. Accepts a comma separated list of qos's.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterUsers, "user", "u", "",
		"Request jobs or job steps from a comma separated list of users.")
	cmd.Flags().StringVarP(&cqueue.FlagFilterAccounts, "account", "A", "",
		"Specify the accounts of the jobs to view. Accepts a comma separated list of account names.")
	cmd.Flags().Uint64VarP(&cqueue.FlagIterate, "iterate", "i", 0,
		"Repeatedly gather and report the requested information at the interval specified (in seconds). \nBy default, prints a time stamp with the header.")
	cmd.Flags().StringVarP(&cqueue.FlagFormat, "format", "o", "",
		`Specify the output format.
	Fields are identified by a percent sign (%) followed by a character or string. 
	Use a dot (.) and a number between % and the format character or string to specify a minimum width for the field.
	
Supported Slurm format identifiers:
	%a/%Account            - Display the account associated with the job.
	%C/%ReqCpus            - Display the cpus requested by the job.
	%D/%NodeNum            - Display the number of nodes requested by the job.
	%i/%JobID              - Display the logical job ID.
	                          Materialized array tasks use jobId_arrayTaskId; active array parents use jobId_[range%maxConcurrent] when limited.
	%j/%Name               - Display the name of the job.
	%k/%Comment            - Display the comment of the job.
	%l/%TimeLimit          - Display the time limit for the job.
	%M/%ElapsedTime        - Display the elapsed time from the start of the job.
	%m/%ReqMemPerNode      - Display the requested memory per node.
	%N/%NodeList           - Display the nodes allocated to the job.
	%n/%ReqNodes           - Display the requested nodes.
	%o/%Command            - Display the command line of the job.
	%P/%Partition          - Display the partition the job is running in.
	%p/%Priority           - Display the priority of the job.
	%q/%QoS                - Display the Quality of Service level for the job.
	%r/%Reason             - Display the pending reason.
	%S/%StartTime          - Display the start time of the job.
	%t/%StateCompact       - Display the compact state of the job.
	%T/%State              - Display the full state of the job.
	%u/%User               - Display the user who submitted the job.
	%U/%Uid                - Display the uid of the job.
	%V/%SubmitTime         - Display the submission time of the job.

Supported Crane extension identifiers (long form only):
	%AllocCpus             - Display the cpus allocated to the job.
	%AllocMemPerNode       - Display the memory allocated per node.
	%Deadline              - Display the deadline time of the job.
	%ExcludeNodes          - Display the excluded nodes.
	%Exclusive             - Display the exclusive status of the job.
	%Held                  - Display the hold state of the job.
	%JobType               - Display the job type.
	%ReqCpuPerTask         - Display the requested cpus per task.
	%StepId                - Display the step ID (for steps only).
	%Wckey                 - Display the wckey of the job.
Each format specifier or string can be modified with a width specifier (e.g., "%.5j").
If the width is specified, the field will be formatted to at least that width. 
If the format is invalid or unrecognized, the program will terminate with an error message.

Example: --format "%.5i %.20j %t" would output the job's ID with a minimum width of 5,
         Name with a minimum width of 20, and the State.
`)
	return cmd
}

var squeueFormatSpecRegex = regexp.MustCompile(`%(\.?\d*)([a-zA-Z]+)`)

// Maps Slurm squeue format specifiers to cqueue equivalents.
var squeueFormatSpecMapping = map[string]string{
	"a": "a",
	"C": "C",
	"D": "N",
	"i": "j",
	"j": "n",
	"k": "k",
	"l": "l",
	"M": "e",
	"m": "M",
	"N": "L",
	"n": "r",
	"o": "o",
	"P": "P",
	"p": "p",
	"q": "q",
	"r": "R",
	"S": "S",
	"T": "state",
	"t": "statecompact",
	"u": "u",
	"U": "U",
	"V": "s",
}

// convertSqueueFormat rewrites the Slurm single-letter format specifiers in a
// format string to their cqueue equivalents. Multi-letter (cqueue-style)
// specifiers are passed through unchanged. Slurm specifiers that have no
// cqueue field with the same meaning (e.g. %L time left, %e end time) are
// rejected instead of silently displaying different data.
func convertSqueueFormat(format string) (string, error) {
	var invalid string
	converted := squeueFormatSpecRegex.ReplaceAllStringFunc(format, func(spec string) string {
		sub := squeueFormatSpecRegex.FindStringSubmatch(spec)
		if len(sub[2]) > 1 {
			return spec
		}
		mapped, ok := squeueFormatSpecMapping[sub[2]]
		if !ok {
			if invalid == "" {
				invalid = sub[2]
			}
			return spec
		}
		return "%" + sub[1] + mapped
	})
	if invalid != "" {
		return "", fmt.Errorf("format specifier %%%s is not supported by cwrapper", invalid)
	}
	return converted, nil
}

func sreport() *cobra.Command {
	sreportCommandTokens := map[string]struct{}{
		"user":                     {},
		"cluster":                  {},
		"job":                      {},
		"topusage":                 {},
		"accountutilizationbyuser": {},
		"userutilizationbyaccount": {},
		"userutilizationbywckey":   {},
		"wckeyutilizationbyuser":   {},
		"accountutilizationbyqos":  {},
		"sizesbyaccount":           {},
		"sizesbywckey":             {},
		"sizesbyaccountandwckey":   {},
	}

	cmd := &cobra.Command{
		Use:                "sreport",
		Short:              "Generate accounting reports",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			creport.RootCmd.Use = "sreport"
			// Slurm uses -V for version and -v for verbose.
			creport.RootCmd.Flags().BoolP("version", "V", false, "version for sreport")
			convertedArgs := make([]string, 0, len(args))
			normalizeCommandToken := true
			for _, arg := range args {
				if isSlurmVerboseArg(arg) || arg == "--verbose" {
					log.Warning("The feature --verbose/-v is not yet supported by Crane, the use is ignored.")
					continue
				}
				if strings.Contains(arg, "=") && !strings.HasPrefix(arg, "-") {
					log.Warningf("Slurm-style key=value argument %q is not supported in sreport wrapper. "+
						"Please use sreport with explicit flags, e.g. --start-time/--end-time.", arg)
					os.Exit(util.ErrorCmdArg)
				}
				if strings.HasPrefix(arg, "-") {
					normalizeCommandToken = false
					convertedArgs = append(convertedArgs, arg)
				} else {
					lowerArg := strings.ToLower(arg)
					if normalizeCommandToken {
						if _, ok := sreportCommandTokens[lowerArg]; ok {
							convertedArgs = append(convertedArgs, lowerArg)
							continue
						}
						normalizeCommandToken = false
					}
					convertedArgs = append(convertedArgs, arg)
				}
			}

			wrapCreportLeafRunEOnce.Do(func() {
				util.RunEWrapperForLeafCommand(creport.RootCmd)
			})
			creport.RootCmd.SetArgs(convertedArgs)
			return creport.RootCmd.Execute()
		},
	}

	return cmd
}

func srun() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "srun",
		Short:              "Run a job step",
		Long:               "",
		GroupID:            "slurm",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			crun.RootCmd.Use = "srun [flags] executable"
			crun.RootCmd.InitDefaultHelpFlag()
			// Slurm uses -V for version and -v for verbose.
			crun.RootCmd.Flags().BoolP("version", "V", false, "version for srun")
			for i, arg := range args {
				if isSlurmVerboseArg(arg) {
					args[i] = "--verbose"
				}
			}

			if err := crun.RootCmd.ParseFlags(args); err != nil {
				log.Errorf("srun: error: %v", err)
				os.Exit(util.ErrorCmdArg)
			}
			args = crun.RootCmd.Flags().Args()
			if help, err := crun.RootCmd.Flags().GetBool("help"); err != nil || help {
				return crun.RootCmd.Help()
			}
			if version, err := crun.RootCmd.Flags().GetBool("version"); err == nil && version {
				fmt.Println(util.Version())
				return nil
			}
			PrintSrunIgnoreDummyArgsMessage()
			crun.RootCmd.PersistentPreRun(cmd, args)
			if err := Validate(crun.RootCmd, args); err != nil {
				log.Errorf("srun: error: %v", err)
				os.Exit(util.ErrorCmdArg)
			}

			return crun.RootCmd.RunE(crun.RootCmd, args)
		},
	}
	// not implement features:
	cmd.Flags().StringVarP(&crun.FlagNTasks, "ntasks", "n", "", "")
	cmd.Flags().StringVar(&crun.FlagMultiProg, "multi-prog", "", "")
	cmd.Flags().StringVarP(&crun.FlagOversubscribe, "oversubscribe", "s", "", "")
	cmd.Flags().StringVar(&crun.FlagCpuBind, "cpu-bind", "", "")
	cmd.Flags().StringVarP(&crun.FlagWait, "wait", "w", "", "")
	cmd.Flags().StringVar(&crun.FlagMpi, "mpi", "", "")
	cmd.Flags().StringVarP(&crun.FlagDependency, "dependency", "d", "", "")
	cmd.Flags().BoolVarP(&crun.FlagVerbose, "verbose", "v", false, "")
	cmd.Flags().StringVarP(&crun.FlagError, "error", "e", "", "")
	cmd.Flags().StringVarP(&crun.FlagKillOnBadExit, "kill-on-bad-exit", "k", "", "")
	cmd.Flags().StringVarP(&crun.FlagExtraNodeInfo, "extra-node-info", "B", "", "")
	cmd.Flags().StringVar(&crun.FlagNTasksPerCore, "ntasks-per-core", "", "")
	cmd.Flags().StringVarP(&crun.FlagConstraint, "constraint", "C", "", "")
	cmd.Flags().StringVar(&crun.FlagDistribution, "distribution", "", "")
	cmd.Flags().BoolVar(&crun.FlagLabel, "label", false, "")
	return cmd
}

func Validate(c *cobra.Command, args []string) error {
	if err := c.ValidateArgs(args); err != nil {
		return err
	}
	if err := c.ValidateRequiredFlags(); err != nil {
		return err
	}
	if err := c.ValidateFlagGroups(); err != nil {
		return err
	}
	return nil
}

func PrintSrunIgnoreDummyArgsMessage() {
	if crun.FlagNTasks != "" {
		log.Warning("The feature --ntasks/-n is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagMultiProg != "" {
		log.Warning("The feature --multi-prog is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagOversubscribe != "" {
		log.Warning("The feature --oversubscribe/-s is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagCpuBind != "" {
		log.Warning("The feature --cpu-bind is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagWait != "" {
		log.Warning("The feature --wait/-w is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagMpi != "" {
		log.Warning("The feature --mpi is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagDependency != "" {
		log.Warning("The feature --dependency/-d is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagVerbose {
		log.Warning("The feature --verbose/-v is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagError != "" {
		log.Warning("The feature --error/-e is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagKillOnBadExit != "" {
		log.Warning("The feature --kill-on-bad-exit/-k is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagExtraNodeInfo != "" {
		log.Warning("The feature --extra-node-info/-B is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagNTasksPerCore != "" {
		log.Warning("The feature --ntasks-per-core is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagConstraint != "" {
		log.Warning("The feature --constraint/-C is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagDistribution != "" {
		log.Warning("The feature --distribution is not yet supported by Crane, the use is ignored.")
	}
	if crun.FlagLabel {
		log.Warning("The feature --label is not yet supported by Crane, the use is ignored.")
	}
}

func PrintSallocIgnoreDummyArgsMessage() {
	if calloc.FlagNoKill {
		log.Warning("The feature --no-kill/-k is not yet supported by Crane, the use is ignored.")
	}
	if calloc.FlagVerbose {
		log.Warning("The feature --verbose/-v is not yet supported by Crane, the use is ignored.")
	}
}

func PrintSbatchIgnoreArgsMessage() {
	if cbatch.FlagVerbose {
		log.Warning("The feature --verbose/-v is not yet supported by Crane, the use is ignored.")
	}
	if cbatch.FlagNTasksPerSocket != "" {
		log.Warning("The feature --ntasks-per-socket is not yet supported by Crane, the use is ignored.")
	}
	if cbatch.FlagCpuFreq != "" {
		log.Warning("The feature --cpu-freq is not yet supported by Crane, the use is ignored.")
	}
	if cbatch.FlagPriority != "" {
		log.Warning("The feature --priority is not yet supported by Crane, the use is ignored.")
	}
	if cbatch.FlagThreadsPerCore != "" {
		log.Warning("The feature --threads-per-core is not yet supported by Crane, the use is ignored.")
	}
	if cbatch.FlagDistribution != "" {
		log.Warning("The feature --distribution/-m is not yet supported by Crane, the use is ignored.")
	}
	if cbatch.FlagSocketsPerNode != "" {
		log.Warning("The feature --sockets-per-node is not yet supported by Crane, the use is ignored.")
	}
	if cbatch.FlagCoresPerSocket != "" {
		log.Warning("The feature --cores-per-socket is not yet supported by Crane, the use is ignored.")
	}
	if cbatch.FlagWait != "" {
		log.Warning("The feature --wait/-W is not yet supported by Crane, the use is ignored.")
	}
}
