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
	"CraneFrontEnd/internal/cbatch"
	"CraneFrontEnd/internal/ccancel"
	"CraneFrontEnd/internal/cqueue"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type SGEWrapper struct {
}

func (w SGEWrapper) Group() *cobra.Group {
	return &cobra.Group{
		ID:    "sge",
		Title: "SGE Commands:",
	}
}

func (w SGEWrapper) SubCommands() []*cobra.Command {
	return []*cobra.Command{
		qacct(),
		qdel(),
		qsub(),
		qstat(),
	}
}

func (w SGEWrapper) HasCommand(cmd string) bool {
	return slices.Contains([]string{"qacct", "qdel", "qsub", "qstat"}, cmd)
}

func (w SGEWrapper) Preprocess() error {
	subCommand := ""
	if len(os.Args) > 1 {
		subCommand = os.Args[1]
	}

	for i, v := range os.Args {
		// Skip program name and subcommand
		if i <= 1 {
			continue
		}

		if v == "--" {
			break
		}

		switch v {
		case "-?":
			os.Args[i] = "--help"
			continue
		case "-h":
			if sgeHelpShortAliasEnabled(subCommand) {
				os.Args[i] = "--help"
				continue
			}
		}

		if len(v) >= 2 && v[0] == '-' && v[1] != '-' {
			os.Args[i] = "-" + v
		}
	}

	return nil
}

func sgeHelpShortAliasEnabled(subCommand string) bool {
	return slices.Contains([]string{"qacct", "qdel", "qstat"}, subCommand)
}

var (
	FlagQsubA   string
	FlagQsubN   string
	FlagQsubO   string
	FlagQsubE   string
	FlagQsubAt  string
	FlagQsubQ   string
	FlagQsubT   string
	FlagQsubV   string
	FlagQsubWd  string
	FlagQsubM   string
	FlagQsubm   string
	FlagQsubS   string
	FlagQsubCwd bool
	FlagQsubAll bool
	FlagQsubH   bool

	FlagQstatF bool
	FlagQstatI bool
	FlagQstatN bool
	FlagQstatQ string
	FlagQstatR bool
	FlagQstatU string

	FlagQacctA    string
	FlagQacctB    string
	FlagQacctD    int
	FlagQacctE    string
	FlagQacctEEnd bool
	FlagQacctJ    string
	FlagQacctO    string
	FlagQacctQ    string
	FlagQacctU    string
)

func qsub() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "qsub [flags] file",
		Short: "Wrapper of cbatch command",
		Long: `Wrapper of cbatch command.
Currently supports a practical subset of qsub options that can be mapped to Crane.`,
		GroupID: "sge",
		Args: func(cmd *cobra.Command, args []string) error {
			if sgeHelpRequested(cmd) {
				return nil
			}
			return cobra.ExactArgs(1)(cmd, args)
		},
		Run: func(cmd *cobra.Command, args []string) {
			if sgeHelpRequested(cmd) {
				_ = cmd.Help()
				os.Exit(util.ErrorSuccess)
			}

			cbatchArgs := make([]string, 0)

			if FlagQsubA != "" {
				cbatchArgs = append(cbatchArgs, "--account", FlagQsubA)
			}
			if FlagQsubN != "" {
				cbatchArgs = append(cbatchArgs, "--job-name", FlagQsubN)
			}
			if FlagQsubO != "" {
				cbatchArgs = append(cbatchArgs, "--output", FlagQsubO)
			}
			if FlagQsubE != "" {
				cbatchArgs = append(cbatchArgs, "--error", FlagQsubE)
			}
			if FlagQsubAt != "" {
				beginAt, err := convertSGEDateTime(FlagQsubAt)
				if err != nil {
					log.Errorf("qsub: error: %v", err)
					os.Exit(util.ErrorCmdArg)
				}
				cbatchArgs = append(cbatchArgs, "--begin", beginAt)
			}
			if FlagQsubQ != "" {
				cbatchArgs = append(cbatchArgs, "--partition", FlagQsubQ)
			}
			if FlagQsubT != "" {
				cbatchArgs = append(cbatchArgs, "--array", FlagQsubT)
			}
			if FlagQsubV != "" {
				cbatchArgs = append(cbatchArgs, "--export", FlagQsubV)
			}
			if FlagQsubAll {
				cbatchArgs = append(cbatchArgs, "--get-user-env")
			}
			if FlagQsubWd != "" {
				cbatchArgs = append(cbatchArgs, "--chdir", FlagQsubWd)
			} else if FlagQsubCwd {
				wd, err := os.Getwd()
				if err != nil {
					log.Errorf("qsub: error: failed to get current working directory: %v", err)
					os.Exit(util.ErrorCmdArg)
				}
				cbatchArgs = append(cbatchArgs, "--chdir", wd)
			}
			if FlagQsubM != "" {
				cbatchArgs = append(cbatchArgs, "--mail-user", FlagQsubM)
			}
			if FlagQsubm != "" {
				mailType, err := convertSGEMailType(FlagQsubm)
				if err != nil {
					log.Errorf("qsub: error: %v", err)
					os.Exit(util.ErrorCmdArg)
				}
				if mailType != "" {
					cbatchArgs = append(cbatchArgs, "--mail-type", mailType)
				}
			}
			if FlagQsubS != "" {
				cbatchArgs = append(cbatchArgs, "--interpreter", FlagQsubS)
			}
			if FlagQsubH {
				cbatchArgs = append(cbatchArgs, "--hold")
			}

			cbatchArgs = append(cbatchArgs, args[0])
			cbatch.RootCmd.SetArgs(cbatchArgs)

			err := cbatch.RootCmd.Execute()
			if err != nil {
				exitWithCommandError("qsub", err)
			} else {
				os.Exit(util.ErrorSuccess)
			}
		},
	}

	addConfigPathFlag(cmd, &cbatch.FlagConfigFilePath)
	cmd.Flags().StringVar(&FlagQsubA, "A", "", "Account string used for the job.")
	cmd.Flags().StringVar(&FlagQsubN, "N", "", "Name of the job.")
	cmd.Flags().StringVar(&FlagQsubO, "o", "", "Path for standard output.")
	cmd.Flags().StringVar(&FlagQsubE, "e", "", "Path for standard error.")
	cmd.Flags().StringVar(&FlagQsubAt, "a", "", "Start the job at the specified time, format [[CC]YY]MMDDhhmm[.SS].")
	cmd.Flags().StringVar(&FlagQsubQ, "q", "", "Queue to submit to.")
	cmd.Flags().StringVar(&FlagQsubT, "t", "", "Array task range, for example 1-10 or 1-10:2.")
	cmd.Flags().StringVar(&FlagQsubV, "v", "", "Export the specified environment variables.")
	cmd.Flags().StringVar(&FlagQsubWd, "wd", "", "Working directory of the job.")
	cmd.Flags().StringVar(&FlagQsubM, "M", "", "Mail address of the notification receiver.")
	cmd.Flags().StringVar(&FlagQsubm, "m", "", "Mail options. Supported values: b, e, a, n.")
	cmd.Flags().StringVar(&FlagQsubS, "S", "", "Interpreter used to run the script.")
	cmd.Flags().BoolVar(&FlagQsubCwd, "cwd", false, "Run the job from the current working directory.")
	cmd.Flags().BoolVar(&FlagQsubAll, "V", false, "Export the current environment.")
	cmd.Flags().BoolVar(&FlagQsubH, "h", false, "Submit the job in held state.")

	return cmd
}

func qdel() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "qdel job_id ...",
		Short:   "Wrapper of ccancel command",
		Long:    "Wrapper of ccancel command.",
		GroupID: "sge",
		Args: func(cmd *cobra.Command, args []string) error {
			if sgeHelpRequested(cmd) {
				return nil
			}
			return cobra.MinimumNArgs(1)(cmd, args)
		},
		Run: func(cmd *cobra.Command, args []string) {
			if sgeHelpRequested(cmd) {
				_ = cmd.Help()
				os.Exit(util.ErrorSuccess)
			}

			ccancelArgs := []string{strings.Join(args, ",")}
			ccancel.RootCmd.SetArgs(ccancelArgs)
			err := ccancel.RootCmd.Execute()
			if err != nil {
				exitWithCommandError("qdel", err)
			} else {
				os.Exit(util.ErrorSuccess)
			}
		},
	}

	addConfigPathFlag(cmd, &ccancel.FlagConfigFilePath)
	return cmd
}

func qacct() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "qacct [flags]",
		Short:   "Wrapper of cacct command",
		Long:    "Wrapper of cacct command.",
		GroupID: "sge",
		Args: func(cmd *cobra.Command, args []string) error {
			if sgeHelpRequested(cmd) {
				return nil
			}
			return cobra.NoArgs(cmd, args)
		},
		Run: func(cmd *cobra.Command, args []string) {
			if sgeHelpRequested(cmd) {
				_ = cmd.Help()
				os.Exit(util.ErrorSuccess)
			}

			cacctArgs := make([]string, 0)

			if FlagQacctA != "" {
				cacctArgs = append(cacctArgs, "--account", FlagQacctA)
			}
			if FlagQacctJ != "" {
				if isSGEJobIDFilter(FlagQacctJ) {
					cacctArgs = append(cacctArgs, "--job", FlagQacctJ)
				} else {
					cacctArgs = append(cacctArgs, "--name", FlagQacctJ)
				}
			}
			if FlagQacctO != "" {
				cacctArgs = append(cacctArgs, "--user", FlagQacctO)
			}
			if FlagQacctU != "" {
				cacctArgs = append(cacctArgs, "--user", FlagQacctU)
			}
			if FlagQacctQ != "" {
				cacctArgs = append(cacctArgs, "--partition", FlagQacctQ)
			}

			if FlagQacctB != "" || FlagQacctE != "" || FlagQacctD > 0 {
				start, end, err := buildQacctTimeRange()
				if err != nil {
					log.Errorf("qacct: error: %v", err)
					os.Exit(util.ErrorCmdArg)
				}

				if FlagQacctEEnd {
					if start != "" || end != "" {
						cacctArgs = append(cacctArgs, "--end-time", start+"~"+end)
					}
				} else {
					if start != "" || end != "" {
						cacctArgs = append(cacctArgs, "--start-time", start+"~"+end)
					}
				}
			}

			cacct.RootCmd.SetArgs(cacctArgs)
			err := cacct.RootCmd.Execute()
			if err != nil {
				exitWithCommandError("qacct", err)
			} else {
				os.Exit(util.ErrorSuccess)
			}
		},
	}

	addConfigPathFlag(cmd, &cacct.FlagConfigFilePath)
	cmd.Flags().StringVar(&FlagQacctA, "A", "", "List matching accounts.")
	cmd.Flags().StringVar(&FlagQacctB, "b", "", "Jobs started after begin_time.")
	cmd.Flags().IntVar(&FlagQacctD, "d", 0, "Jobs started during the last d days.")
	cmd.Flags().StringVar(&FlagQacctE, "e", "", "Jobs started before end_time.")
	cmd.Flags().BoolVar(&FlagQacctEEnd, "E", false, "Use job end time instead of start time.")
	cmd.Flags().StringVar(&FlagQacctJ, "j", "", "List matching jobs by id or name.")
	cmd.Flags().StringVar(&FlagQacctO, "o", "", "List matching owners.")
	cmd.Flags().StringVar(&FlagQacctQ, "q", "", "List matching queue.")
	cmd.Flags().StringVar(&FlagQacctU, "u", "", "List matching owners.")

	return cmd
}

func qstat() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "qstat [flags] [job_id ...]",
		Short:   "Wrapper of cqueue command",
		Long:    "Wrapper of cqueue command.",
		GroupID: "sge",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			if sgeHelpRequested(cmd) {
				_ = cmd.Help()
				os.Exit(util.ErrorSuccess)
			}

			if FlagQstatI && FlagQstatR {
				log.Error("qstat: error: options -i and -r are mutually exclusive")
				os.Exit(util.ErrorCmdArg)
			}

			cqueueArgs := make([]string, 0)
			singleLineMode := false

			if FlagQstatF {
				cqueueArgs = append(cqueueArgs, "--full")
			}
			if FlagQstatI {
				cqueueArgs = append(cqueueArgs, "--state", "pending,suspended")
				singleLineMode = true
			}
			if FlagQstatR {
				cqueueArgs = append(cqueueArgs, "--state", "running")
				singleLineMode = true
			}
			if FlagQstatQ != "" {
				cqueueArgs = append(cqueueArgs, "--partition", FlagQstatQ)
				singleLineMode = true
			}
			if FlagQstatU != "" && FlagQstatU != "*" {
				cqueueArgs = append(cqueueArgs, "--user", FlagQstatU)
				singleLineMode = true
			}
			if FlagQstatN {
				singleLineMode = true
			}
			if len(args) > 0 {
				cqueueArgs = append(cqueueArgs, "--job", strings.Join(args, ","))
			}

			if singleLineMode && !FlagQstatF {
				fmt.Printf("%s %s %s %s %s %s %s %s %s %s %s\n",
					"JOBID", "USER", "QUEUE", "NAME", "SESSID", "NDS", "TSK",
					"MEM", "REQTIME", "STATE", "ELAPSED")
				cqueueArgs = append(cqueueArgs, "--noheader")
				cqueueArgs = append(cqueueArgs, "--format", "%j %u %P %n - %N %C %M %l %t %e")
			}

			cqueue.RootCmd.SetArgs(cqueueArgs)
			err := cqueue.RootCmd.Execute()
			if err != nil {
				exitWithCommandError("qstat", err)
			} else {
				os.Exit(util.ErrorSuccess)
			}
		},
	}

	addConfigPathFlag(cmd, &cqueue.FlagConfigFilePath)
	cmd.Flags().BoolVar(&FlagQstatF, "f", false, "Display full job information.")
	cmd.Flags().BoolVar(&FlagQstatI, "i", false, "Display pending and suspended jobs.")
	cmd.Flags().BoolVar(&FlagQstatN, "n", false, "Use the single-line job display format with node information.")
	cmd.Flags().StringVar(&FlagQstatQ, "q", "", "Display jobs in the specified queue.")
	cmd.Flags().BoolVar(&FlagQstatR, "r", false, "Display running jobs.")
	cmd.Flags().StringVar(&FlagQstatU, "u", "", "Display jobs submitted by the specified user.")

	return cmd
}

func sgeHelpRequested(cmd *cobra.Command) bool {
	help, err := cmd.Flags().GetBool("help")
	return err == nil && help
}

func isSGEJobIDFilter(value string) bool {
	_, err := util.ParseJobIdSelectorList(value, ",")
	return err == nil
}

func buildQacctTimeRange() (string, string, error) {
	var start, end string

	if FlagQacctD > 0 {
		now := time.Now()
		start = now.AddDate(0, 0, -FlagQacctD).Format("2006-01-02T15:04:05")
		end = now.Format("2006-01-02T15:04:05")
	}

	if FlagQacctB != "" {
		beginAt, err := convertSGEDateTime(FlagQacctB)
		if err != nil {
			return "", "", err
		}
		start = beginAt
	}

	if FlagQacctE != "" {
		endAt, err := convertSGEDateTime(FlagQacctE)
		if err != nil {
			return "", "", err
		}
		end = endAt
	}

	return start, end, nil
}

func convertSGEDateTime(value string) (string, error) {
	parts := strings.Split(value, ".")
	if len(parts) > 2 {
		return "", fmt.Errorf("invalid -a value %q", value)
	}

	main := parts[0]
	seconds := 0
	if len(parts) == 2 {
		if len(parts[1]) != 2 {
			return "", fmt.Errorf("invalid -a value %q: seconds must be two digits", value)
		}
		sec, err := strconv.Atoi(parts[1])
		if err != nil {
			return "", fmt.Errorf("invalid -a value %q: %w", value, err)
		}
		seconds = sec
	}

	now := time.Now()
	var year, month, day, hour, minute int
	switch len(main) {
	case 8:
		year = now.Year()
		month = mustAtoi(main[0:2])
		day = mustAtoi(main[2:4])
		hour = mustAtoi(main[4:6])
		minute = mustAtoi(main[6:8])
	case 10:
		currentCentury := now.Year() / 100
		year = currentCentury*100 + mustAtoi(main[0:2])
		month = mustAtoi(main[2:4])
		day = mustAtoi(main[4:6])
		hour = mustAtoi(main[6:8])
		minute = mustAtoi(main[8:10])
	case 12:
		year = mustAtoi(main[0:4])
		month = mustAtoi(main[4:6])
		day = mustAtoi(main[6:8])
		hour = mustAtoi(main[8:10])
		minute = mustAtoi(main[10:12])
	default:
		return "", fmt.Errorf("invalid -a value %q: expected [[CC]YY]MMDDhhmm[.SS]", value)
	}

	ts := time.Date(year, time.Month(month), day, hour, minute, seconds, 0, time.Local)
	if ts.Year() != year || int(ts.Month()) != month || ts.Day() != day ||
		ts.Hour() != hour || ts.Minute() != minute || ts.Second() != seconds {
		return "", fmt.Errorf("invalid -a value %q", value)
	}

	return ts.Format("2006-01-02T15:04:05"), nil
}

func mustAtoi(value string) int {
	num, _ := strconv.Atoi(value)
	return num
}

func convertSGEMailType(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return "", nil
	}
	if strings.ContainsRune(normalized, 'n') {
		return "NONE", nil
	}

	mailTypes := make([]string, 0, 3)
	seen := make(map[string]struct{})

	for _, r := range normalized {
		var mailType string
		switch r {
		case 'b':
			mailType = "BEGIN"
		case 'e':
			mailType = "END"
		case 'a':
			mailType = "FAIL"
		case 's':
			return "", fmt.Errorf("unsupported -m value %q: mail option 's' is not supported", value)
		default:
			return "", fmt.Errorf("invalid -m value %q", value)
		}

		if _, ok := seen[mailType]; ok {
			continue
		}
		seen[mailType] = struct{}{}
		mailTypes = append(mailTypes, mailType)
	}

	return strings.Join(mailTypes, ","), nil
}
