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
	"CraneFrontEnd/internal/cinfo"
	"CraneFrontEnd/internal/cqueue"
	"CraneFrontEnd/internal/util"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type LSFWrapper struct {
}

func (w LSFWrapper) Group() *cobra.Group {
	return &cobra.Group{
		ID:    "lsf",
		Title: "LSF Commands:",
	}
}

func (w LSFWrapper) SubCommands() []*cobra.Command {
	return []*cobra.Command{
		bacct(),
		bsub(),
		bjobs(),
		bqueues(),
		bkill(),
	}
}

func (lsf LSFWrapper) HasCommand(cmd string) bool {
	return slices.Contains([]string{"bacct", "bsub", "bjobs", "bqueues", "bkill"}, cmd)
}

func (lsf LSFWrapper) Preprocess() error {
	/*
		LSF recognizes single dash option only. Whereas the CLI library cobra
		does not support defining something like `-env`.

		We imitate that behavior by preprocess the `os.Args`, changing all
		single dash options to double dash.
	*/
	for i, v := range os.Args {
		// Skip program name and subcommand
		if i <= 1 {
			continue
		}
		if v == "--" || v == "-h" {
			break
		} else if len(v) >= 2 && v[0] == '-' && v[1] != '-' {
			os.Args[i] = "-" + v
		}
	}

	return nil
}

var (
	FlagBacct_C string
	FlagBacct_D string
	FlagBacct_S string
	FlagBacct_u string
	FlagBacct_d bool
	FlagBacct_e bool
	FlagBacct_q string
	FlagBacct_m string
	FlagBacct_M string
	FlagBacct_b bool
	FlagBacct_l bool
)

func bacct() *cobra.Command {
	// bacct: args represent job ids
	// cacct: no args
	cmd := &cobra.Command{
		Use:     "bacct [flags] [job_id ...]",
		Short:   "Wrapper of cacct command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cacctArgs := make([]string, 0)
			if FlagBacct_C != "" {
				cacctArgs = append(cacctArgs, "--end-time", ConvertInterval(FlagBacct_C))
			}
			if FlagBacct_D != "" {
				cacctArgs = append(cacctArgs, "--start-time", ConvertInterval(FlagBacct_D))
			}
			if FlagBacct_S != "" {
				cacctArgs = append(cacctArgs, "--submit-time", ConvertInterval(FlagBacct_S))
			}
			if FlagBacct_u != "" {
				cacctArgs = append(cacctArgs, "--user", strings.ReplaceAll(FlagBacct_u, " ", ","))
			}
			states := []string{}
			if FlagBacct_d {
				states = append(states, "completed")
			}
			if FlagBacct_e {
				states = append(states, "failed", "cancelled", "time-limit-exceeded")
			}
			if len(states) > 0 {
				cacctArgs = append(cacctArgs, "--state", strings.Join(states, ","))
			}
			if FlagBacct_q != "" {
				cacctArgs = append(cacctArgs, "--partition", FlagBacct_q)
			}
			if FlagBacct_m != "" {
				cacctArgs = append(cacctArgs, "--nodelist", FlagBacct_m)
			}
			if FlagBacct_M != "" {
				data, err := os.ReadFile(FlagBacct_M)
				if err != nil {
					os.Exit(util.ErrorCmdArg)
				}
				hosts := strings.Fields(string(data))
				if len(hosts) == 0 {
					os.Exit(util.ErrorCmdArg)
				}
				cacctArgs = append(cacctArgs, "--nodelist", strings.Join(hosts, ","))
			}
			if FlagBacct_b {
				cacctArgs = append(cacctArgs, "--noheader")
			}
			if FlagBacct_l {
				cacctArgs = append(cacctArgs, "--full")
			}

			if len(args) == 1 && args[0] == "0" {
				cacctArgs = append(cacctArgs, "--job", "")
			} else if len(args) > 0 {
				cacctArgs = append(cacctArgs, "--job", strings.Join(args, ","))
			}

			cacct.RootCmd.SetArgs(cacctArgs)
			err := cacct.RootCmd.Execute()
			if err != nil {
				exitWithCommandError("bacct", err)
			} else {
				os.Exit(util.ErrorSuccess)
			}
		},
	}

	addConfigPathFlag(cmd, &cacct.FlagConfigFilePath)
	cmd.Flags().StringVar(&FlagBacct_C, "C", "", "Displays accounting statistics for jobs that completed or exited during the specified time interval.")
	cmd.Flags().StringVar(&FlagBacct_D, "D", "", "Displays accounting statistics for jobs that are dispatched during the specified time interval.")
	cmd.Flags().StringVar(&FlagBacct_S, "S", "", "Displays accounting statistics for jobs submitted during the specified time interval.")
	cmd.Flags().StringVar(&FlagBacct_u, "u", "", "Displays accounting statistics for jobs that are submitted by the specified users, or by all users if the keyword all is specified.")
	cmd.Flags().BoolVar(&FlagBacct_d, "d", false, "Displays accounting statistics for successfully completed jobs (with a DONE status).")
	cmd.Flags().BoolVar(&FlagBacct_e, "e", false, "Displays accounting statistics for exited jobs (with an EXIT status).")
	cmd.Flags().StringVar(&FlagBacct_q, "q", "", "Displays accounting statistics for jobs that are submitted to the specified queues.")
	cmd.Flags().StringVar(&FlagBacct_m, "m", "", "Displays accounting statistics for jobs that are dispatched to the specified hosts.")
	cmd.Flags().StringVar(&FlagBacct_M, "M", "", "Displays accounting statistics for jobs that are dispatched to the hosts listed in a file")
	cmd.Flags().BoolVar(&FlagBacct_b, "b", false, "Brief format.")
	cmd.Flags().BoolVar(&FlagBacct_l, "l", false, "Long format.")
	return cmd
}

var (
	FlagBsub_nnodes string
	FlagBsub_n      string
	FlagBsub_W      string
	FlagBsub_M      string
	FlagBsub_q      string
	FlagBsub_J      string
	FlagBsub_cwd    string
	FlagBsub_Lp     string
	FlagBsub_m      string
	FlagBsub_L      string
	FlagBsub_env    string
	FlagBsub_i      string
	FlagBsub_o      string
	FlagBsub_e      string
	FlagBsub_ext    string
	FlagBsub_B      bool
	FlagBsub_N      bool
	FlagBsub_Ne     bool
	FlagBsub_u      string
	FlagBsub_Jd     string
	FlagBsub_json   bool
	FlagBsub_oo     string
	FlagBsub_U      string
	FlagBsub_x      bool
	FlagBsub_H      bool
	FlagBsub_b      string
	FlagBsub_w      string
	FlagBsub_t      string
)

func bsub() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "bsub [flags] file",
		Short:   "Wrapper of cbatch command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cbatchArgs := make([]string, 0)
			if FlagBsub_nnodes != "" {
				cbatchArgs = append(cbatchArgs, "--nodes", FlagBsub_nnodes)
			}
			if FlagBsub_n != "" {
				cbatchArgs = append(cbatchArgs, "--ntasks", FlagBsub_n)
			}
			if FlagBsub_W != "" {
				cbatchArgs = append(cbatchArgs, "--time", cbatch.ConvertLSFRuntimeLimit(FlagBsub_W))
			}
			if FlagBsub_M != "" {
				cbatchArgs = append(cbatchArgs, "--mem", FlagBsub_M)
			}
			if FlagBsub_q != "" {
				cbatchArgs = append(cbatchArgs, "--partition", FlagBsub_q)
			}
			if FlagBsub_J != "" {
				cbatchArgs = append(cbatchArgs, "--job-name", FlagBsub_J)
			}
			if FlagBsub_cwd != "" {
				cbatchArgs = append(cbatchArgs, "--chdir", FlagBsub_cwd)
			}
			if FlagBsub_Lp != "" {
				cbatchArgs = append(cbatchArgs, "--licenses", FlagBsub_Lp)
			}
			if FlagBsub_m != "" {
				cbatchArgs = append(cbatchArgs, "--nodelist", strings.ReplaceAll(FlagBsub_m, " ", ","))
			}
			if FlagBsub_L != "" {
				cbatchArgs = append(cbatchArgs, "--get-user-env")
			}
			if FlagBsub_env != "" {
				cbatchArgs = append(cbatchArgs, "--export", FlagBsub_env)
			}
			if FlagBsub_i != "" {
				cbatchArgs = append(cbatchArgs, "--input", FlagBsub_i)
			}
			if FlagBsub_o != "" {
				cbatchArgs = append(cbatchArgs, "--output", FlagBsub_o)
			}
			if FlagBsub_e != "" {
				cbatchArgs = append(cbatchArgs, "--error", FlagBsub_e)
			}
			if FlagBsub_ext != "" {
				cbatchArgs = append(cbatchArgs, "--extra-attr", FlagBsub_ext)
			}
			mailTypes := make([]string, 0)
			if FlagBsub_B {
				mailTypes = append(mailTypes, "BEGIN")
			}
			if FlagBsub_N {
				mailTypes = append(mailTypes, "END")
			}
			if FlagBsub_Ne {
				mailTypes = append(mailTypes, "FAIL")
			}
			if len(mailTypes) > 0 {
				cbatchArgs = append(cbatchArgs, "--mail-type", strings.Join(mailTypes, ","))
			}
			if FlagBsub_u != "" {
				cbatchArgs = append(cbatchArgs, "--mail-user", FlagBsub_u)
			}
			if FlagBsub_Jd != "" {
				cbatchArgs = append(cbatchArgs, "--comment", FlagBsub_Jd)
			}
			if FlagBsub_json {
				cbatchArgs = append(cbatchArgs, "--json")
			}
			if FlagBsub_oo != "" {
				cbatchArgs = append(cbatchArgs, "--open-mode", FlagBsub_oo)
			}
			if FlagBsub_U != "" {
				cbatchArgs = append(cbatchArgs, "--reservation", FlagBsub_U)
			}
			if FlagBsub_x {
				cbatchArgs = append(cbatchArgs, "--exclusive")
			}
			if FlagBsub_H {
				cbatchArgs = append(cbatchArgs, "--hold")
			}
			if FlagBsub_b != "" {
				cbatchArgs = append(cbatchArgs, "--begin", FlagBsub_b)
			}
			if FlagBsub_w != "" {
				cbatchArgs = append(cbatchArgs, "--dependency", FlagBsub_w)
			}
			if FlagBsub_t != "" {
				cbatchArgs = append(cbatchArgs, "--deadline", FlagBsub_t)
			}
			cbatchArgs = append(cbatchArgs, args[0])
			cbatch.RootCmd.SetArgs(cbatchArgs)
			err := cbatch.RootCmd.Execute()
			if err != nil {
				exitWithCommandError("bsub", err)
			} else {
				os.Exit(util.ErrorSuccess)
			}
		},
	}

	addConfigPathFlag(cmd, &cbatch.FlagConfigFilePath)
	cmd.Flags().StringVar(&FlagBsub_nnodes, "nnode", "", "Specifies the number of compute nodes that are required for the job")
	cmd.Flags().StringVar(&FlagBsub_n, "n", "", "")
	cmd.Flags().StringVar(&FlagBsub_W, "W", "", "")
	cmd.Flags().StringVar(&FlagBsub_M, "M", "", "")
	cmd.Flags().StringVar(&FlagBsub_q, "q", "", "")
	cmd.Flags().StringVar(&FlagBsub_J, "J", "", "")
	cmd.Flags().StringVar(&FlagBsub_cwd, "cwd", "", "")
	cmd.Flags().StringVar(&FlagBsub_Lp, "Lp", "", "")
	cmd.Flags().StringVar(&FlagBsub_m, "m", "", "")
	cmd.Flags().StringVar(&FlagBsub_L, "L", "", "")
	cmd.Flags().StringVar(&FlagBsub_env, "env", "", "")
	cmd.Flags().StringVar(&FlagBsub_i, "i", "", "")
	cmd.Flags().StringVar(&FlagBsub_o, "o", "", "")
	cmd.Flags().StringVar(&FlagBsub_e, "e", "", "")
	cmd.Flags().StringVar(&FlagBsub_ext, "ext", "", "")
	cmd.Flags().BoolVar(&FlagBsub_B, "B", false, "Sends mail to you when the job is dispatched and begins execution")
	cmd.Flags().BoolVar(&FlagBsub_N, "N", false, "Sends the job report to you by mail when the job finishes")
	cmd.Flags().BoolVar(&FlagBsub_Ne, "Ne", false, "Sends the job report to you by mail when the job failed")
	cmd.Flags().StringVar(&FlagBsub_u, "u", "", "")
	cmd.Flags().StringVar(&FlagBsub_Jd, "Jd", "", "")
	cmd.Flags().BoolVar(&FlagBsub_json, "json", false, "")
	cmd.Flags().StringVar(&FlagBsub_oo, "oo", "", "")
	cmd.Flags().StringVar(&FlagBsub_U, "U", "", "")
	cmd.Flags().BoolVar(&FlagBsub_x, "x", false, "")
	cmd.Flags().BoolVar(&FlagBsub_H, "H", false, "")
	cmd.Flags().StringVar(&FlagBsub_b, "b", "", "")
	cmd.Flags().StringVar(&FlagBsub_w, "w", "", "")
	cmd.Flags().StringVar(&FlagBsub_t, "t", "", "")

	return cmd
}

var (
	FlagBjobs_a        bool
	FlagBjobs_l        bool
	FlagBjobs_noheader bool
	FlagBjobs_u        string
	FlagBjobs_q        string
	FlagBjobs_J        string
	FlagBjobs_r        bool
	FlagBjobs_p        bool
	FlagBjobs_json     bool
)

func bjobs() *cobra.Command {
	// bjobs: args represent job ids
	// cqueue: no args
	cmd := &cobra.Command{
		Use:     "bjobs [flags] [job_id ...]",
		Short:   "Wrapper of cqueue command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cqueueArgs := make([]string, 0)
			if FlagBjobs_l {
				cqueueArgs = append(cqueueArgs, "--full")
			}
			if FlagBjobs_noheader || FlagBjobs_json {
				cqueueArgs = append(cqueueArgs, "--noheader")
			} else {
				fmt.Printf("%s %s %s %s %s %s %s %s\n",
					"JOBID", "USER", "STAT", "QUEUE", "FROM_HOST", "EXEC_HOST", "JOB_NAME", "SUBMIT_TIME")
				cqueueArgs = append(cqueueArgs, "--noheader")
			}
			if FlagBjobs_u != "" {
				cqueueArgs = append(cqueueArgs, "--user", FlagBjobs_u)
			}

			if FlagBjobs_q != "" {
				cqueueArgs = append(cqueueArgs, "--partition", FlagBjobs_q)

			}

			if FlagBjobs_J != "" {
				cqueueArgs = append(cqueueArgs, "--name", FlagBjobs_J)

			}
			state := make([]string, 0)
			if FlagBjobs_r {
				state = append(state, "running")
			}
			if FlagBjobs_p {
				state = append(state, "pending")

			}
			if len(state) > 0 {
				cqueueArgs = append(cqueueArgs, "--state", strings.Join(state, ","))
			}
			if FlagBjobs_json {
				cqueueArgs = append(cqueueArgs, "--json")
			}
			if !FlagBjobs_json {
				cqueueArgs = append(cqueueArgs, "--format", "%j %u %t %P %L %L %n %s")
			}
			if len(args) > 0 {
				cqueueArgs = append(cqueueArgs, "--job", strings.Join(args, ","))
			}
			cqueue.RootCmd.SetArgs(cqueueArgs)
			err := cqueue.RootCmd.Execute()
			if err != nil {
				exitWithCommandError("bjobs", err)
			} else {
				os.Exit(util.ErrorSuccess)
			}

		},
	}

	addConfigPathFlag(cmd, &cqueue.FlagConfigFilePath)
	cmd.Flags().BoolVar(&FlagBjobs_a, "a", false, "")
	cmd.Flags().BoolVar(&FlagBjobs_l, "l", false, "")
	cmd.Flags().BoolVar(&FlagBjobs_noheader, "noheader", false, "")
	cmd.Flags().StringVar(&FlagBjobs_u, "u", "", "")
	cmd.Flags().StringVar(&FlagBjobs_q, "q", "", "")
	cmd.Flags().StringVar(&FlagBjobs_J, "J", "", "")
	cmd.Flags().BoolVar(&FlagBjobs_r, "r", false, "")
	cmd.Flags().BoolVar(&FlagBjobs_p, "p", false, "")
	cmd.Flags().BoolVar(&FlagBjobs_json, "json", false, "")
	return cmd
}

var (
	FlagBqueues_noheader bool
	FlagBqueues_json     bool
	FlagBqueues_m        string
	FlagBqueues_l        bool
)

func bqueues() *cobra.Command {
	// bqueues: args represent queue names
	// cinfo: no args
	cmd := &cobra.Command{
		Use:     "bqueues [flags] [queue_name ...]",
		Short:   "Wrapper of cinfo command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cinfoArgs := make([]string, 0)
			if FlagBqueues_noheader {
				cinfoArgs = append(cinfoArgs, "--noheader")
			}
			if FlagBqueues_json {
				cinfoArgs = append(cinfoArgs, "--json")
			}
			if FlagBqueues_m != "" {
				cinfoArgs = append(cinfoArgs, "--nodes", strings.ReplaceAll(FlagBqueues_m, " ", ","))
			}
			if FlagBqueues_l {
				cinfoArgs = append(cinfoArgs, "--format", "%p %a %n %s %l")
			}
			if len(args) > 0 {
				cinfoArgs = append(cinfoArgs, "--partition", strings.Join(args, ","))
			}

			cinfo.RootCmd.SetArgs(cinfoArgs)
			err := cinfo.RootCmd.Execute()
			if err != nil {
				exitWithCommandError("bqueues", err)
			} else {
				os.Exit(util.ErrorSuccess)
			}
		},
	}

	addConfigPathFlag(cmd, &cinfo.FlagConfigFilePath)
	cmd.Flags().BoolVar(&FlagBqueues_noheader, "noheader", false, "")
	cmd.Flags().BoolVar(&FlagBqueues_json, "json", false, "")
	cmd.Flags().StringVar(&FlagBqueues_m, "m", "", "")
	cmd.Flags().BoolVar(&FlagBqueues_l, "l", false, "")

	return cmd
}

var (
	FlagBkill_J    string
	FlagBkill_q    string
	FlagBkill_stat string
	FlagBkill_u    string
	FlagBkill_m    string
)

func bkill() *cobra.Command {
	// bkill: args represent job ids
	// ccancel: 1 arg, format "job_id1,job_id2,.."
	cmd := &cobra.Command{
		Use:     "bkill [flags] [job_id ...]",
		Short:   "Wrapper of ccancel command",
		Long:    "",
		GroupID: "lsf",
		Args:    cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, args []string) {
			ccancelArgs := make([]string, 0)
			if FlagBkill_J != "" {
				ccancelArgs = append(ccancelArgs, "--name", FlagBkill_J)
			}
			if FlagBkill_q != "" {
				ccancelArgs = append(ccancelArgs, "--partition", FlagBkill_q)
			}
			if FlagBkill_stat != "" {
				state := ""
				switch FlagBkill_stat {
				case "run":
					state = "running"
				case "pend":
					state = "pending"
				case "susp":
					state = "suspended"
				default:
					os.Exit(util.ErrorCmdArg)
				}
				ccancelArgs = append(ccancelArgs, "--state", state)
			}
			if FlagBkill_u != "" {
				ccancelArgs = append(ccancelArgs, "--user", FlagBkill_u)
			}
			if FlagBkill_m != "" {
				ccancelArgs = append(ccancelArgs, "--nodes", FlagBkill_m)
			}
			if len(args) > 0 {
				ccancelArgs = append(ccancelArgs, strings.Join(args, ","))
			}
			ccancel.RootCmd.SetArgs(ccancelArgs)
			err := ccancel.RootCmd.Execute()
			if err != nil {
				exitWithCommandError("bkill", err)
			} else {
				os.Exit(util.ErrorSuccess)
			}
		},
	}

	addConfigPathFlag(cmd, &ccancel.FlagConfigFilePath)
	cmd.Flags().StringVar(&FlagBkill_J, "J", "", "Operates only on jobs with the specified job name")
	cmd.Flags().StringVar(&FlagBkill_u, "u", "", "Operates only on jobs that are submitted by the specified user")
	cmd.Flags().StringVar(&FlagBkill_q, "q", "", "Operates only on jobs in the specified queue (partition)")
	cmd.Flags().StringVar(&FlagBkill_m, "m", "", "Operates only on jobs that are dispatched to the specified host")
	cmd.Flags().StringVar(&FlagBkill_stat, "stat", "", "Operates only on jobs in the specified status")

	return cmd
}

func ConvertInterval(t string) string {
	if t == "" {
		return t
	}
	ts := strings.Split(t, ",")
	if len(ts) == 1 {
		log.Fatal("bacct: error: Invalid LSF time format")
	}
	t1, t2 := ts[0], ts[1]
	t1, err1 := ConvertTime(t1, "left")
	if err1 != nil {
		log.Fatalf("bacct: error: Failed to parse LSF time format: %s", err1)
	}
	t2, err2 := ConvertTime(t2, "right")
	if err2 != nil {
		log.Fatalf("bacct: error: Failed to parse LSF time format: %s", err2)
	}
	return t1 + "~" + t2
}

func ConvertTime(t string, side string) (string, error) {
	curTime := time.Now()
	if t == "" || t == "." {
		return "", nil
	}
	// [year/][month/][day][/hour:minute|/hour:]
	re := regexp.MustCompile(`(?:(\d{4})/)?(?:(\d+)/)?(\d+)?(?:/(\d+)\:(\d+)?)?`)
	x := re.FindStringSubmatch(t)
	if x[0] != t {
		return t, errors.New("invalid LSF time format: " + t)
	}
	y, m, d, H, M := x[1], x[2], x[3], x[4], x[5]
	var S string
	if side == "left" {
		S = "00"
	} else if side == "right" {
		S = "59"
	} else {
		return t, errors.New("invalid side")
	}
	if y == "" {
		y = strconv.Itoa(curTime.Year())
	}
	if m == "" {
		m = strconv.Itoa(int(curTime.Month()))
	}
	if d == "" {
		d = strconv.Itoa(curTime.Day())
	}
	if H == "" {
		if side == "left" {
			H = "00"
		} else if side == "right" {
			H = "23"
		}
	}
	if M == "" {
		if side == "left" {
			M = "00"
		} else if side == "right" {
			M = "59"
		}
	}
	return fmt.Sprintf("%04s-%02s-%02sT%02s:%02s:%s", y, m, d, H, M, S), nil
}
