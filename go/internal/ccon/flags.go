/**
 * Copyright (c) 2025 Peking University and Peking University
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

package ccon

import (
	"github.com/spf13/pflag"
)

type GlobalFlags struct {
	ConfigPath string
	Json       bool
	DebugLevel string
}

// CraneFlags is a special group of flags that are only valid for the 'run' command
// but exists at the root level to avoid collision with docker-style flags
type CraneFlags struct {
	Mem           string
	Gres          string
	Partition     string
	Nodelist      string
	Time          string
	Account       string
	Qos           string
	Nodes         uint32
	NtasksPerNode uint32
	Ntasks        uint32
	CpusPerTask   float64
	Exclusive     bool
	Hold          bool
	Reservation   string
	Excludes      string
	ExtraAttr     string
	MailType      string
	MailUser      string
	Comment       string
}

type RunFlags struct {
	Name                string
	Ports               []string
	Env                 []string
	Volume              []string
	Detach              bool
	Interactive         bool
	Tty                 bool
	Entrypoint          string
	User                string
	UserNS              bool
	Network             string
	Workdir             string
	Cpus                float64
	Memory              string
	Gpus                string
	PullPolicy          string
	ImagePullingTimeout int64
	Dns                 []string
}

type StopFlags struct {
	Timeout int
}

type PsFlags struct {
	All   bool
	Quiet bool
}

type PodFlags struct {
	All   bool
	Quiet bool
}

type LogFlags struct {
	Follow     bool
	Tail       int
	Timestamps bool
	Since      string
	Until      string
	TargetNode string
}

type LoginFlags struct {
	Username      string
	Password      string
	PasswordStdin bool
}

type AttachFlags struct {
	Stdin      bool
	Stdout     bool
	Stderr     bool
	Tty        bool
	Transport  string
	TargetNode string
}

type ExecFlags struct {
	Interactive bool
	Tty         bool
	Transport   string
	TargetNode  string
}

type WaitFlags struct {
	Interval int
}

type Flags struct {
	Global GlobalFlags
	Crane  CraneFlags
	Run    RunFlags
	Stop   StopFlags
	Ps     PsFlags
	Pod    PodFlags
	Log    LogFlags
	Login  LoginFlags
	Attach AttachFlags
	Exec   ExecFlags
	Wait   WaitFlags

	flagSetCrane *pflag.FlagSet
}

var flags = &Flags{}

func GetFlags() *Flags {
	return flags
}

func IsCraneFlag(flag *pflag.Flag) bool {
	if flag == nil {
		return false
	}
	category, ok := flag.Annotations["category"]
	return ok && len(category) > 0 && category[0] == "crane"
}

func (f *Flags) InitializeCraneFlags() {
	f.flagSetCrane = pflag.NewFlagSet("crane", pflag.ExitOnError)

	f.flagSetCrane.StringVar(&f.Crane.Mem, "mem", "", "Maximum amount of real memory, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB")
	f.flagSetCrane.StringVar(&f.Crane.Gres, "gres", "", "Gres required per node, format: \"gpu:a100:1\" or \"gpu:1\"")
	f.flagSetCrane.Float64VarP(&f.Crane.CpusPerTask, "cpus-per-task", "c", 1, "Number of cpus required per task")

	f.flagSetCrane.StringVarP(&f.Crane.Partition, "partition", "p", "", "Partition for job scheduling")
	f.flagSetCrane.StringVarP(&f.Crane.Nodelist, "nodelist", "w", "", "Nodes to be allocated to the job (comma-separated list or hostlist expression)")
	f.flagSetCrane.StringVarP(&f.Crane.Time, "time", "t", "", "Time limit, format: \"day-hours:minutes:seconds\" or \"hours:minutes:seconds\"")
	f.flagSetCrane.StringVarP(&f.Crane.Account, "account", "A", "", "Account used for the job")
	f.flagSetCrane.StringVarP(&f.Crane.Qos, "qos", "q", "", "QoS used for the job")
	f.flagSetCrane.Uint32VarP(&f.Crane.Nodes, "nodes", "N", 1, "Number of nodes requested")
	f.flagSetCrane.Uint32Var(&f.Crane.NtasksPerNode, "ntasks-per-node", 0, "Maximum number of tasks per node (default: unlimited)")
	f.flagSetCrane.Uint32VarP(&f.Crane.Ntasks, "ntasks", "n", 1, "Total number of tasks")
	f.flagSetCrane.StringVarP(&f.Crane.Excludes, "exclude", "x", "", "Exclude specific nodes from allocating (comma-separated list or hostlist expression)")
	f.flagSetCrane.StringVarP(&f.Crane.Reservation, "reservation", "r", "", "Use reserved resources")
	f.flagSetCrane.BoolVar(&f.Crane.Exclusive, "exclusive", false, "Exclusive node resources")
	f.flagSetCrane.BoolVarP(&f.Crane.Hold, "hold", "H", false, "Hold the job until it is released")

	f.flagSetCrane.StringVar(&f.Crane.ExtraAttr, "extra-attr", "", "Extra attributes of the job (in JSON format)")
	f.flagSetCrane.StringVar(&f.Crane.MailType, "mail-type", "", "Notify user by mail when certain events occur, supported values: NONE, BEGIN, END, FAIL, TIMELIMIT, ALL")
	f.flagSetCrane.StringVar(&f.Crane.MailUser, "mail-user", "", "Mail address of the notification receiver")
	f.flagSetCrane.StringVar(&f.Crane.Comment, "comment", "", "Comment of the job")

	f.flagSetCrane.VisitAll(func(flag *pflag.Flag) {
		flag.Annotations = map[string][]string{
			"category": {"crane"},
		}
	})
}

func (f *Flags) GetCraneFlagSet() *pflag.FlagSet {
	if f.flagSetCrane == nil {
		f.InitializeCraneFlags()
	}
	return f.flagSetCrane
}
