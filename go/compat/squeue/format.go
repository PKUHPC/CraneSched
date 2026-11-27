// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	pb "github.com/PKUHPC/CraneSched/go/generated/protos"
)

type field struct {
	name, header string
	code         byte
}
type catalog struct {
	ordered []field
	names   map[string]field
	codes   map[byte]field
}

func registry(fields []field, shortOnly field) catalog {
	r := catalog{ordered: fields, names: map[string]field{}, codes: map[byte]field{}}
	for _, f := range fields {
		if f.code != 0 {
			r.codes[f.code] = f
		}
		r.names[strings.ToLower(f.name)] = f
	}
	// Short-only fields remain accepted even though --helpformat omits them.
	r.codes[shortOnly.code] = shortOnly
	return r
}

// Field names, headings and short codes match the Slurm 26.05.4 interface.
var jobs = registry([]field{
	{name: "Account", header: "ACCOUNT", code: 'a'},
	{name: "AccrueTime", header: "ACCRUE_TIME"},
	{name: "admin_comment", header: "ADMIN_COMMENT"},
	{name: "AllocNodes", header: "ALLOC_NODES"},
	{name: "AllocSID", header: "ALLOC_SID"},
	{name: "ArrayJobId", header: "ARRAY_JOB_ID", code: 'F'},
	{name: "ArrayTaskId", header: "ARRAY_TASK_ID", code: 'K'},
	{name: "AssocId", header: "ASSOC_ID"},
	{name: "BatchFlag", header: "BATCH_FLAG"},
	{name: "BatchHost", header: "EXEC_HOST", code: 'B'},
	{name: "BoardsPerNode", header: "BOARDS_PER_NODE"},
	{name: "BurstBuffer", header: "BURST_BUFFER"},
	{name: "BurstBufferState", header: "BURST_BUFFER_STATE"},
	{name: "Cluster", header: "CLUSTER"},
	{name: "ClusterFeature", header: "CLUSTER_FEATURES"},
	{name: "Command", header: "COMMAND", code: 'o'},
	{name: "Comment", header: "COMMENT", code: 'k'},
	{name: "Container", header: "CONTAINER"},
	{name: "ContainerId", header: "CONTAINERID"},
	{name: "ContainerType", header: "CONTAINERTYPE"},
	{name: "Contiguous", header: "CONTIGUOUS", code: 'O'},
	{name: "Cores", header: "CORES_PER_SOCKET", code: 'I'},
	{name: "CoreSpec", header: "CORE_SPEC", code: 'X'},
	{name: "CPUsPerTask", header: "CPUS_PER_TASK"},
	{name: "cpus-per-task", header: "CPUS_PER_TASK"},
	{name: "cpus-per-tres", header: "CPUS_PER_TRES"},
	{name: "CronJob", header: "CRON_JOB"},
	{name: "Deadline", header: "DEADLINE"},
	{name: "DelayBoot", header: "DELAY_BOOT"},
	{name: "Dependency", header: "DEPENDENCY", code: 'E'},
	{name: "DerivedEC", header: "DERIVED_EC"},
	{name: "EligibleTime", header: "ELIGIBLE_TIME"},
	{name: "EndTime", header: "END_TIME", code: 'e'},
	{name: "ExcNodes", header: "EXC_NODES", code: 'x'},
	{name: "Exclusive", header: "EXCLUSIVE"},
	{name: "exit_code", header: "EXIT_CODE"},
	{name: "Feature", header: "FEATURES", code: 'f'},
	{name: "GroupId", header: "GROUP", code: 'G'},
	{name: "GroupName", header: "GROUP", code: 'g'},
	{name: "HetJobId", header: "HET_JOB_ID"},
	{name: "HetJobIdSet", header: "HET_JOB_ID_SET"},
	{name: "HetJobOffset", header: "HET_JOB_OFFSET"},
	{name: "JobArrayId", header: "JOBID", code: 'i'},
	{name: "JobId", header: "JOBID", code: 'A'},
	{name: "LastSchedEval", header: "LAST_SCHED_EVAL"},
	{name: "Licenses", header: "LICENSES", code: 'W'},
	{name: "LicensesAlloc", header: "LICENSES_ALLOC"},
	{name: "MaxCPUs", header: "MAX_CPUS"},
	{name: "MaxNodes", header: "MAX_NODES"},
	{name: "mem-per-tres", header: "MEM_PER_TRES"},
	{name: "MCSLabel", header: "MCSLABEL"},
	{name: "MinCPUs", header: "MIN_CPUS", code: 'c'},
	{name: "MinMemory", header: "MIN_MEMORY", code: 'm'},
	{name: "MinTime", header: "TIME_MIN"},
	{name: "MinTmpDisk", header: "MIN_TMP_DISK", code: 'd'},
	{name: "Name", header: "NAME", code: 'j'},
	{name: "Network", header: "NETWORK"},
	{name: "Nice", header: "NICE", code: 'y'},
	{name: "NodeList", header: "NODELIST", code: 'N'},
	{name: "NTPerCore", header: "NTASKS_PER_CORE"},
	{name: "NTPerNode", header: "NTASKS_PER_NODE"},
	{name: "NTPerSocket", header: "NTASKS_PER_SOCKET"},
	{name: "NTPerBoard", header: "NTASKS_PER_BOARD"},
	{name: "NumCPUs", header: "CPUS", code: 'C'},
	{name: "NumNodes", header: "NODES", code: 'D'},
	{name: "NumTasks", header: "TASKS"},
	{name: "Origin", header: "ORIGIN"},
	{name: "OriginRaw", header: "ORIGIN_RAW"},
	{name: "OverSubscribe", header: "OVER_SUBSCRIBE", code: 'h'},
	{name: "Partition", header: "PARTITION", code: 'P'},
	{name: "PendingTime", header: "PENDING_TIME"},
	{name: "PreemptTime", header: "PREEMPT_TIME"},
	{name: "Prefer", header: "PREFER"},
	{name: "Priority", header: "PRIORITY", code: 'p'},
	{name: "PriorityLong", header: "PRIORITY", code: 'Q'},
	{name: "Profile", header: "PROFILE"},
	{name: "QOS", header: "QOS", code: 'q'},
	{name: "Reason", header: "REASON", code: 'r'},
	{name: "ReasonList", header: "NODELIST(REASON)", code: 'R'},
	{name: "Reboot", header: "REBOOT"},
	{name: "ReqNodes", header: "REQ_NODES", code: 'n'},
	{name: "ReqSwitch", header: "REQ_SWITCH"},
	{name: "Requeue", header: "REQUEUE"},
	{name: "Reservation", header: "RESERVATION", code: 'v'},
	{name: "ResizeTime", header: "RESIZE_TIME"},
	{name: "RestartCnt", header: "RESTART_COUNT"},
	{name: "SchedNodes", header: "SCHEDNODES", code: 'Y'},
	{name: "SCT", header: "S:C:T", code: 'z'},
	{name: "SegmentSize", header: "SEGMENT_SIZE"},
	{name: "SiblingsActive", header: "ACTIVE_SIBLINGS"},
	{name: "SiblingsActiveRaw", header: "ACTIVE_SIBLINGS_RAW"},
	{name: "SiblingsViable", header: "VIABLE_SIBLINGS"},
	{name: "SiblingsViableRaw", header: "VIALBLE_SIBLINGS_RAW"},
	{name: "SLUID", header: "SLUID", code: 's'},
	{name: "Sockets", header: "SOCKETS_PER_NODE", code: 'H'},
	{name: "SPerBoard", header: "SOCKETS_PER_BOARD"},
	{name: "StartTime", header: "START_TIME", code: 'S'},
	{name: "State", header: "STATE", code: 'T'},
	{name: "StateCompact", header: "ST", code: 't'},
	{name: "StdErr", header: "STDERR"},
	{name: "StdIn", header: "STDIN"},
	{name: "StdOut", header: "STDOUT"},
	{name: "SubmitTime", header: "SUBMIT_TIME", code: 'V'},
	{name: "system_comment", header: "SYSTEM_COMMENT"},
	{name: "Threads", header: "THREADS_PER_CORE", code: 'J'},
	{name: "TimeLeft", header: "TIME_LEFT", code: 'L'},
	{name: "TimeLimit", header: "TIME_LIMIT", code: 'l'},
	{name: "TimeUsed", header: "TIME", code: 'M'},
	{name: "tres-alloc", header: "TRES_ALLOC"},
	{name: "tres-bind", header: "TRES_BIND"},
	{name: "tres-freq", header: "TRES_FREQ"},
	{name: "tres-per-job", header: "TRES_PER_JOB"},
	{name: "tres-per-socket", header: "TRES_PER_SOCKET"},
	{name: "tres-per-task", header: "TRES_PER_TASK"},
	{name: "UserId", header: "UID", code: 'U'},
	{name: "UserName", header: "USER", code: 'u'},
	{name: "Wait4Switch", header: "WAIT4SWITCH"},
	{name: "WCKey", header: "WCKEY", code: 'w'},
	{name: "WorkDir", header: "WORK_DIR", code: 'Z'},
}, field{name: "tres-per-node", header: "TRES_PER_NODE", code: 'b'})

var steps = registry([]field{
	{name: "ArrayJobId", header: "ARRAY_JOB_ID"},
	{name: "ArrayTaskId", header: "ARRAY_TASK_ID"},
	{name: "Cluster", header: "CLUSTER"},
	{name: "Container", header: "CONTAINER"},
	{name: "ContainerId", header: "CONTAINERID"},
	{name: "ContainerType", header: "CONTAINERTYPE"},
	{name: "CPUFreq", header: "CPU_FREQ"},
	{name: "cpus-per-tres", header: "CPUS_PER_TRES"},
	{name: "JobId", header: "JOB_ID"},
	{name: "mem-per-tres", header: "MEM_PER_TRES"},
	{name: "Network", header: "NETWORK"},
	{name: "Nodes", header: "NODELIST", code: 'N'},
	{name: "NumCPUs", header: "NUM_CPUS"},
	{name: "NumTasks", header: "TASKS", code: 'A'},
	{name: "Partition", header: "PARTITION", code: 'P'},
	{name: "ResvPorts", header: "RESERVED_PORTS"},
	{name: "StartTime", header: "START_TIME", code: 'S'},
	{name: "StdErr", header: "STDERR"},
	{name: "StdIn", header: "STDIN"},
	{name: "StdOut", header: "STDOUT"},
	{name: "StepId", header: "STEPID", code: 'i'},
	{name: "StepName", header: "NAME", code: 'j'},
	{name: "StepState", header: "STATE"},
	{name: "TimeLimit", header: "TIME_LIMIT", code: 'l'},
	{name: "TimeUsed", header: "TIME", code: 'M'},
	{name: "tres-bind", header: "TRES_BIND"},
	{name: "tres-freq", header: "TRES_FREQ"},
	{name: "tres-per-job", header: "TRES_PER_STEP"},
	{name: "tres-per-socket", header: "TRES_PER_SOCKET"},
	{name: "tres-per-step", header: "TRES_PER_STEP"},
	{name: "tres-per-task", header: "TRES_PER_TASK"},
	{name: "UserId", header: "UID", code: 'U'},
	{name: "UserName", header: "USER", code: 'u'},
}, field{name: "tres-per-step", header: "TRES_PER_JOB", code: 'b'})

func (o *options) catalog() catalog {
	if o.steps {
		return steps
	}
	return jobs
}
func (o *options) helpFields(short bool) string {
	var b strings.Builder
	n, cols := 0, 4
	if short {
		cols = 8
	}
	for _, f := range o.catalog().ordered {
		if short && f.code == 0 {
			continue
		}
		if n > 0 && n%cols == 0 {
			b.WriteByte('\n')
		}
		if short {
			fmt.Fprintf(&b, "%%%-5c", f.code)
		} else {
			fmt.Fprintf(&b, "%-20s", f.name)
		}
		n++
	}
	b.WriteByte('\n')
	return b.String()
}

type column struct {
	field  field
	width  int
	right  bool
	suffix string
}

func width(s string, defaultWidth int) (int, bool, string) {
	right := strings.HasPrefix(s, ".")
	if right {
		s = s[1:]
	}
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return defaultWidth, right, s
	}
	v, _ := strconv.ParseUint(s[:i], 10, 32)
	return int(v), right, s[i:]
}
func (o *options) columns(diag io.Writer) ([]column, error) {
	cat := o.catalog()
	var cols []column
	if o.customLong {
		for part := range strings.SplitSeq(o.longFormat, ",") {
			name, tail, has := strings.Cut(part, ":")
			w, right, suffix := 20, false, ""
			if has {
				w, right, suffix = width(tail, 0)
			}
			f, ok := cat.names[strings.ToLower(name)]
			if !ok {
				kind := "job"
				if o.steps {
					kind = "job step"
				}
				fmt.Fprintf(diag, "squeue: error: Invalid %s format specification: %s\n", kind, name)
				w = 0
			}
			cols = append(cols, column{f, w, right, suffix})
		}
	} else {
		spec := o.format
		if !o.customFormat {
			switch {
			case o.steps:
				spec = "%.15i %.8j %.9P %.8u %.9M %N"
			case o.onlyState:
				spec = "%.18i %.2t"
			case o.start:
				spec = "%.18i %.9P %.8j %.8u %.2t %.19S %.6D %20Y %R"
			case o.long:
				spec = "%.18i %.9P %.8j %.8u %.8T %.10M %.9l %.6D %R"
			default:
				spec = "%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R"
			}
		}
		prefix, rest, has := strings.Cut(spec, "%")
		if prefix != "" {
			cols = append(cols, column{suffix: prefix})
		}
		if has && strings.EqualFold(rest, "all") {
			for _, code := range "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" {
				if f, ok := cat.codes[byte(code)]; ok {
					cols = append(cols, column{field: f, suffix: "|"})
				}
			}
		} else if has {
			for chunk := range strings.SplitSeq(rest, "%") {
				if chunk == "" {
					continue
				}
				w, right, tail := width(chunk, 0)
				if tail == "" {
					// A missing format code selects the first long-only field.
					f := cat.ordered[0]
					if !o.steps {
						f = cat.names["accruetime"]
					}
					cols = append(cols, column{field: f, width: w, right: right})
					continue
				}
				f, ok := cat.codes[tail[0]]
				if !ok {
					kind := "job"
					if o.steps {
						kind = "job step"
					}
					fmt.Fprintf(diag, "squeue: error: Invalid %s format specification: %c\n", kind, tail[0])
					cols = append(cols, column{suffix: "%" + chunk})
					continue
				}
				cols = append(cols, column{f, w, right, tail[1:]})
			}
		}
	}
	for _, c := range cols {
		if c.width > 1<<20 {
			return nil, fmt.Errorf("squeue: error: field width exceeds output limit")
		}
		if c.field.name != "" && !supported(c.field.name) {
			return nil, unsupported("field " + c.field.name)
		}
	}
	return cols, nil
}

// Slurm treats a right-aligned host list's width as a minimum.
func (c column) truncate(r *row) bool {
	if !c.right {
		return true
	}
	switch c.field.name {
	case "NodeList", "Nodes", "SchedNodes":
		return false
	case "ReasonList":
		return r != nil && r.job.Status == pb.JobStatus_Pending
	case "ReqNodes", "ExcNodes":
		return r == nil
	}
	return true
}

func printLine(w io.Writer, cols []column, r *row, o *options) error {
	var b strings.Builder
	for _, c := range cols {
		v := c.field.header
		if r != nil && c.field.name != "" {
			var err error
			v, err = r.value(c.field.name, o)
			if err != nil {
				return err
			}
		}
		if c.width > 0 {
			if len(v) > c.width && c.truncate(r) {
				v = v[:c.width]
			}
			pad := strings.Repeat(" ", max(0, c.width-len(v)))
			if c.right {
				b.WriteString(pad)
				b.WriteString(v)
			} else {
				b.WriteString(v)
				b.WriteString(pad)
			}
		} else {
			b.WriteString(v)
		}
		b.WriteString(c.suffix)
	}
	b.WriteByte('\n')
	_, err := io.WriteString(w, b.String())
	return err
}
