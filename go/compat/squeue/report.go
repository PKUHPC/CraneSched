// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	"cmp"
	"fmt"
	"io"
	"math"
	"os"
	"os/user"
	"strconv"
	"strings"

	"github.com/PKUHPC/CraneSched/go/compat/protocol/v45"
	"github.com/PKUHPC/CraneSched/go/compat/utils"
	pb "github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/util"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type reportOptions struct {
	schema, complex bool
	plugin          string
	encoding        utils.SerializerOptions
}

func (o *options) reportOptions(diag io.Writer) (reportOptions, error) {
	parser := strings.TrimPrefix(o.parser, "data_parser/")
	if !o.parserAttached {
		parser = "v0.0.45"
	}
	version, flags, _ := strings.Cut(parser, "+")
	s := reportOptions{plugin: "data_parser/" + parser}
	if version != "v0.0.45" {
		return s, unsupported(fmt.Sprintf("data parser %q (supported: v0.0.45)", version))
	}
	for flag := range strings.SplitSeq(strings.ToLower(flags), "+") {
		switch flag {
		case "", "fast", "minimize_refs":
		case "spec_only":
			s.schema = true
		case "complex":
			s.complex = true
		case "prefer_refs":
			fmt.Fprintln(diag, "squeue: data_parser/v0.0.45 ignoring default flag FLAG_PREFER_REFS")
		default:
			fmt.Fprintf(diag, "squeue: warning: data_parser/v0.0.45 ignoring unknown flag %s\n", flag)
		}
	}
	for _, kind := range []string{"json", "yaml"} {
		raw := o.env["SLURM_"+strings.ToUpper(kind)]
		flags := utils.SerializerOptions{Compact: raw != ""}
		for flag := range strings.SplitSeq(strings.ToLower(raw), ",") {
			switch flag {
			case "":
			case "compact":
				flags.Compact = true
			case "pretty":
				flags.Compact = false
			case "complex":
				flags.Complex = true
			case "no_tag":
				flags.NoTag = true
			default:
				return s, fmt.Errorf("squeue: fatal: Unable to parse serializer %q flags: Invalid argument", raw)
			}
		}
		if kind == o.serializer {
			s.encoding = flags
		}
	}
	return s, nil
}
func (o *options) writeReport(out io.Writer, s reportOptions, argv []string, rows []row) error {
	if s.schema {
		switch {
		case o.steps:
			return utils.WriteSchema[v45.QueueStepsResponse](out, o.serializer, s.encoding)
		case o.onlyState:
			return utils.WriteSchema[v45.QueueStatesResponse](out, o.serializer, s.encoding)
		default:
			return utils.WriteSchema[v45.QueueResponse](out, o.serializer, s.encoding)
		}
	}
	uid := strconv.Itoa(os.Getuid())
	gid := strconv.Itoa(os.Getgid())
	if u, e := user.LookupId(uid); e == nil {
		uid = u.Username
	}
	if g, e := user.LookupGroupId(gid); e == nil {
		gid = g.Name
	}
	meta := v45.Meta{Plugin: v45.MetaPlugin{DataParser: s.plugin}, Client: v45.MetaClient{User: uid, Group: gid}, Command: argv, Slurm: v45.SlurmMeta{Version: v45.SlurmVersion{Major: "26", Minor: "05", Micro: "4"}, Release: "26.05.4", Cluster: o.clusterName}}
	if o.steps {
		response := v45.QueueStepsResponse{Meta: meta}
		if len(rows) == 0 {
			response.Warnings = []v45.Warning{{Description: "Zero steps to dump"}}
		}
		for _, r := range rows {
			response.Steps = append(response.Steps, stepReport(r, o))
		}
		return utils.WriteReport(out, response, o.serializer, s.encoding, s.complex)
	}
	if !o.onlyState {
		response := v45.QueueResponse{Meta: meta}
		if len(rows) == 0 {
			response.Warnings = []v45.Warning{{Description: "Zero jobs to dump"}}
		}
		for _, r := range rows {
			response.Jobs = append(response.Jobs, jobReport(r, o))
		}
		return utils.WriteReport(out, response, o.serializer, s.encoding, s.complex)
	}
	response := v45.QueueStatesResponse{Meta: meta}
	for _, r := range rows {
		state, err := r.value("State", o)
		if err != nil {
			return err
		}
		response.Jobs = append(response.Jobs, v45.QueueJobState{JobID: r.id(), State: []string{state}})
	}
	return utils.WriteReport(out, response, o.serializer, s.encoding, s.complex)
}

func number32(n uint32) v45.Number[v45.Int32] { return v45.Numeric[v45.Int32](uint64(n), false, false) }
func optional32(n *uint32) *v45.Number[v45.Int32] {
	if n == nil {
		return nil
	}
	return new(number32(*n))
}
func cpuNumber(n float64) *v45.Number[v45.Int32] {
	if n < 0 || n > math.MaxUint32 || math.Trunc(n) != n {
		return nil
	}
	return new(number32(uint32(n)))
}
func timestamp(t *timestamppb.Timestamp) *v45.Number[uint64] {
	if instant, ok := util.ParseCraneTimestamp(t); ok {
		return new(v45.Numeric[uint64](uint64(instant.Unix()), false, false))
	}
	return nil
}
func timeLimit(t *durationpb.Duration) v45.Number[v45.Int32] {
	if t == nil {
		return v45.Number[v45.Int32]{}
	}
	return v45.Numeric[v45.Int32](uint64(max(0, t.Seconds)+59)/60, false, t.Seconds >= util.MaxJobTimeLimit)
}

func (r row) paths(expand bool) (in, out, errout string) {
	return r.ioPath("StdIn", expand), r.ioPath("StdOut", expand), r.ioPath("StdErr", expand)
}

func jobReport(r row, o *options) v45.QueueJob {
	j := r.job
	state, _ := r.value("State", o)
	reason, _ := r.value("Reason", o)
	group, _ := r.value("GroupName", o)
	result := v45.QueueJob{
		Account:                 j.Account,
		AllocatingNode:          j.SubmitHostname,
		BatchFlag:               j.Type == pb.JobType_Batch,
		Cluster:                 o.clusterName,
		Comment:                 gjson.Get(j.ExtraAttr, "comment").String(),
		EndTime:                 timestamp(j.EndTime),
		ExcludedNodes:           utils.CompressHosts(j.ExcludeNodes),
		GroupID:                 v45.Int32(j.Gid),
		GroupName:               group,
		JobID:                   v45.Int32(j.JobId),
		JobState:                []string{state},
		Name:                    j.Name,
		Nodes:                   j.GetCranedList(),
		TasksPerNode:            optional32(j.TasksPerNode),
		NodeCount:               number32(j.NodeNum),
		Tasks:                   optional32(j.Ntasks),
		Partition:               j.Partition,
		Hold:                    j.Held,
		Priority:                number32(j.Priority),
		QoS:                     j.Qos,
		RequiredNodes:           utils.CompressHosts(j.ReqNodes),
		Requeue:                 j.RequeueEligible,
		RestartCnt:              v45.Int32(j.RequeueCount),
		ResvName:                j.Reservation,
		ScheduledNodes:          utils.CompressHosts(j.ScheduledNodes),
		StartTime:               timestamp(j.StartTime),
		StateReason:             reason,
		SubmitTime:              timestamp(j.SubmitTime),
		SubmitLine:              j.CmdLine,
		TimeLimit:               timeLimit(j.TimeLimit),
		UserID:                  v45.Int32(j.Uid),
		UserName:                j.Username,
		Wckey:                   strings.TrimPrefix(j.Wckey, "*"),
		CurrentWorkingDirectory: j.Cwd,
		ArrayJobID:              number32(0),
		ArrayMaxTasks:           number32(j.GetArraySpec().GetMaxConcurrent()),
	}
	if deadline := timestamp(j.DeadlineTime); deadline != nil {
		result.Deadline = *deadline
	} else {
		// The v45 deadline for a job without a deadline is the Unix epoch.
		result.Deadline = v45.Numeric[uint64](0, false, false)
	}
	if j.ExitCode < 256 {
		status := "SUCCESS"
		if j.ExitCode != 0 {
			status = "ERROR"
		}
		result.ExitCode = &v45.QueueJobDerivedExitCode{Status: []string{status}, ReturnCode: number32(j.ExitCode)}
	}
	if j.Type == pb.JobType_Batch {
		result.Command = j.ScriptPath
	} else {
		result.Command = new(j.CmdLine)
	}
	if j.ArrayTask != nil {
		result.ArrayJobID = number32(j.ArrayTask.ArrayJobId)
		result.ArrayTaskID = number32(j.ArrayTask.TaskId)
	}
	if j.ArraySpec != nil {
		result.ArrayJobID = number32(j.JobId)
		result.ArrayTaskString = arrayRange(j.ArraySpec)
	}
	if r.task != nil {
		result.ArrayTaskID = number32(*r.task)
		result.ArrayTaskString = ""
	}
	if r.selection != "" {
		result.ArrayTaskString = r.selection
	}
	if len(j.ExecutionNode) > 0 {
		result.BatchHost = j.ExecutionNode[0]
	}
	if len(j.LicensesCount) > 0 {
		result.Licenses, _ = r.value("Licenses", o)
	}
	if len(j.AllocatedLicenses) > 0 {
		result.LicensesAllocated, _ = r.value("LicensesAlloc", o)
	}
	if len(j.DependencyStatus.GetPending()) > 0 {
		result.Dependency, _ = r.value("Dependency", o)
	}
	result.StandardInput, result.StandardOutput, result.StandardError = r.paths(false)
	result.StdinExpanded, result.StdoutExpanded, result.StderrExpanded = r.paths(true)
	if rv := j.ReqTotalResView; rv != nil {
		result.TRESReqStr = new(resourceTRES(rv, j.NodeNum, true))
		if j.NodeNum != 0 {
			result.MinimumCPUsPerNode = cpuNumber(rv.CpuCount / float64(j.NodeNum))
		}
		if j.GetNtasks() > 0 {
			result.CPUsPerTask = cpuNumber(rv.CpuCount / float64(j.GetNtasks()))
			if result.CPUsPerTask != nil {
				result.TRESPerTask = fmt.Sprintf("cpu=%d", result.CPUsPerTask.Number)
			}
		}
		if j.MemoryPerCpu != nil {
			result.MemoryPerCPU = new(v45.Numeric[uint64](*j.MemoryPerCpu>>20, false, false))
			result.MemoryPerNode = new(v45.Number[uint64]{})
		} else if j.NodeNum != 0 {
			result.MemoryPerNode = new(v45.Numeric[uint64](rv.MemoryBytes/uint64(j.NodeNum)>>20, false, false))
			result.MemoryPerCPU = new(v45.Number[uint64]{})
		}
	}
	rv := j.AllocatedResView
	if j.Status == pb.JobStatus_Pending {
		rv = j.ReqTotalResView
	}
	if rv != nil {
		result.CPUs = cpuNumber(rv.CpuCount)
		result.BillableTRES = new(v45.Number[float64]{Set: true, Number: rv.CpuCount})
	}
	if j.Status != pb.JobStatus_Pending && j.AllocatedResView != nil {
		result.TRESAllocStr = new(resourceTRES(j.AllocatedResView, j.NodeNum, true))
	}
	result.TRESPerNode = strings.Join(gresParts(j.GresPerNode, ":"), ",")
	return result
}
func stepReport(r row, o *options) v45.QueueStep {
	j, s := r.job, r.step
	name, _ := r.value("StepName", o)
	state, _ := r.value("StepState", o)
	result := v45.QueueStep{
		Cluster:   o.clusterName,
		Name:      name,
		Nodes:     cmp.Or(s.CranedList, utils.CompressHosts(s.ExecutionNode)),
		Partition: j.Partition,
		State:     []string{state},
		ID:        r.stepID(),
		User:      j.Username,
		Array:     v45.QueueStepArray{TaskID: v45.Int32(math.MaxUint32 - 1)}}
	if s.Uid != j.Uid {
		result.User = strconv.FormatUint(uint64(s.Uid), 10)
		if account, err := user.LookupId(result.User); err == nil {
			result.User = account.Username
		}
	}
	if a := j.ArrayTask; a != nil {
		result.Array = v45.QueueStepArray{JobID: v45.Int32(a.ArrayJobId), TaskID: v45.Int32(a.TaskId)}
	}
	if s.Ntasks != nil {
		result.NumberTasks = new(v45.Int32(*s.Ntasks))
	}
	special := s.StepType == pb.StepType_DAEMON || s.StepType == pb.StepType_PRIMARY && j.Type == pb.JobType_Batch
	if rv := s.AllocatedResView; rv != nil && !special {
		if n := cpuNumber(rv.CpuCount); n != nil {
			result.NumberCPUs = new(n.Number)
		}
		result.TRES.Allocation = new(resourceTRES(rv, s.NodeNum, false))
	}
	result.TRES.Per.Node = strings.Join(gresParts(s.GresPerNode, ":"), ",")
	if !special && s.ReqTotalResView != nil && s.GetNtasks() > 0 {
		if n := cpuNumber(s.ReqTotalResView.CpuCount / float64(s.GetNtasks())); n != nil {
			result.TRES.Per.Task = fmt.Sprintf("cpu=%d", n.Number)
		}
	}
	result.Time = v45.QueueStepTime{Running: v45.Numeric[uint64](uint64(max(0, s.ElapsedTime.GetSeconds())), s.ElapsedTime == nil, false), Start: timestamp(s.StartTime), Limit: timeLimit(s.TimeLimit)}
	if special {
		// Slurm exposes these synthetic steps without a separate submission or CPU allocation.
		result.SubmitLine = new("")
		result.NumberCPUs = new(v45.Int32(0))
		result.TRES.Allocation = new("")
		result.Time.Limit = v45.Numeric[v45.Int32](0, false, true)
	}
	result.Stdin, result.Stdout, result.Stderr = r.paths(false)
	result.StdinExpanded, result.StdoutExpanded, result.StderrExpanded = r.paths(true)
	return result
}
