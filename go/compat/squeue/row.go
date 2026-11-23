// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	"cmp"
	"fmt"
	"math"
	"os/user"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PKUHPC/CraneSched/go/compat/utils"
	pb "github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/util"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var jobStates = [...]struct{ long, short string }{
	pb.JobStatus_Pending: {"PENDING", "PD"}, pb.JobStatus_Running: {"RUNNING", "R"},
	pb.JobStatus_Suspended: {"SUSPENDED", "S"}, pb.JobStatus_Completed: {"COMPLETED", "CD"},
	pb.JobStatus_Cancelled: {"CANCELLED", "CA"}, pb.JobStatus_Failed: {"FAILED", "F"},
	pb.JobStatus_ExceedTimeLimit: {"TIMEOUT", "TO"}, pb.JobStatus_OutOfMemory: {"OUT_OF_MEMORY", "OOM"},
	pb.JobStatus_Configuring: {"CONFIGURING", "CF"}, pb.JobStatus_Starting: {"CONFIGURING", "CF"},
	pb.JobStatus_Completing: {"COMPLETING", "CG"}, pb.JobStatus_Deadline: {"DEADLINE", "DL"},
}

type row struct {
	selection string
	job       *pb.JobInfo
	step      *pb.StepInfo
	task      *uint32
}

func (r row) id() string {
	if r.selection != "" {
		return num(r.job.JobId) + "_[" + r.selection + "]"
	}
	if r.task != nil {
		return util.FormatJobIdFromArrayTaskId(r.job.JobId, r.task)
	}
	if util.IsPendingArrayParent(r.job) {
		return num(r.job.JobId) + "_[" + arrayRange(r.job.ArraySpec) + "]"
	}
	return util.FormatJobId(r.job.JobId, r.job.ArrayTask)
}
func (r row) stepID() string { return r.id() + "." + r.stepSuffix() }
func (r row) stepSuffix() string {
	switch r.step.StepType {
	case pb.StepType_DAEMON:
		return "extern"
	case pb.StepType_PRIMARY:
		if r.job.Type == pb.JobType_Batch {
			return "batch"
		}
	}
	offset := uint32(1)
	if r.job.Type == pb.JobType_Batch {
		offset = 2
	}
	return num(r.step.StepId - offset)
}
func num(n uint32) string { return strconv.FormatUint(uint64(n), 10) }
func duration(sec int64) string {
	if sec < 0 {
		return "INVALID"
	}
	switch {
	case sec >= 86400:
		return fmt.Sprintf("%d-%02d:%02d:%02d", sec/86400, sec/3600%24, sec/60%60, sec%60)
	case sec >= 3600:
		return fmt.Sprintf("%d:%02d:%02d", sec/3600, sec/60%60, sec%60)
	default:
		return fmt.Sprintf("%d:%02d", sec/60, sec%60)
	}
}
func slurmSeconds(s string) (int64, error) {
	if s == "infinite" || s == "unlimited" {
		return math.MaxInt64, nil
	}
	d, clock, days := strings.Cut(s, "-")
	if !days {
		clock = d
		d = "0"
	}
	parts := strings.Split(clock, ":")
	if len(parts) > 3 {
		return 0, fmt.Errorf("invalid time")
	}
	if days {
		for len(parts) < 3 {
			parts = append(parts, "0")
		}
	} else if len(parts) == 1 {
		parts = append(parts, "0")
	}
	ds, err := strconv.ParseUint(d, 10, 32)
	if err != nil {
		return 0, err
	}
	n := int64(0)
	for _, p := range parts {
		x, err := strconv.ParseUint(p, 10, 32)
		if err != nil {
			return 0, err
		}
		n = n*60 + int64(x)
	}
	return int64(ds)*86400 + n, nil
}
func (o *options) timestamp(stamp *timestamppb.Timestamp) string {
	t, ok := util.ParseCraneTimestamp(stamp)
	if !ok {
		return "N/A"
	}
	return o.formatTime(t)
}
func supported(f string) bool {
	switch f {
	case "StdIn", "StdOut", "StdErr", "tres-per-node", "tres-per-task", "LicensesAlloc", "NTPerNode", "MaxNodes", "MaxCPUs", "AllocNodes", "DerivedEC", "tres-alloc", "Account", "ArrayJobId", "ArrayTaskId", "JobArrayId", "JobId", "StepId", "StepName", "StepState", "BatchFlag", "BatchHost", "Cluster", "Command", "Comment", "CPUsPerTask", "cpus-per-task", "Deadline", "Dependency", "EndTime", "ExcNodes", "Exclusive", "exit_code", "GroupId", "GroupName", "Licenses", "MinCPUs", "MinMemory", "Name", "Nodes", "NodeList", "NumCPUs", "NumNodes", "NumTasks", "Partition", "PendingTime", "Priority", "PriorityLong", "QOS", "Reason", "ReasonList", "ReqNodes", "Requeue", "Reservation", "RestartCnt", "StartTime", "State", "StateCompact", "SubmitTime", "TimeLeft", "TimeLimit", "TimeUsed", "UserId", "UserName", "WCKey", "WorkDir", "SchedNodes", "OverSubscribe":
		return true
	default:
		return false
	}
}
func (r row) value(f string, o *options) (string, error) {
	j := r.job
	status := j.Status
	elapsed := j.ElapsedTime.GetSeconds()
	limit := j.TimeLimit.GetSeconds()
	start := j.StartTime
	requested := j.ReqTotalResView
	allocated := j.AllocatedResView
	nodes := j.NodeNum
	tasks := j.Ntasks
	name := j.Name
	hosts := j.GetCranedList()
	if s := r.step; s != nil {
		status = s.Status
		elapsed = s.ElapsedTime.GetSeconds()
		limit = s.TimeLimit.GetSeconds()
		start = s.StartTime
		requested = s.ReqTotalResView
		allocated = s.AllocatedResView
		nodes = s.NodeNum
		tasks = s.Ntasks
		name = s.Name
		if s.StepType == pb.StepType_DAEMON {
			name = "extern"
		} else if s.StepType == pb.StepType_PRIMARY && j.Type == pb.JobType_Batch {
			name = "batch"
		}
		hosts = cmp.Or(s.CranedList, utils.CompressHosts(s.ExecutionNode))
	}
	if status == pb.JobStatus_Pending {
		elapsed = 0
	}
	reason := cmp.Or(j.GetPendingReason(), "None")
	if reason == "Resource" {
		reason = "Resources"
	}
	if j.Held && status == pb.JobStatus_Pending {
		reason = "JobHeldUser"
	}
	switch f {
	case "JobId":
		return num(j.JobId), nil
	case "JobArrayId":
		id := r.id()
		if o.onlyState && r.selection != "" && !o.fullBitmapID && len(id) > 31 {
			// The default state-table JOBID buffer includes the terminating NUL.
			id = id[:31]
		}
		return id, nil
	case "StepId":
		return r.stepID(), nil
	case "ArrayJobId":
		if j.ArrayTask != nil {
			return num(j.ArrayTask.ArrayJobId), nil
		}
		if r.step != nil {
			return "0", nil
		}
		return num(j.JobId), nil
	case "ArrayTaskId":
		if r.selection != "" {
			return r.selection, nil
		}
		if r.task != nil {
			return num(*r.task), nil
		}
		if j.ArrayTask != nil {
			return num(j.ArrayTask.TaskId), nil
		}
		if j.ArraySpec != nil {
			return arrayRange(j.ArraySpec), nil
		}
		return "N/A", nil
	case "Cluster":
		return o.clusterName, nil
	case "Account":
		return j.Account, nil
	case "UserName":
		return cmp.Or(j.Username, "(null)"), nil
	case "UserId":
		return num(j.Uid), nil
	case "GroupId":
		return num(j.Gid), nil
	case "GroupName":
		gid := num(j.Gid)
		g, err := user.LookupGroupId(gid)
		if err == nil {
			return g.Name, nil
		}
		return gid, nil
	case "Name", "StepName":
		return cmp.Or(name, "(null)"), nil
	case "Partition":
		return cmp.Or(j.Partition, "(null)"), nil
	case "QOS":
		return j.Qos, nil
	case "Reservation":
		return cmp.Or(j.Reservation, "(null)"), nil
	case "WCKey":
		return cmp.Or(strings.TrimPrefix(j.Wckey, "*"), "(null)"), nil
	case "Comment":
		return cmp.Or(gjson.Get(j.ExtraAttr, "comment").String(), "(null)"), nil
	case "Command":
		if j.Type == pb.JobType_Batch {
			if j.ScriptPath == nil {
				return "", fmt.Errorf("squeue: error: submitted script path is unavailable for job %d", j.JobId)
			}
			return cmp.Or(j.GetScriptPath(), "(null)"), nil
		}
		return cmp.Or(j.CmdLine, "(null)"), nil
	case "WorkDir":
		return j.Cwd, nil
	case "State", "StepState", "StateCompact":
		if int(status) >= len(jobStates) || status < 0 {
			return "?", nil
		}
		if f == "StateCompact" {
			return jobStates[status].short, nil
		}
		return jobStates[status].long, nil
	case "Reason":
		return reason, nil
	case "ReasonList":
		if status == pb.JobStatus_Pending {
			return "(" + reason + ")", nil
		}
		return hosts, nil
	case "NodeList", "Nodes":
		return hosts, nil
	case "SchedNodes":
		return cmp.Or(utils.CompressHosts(j.ScheduledNodes), "(null)"), nil
	case "StdIn", "StdOut", "StdErr":
		path := r.ioPath(f, o.expand)
		if r.step == nil {
			return cmp.Or(path, "(null)"), nil
		}
		return path, nil
	case "tres-per-node":
		gres := j.GresPerNode
		if r.step != nil {
			gres = r.step.GresPerNode
		}
		return cmp.Or(strings.Join(gresParts(gres, ":"), ","), "N/A"), nil
	case "MaxNodes":
		if limit := r.maxNodes(); limit != nil && *limit > 0 {
			return num(*limit), nil
		}
		if j.SubmittedSize == nil && j.Status == pb.JobStatus_Pending && j.GetMaxNodes() > 0 {
			return num(j.GetMaxNodes()), nil
		}
		return num(j.NodeNum), nil
	case "NTPerNode":
		if j.TasksPerNode == nil {
			return "N/A", nil
		}
		return num(j.GetTasksPerNode()), nil
	case "tres-alloc":
		if status == pb.JobStatus_Pending {
			allocated = requested
		}
		if allocated == nil {
			return "N/A", nil
		}
		return resourceTRES(allocated, nodes, true), nil
	case "ReqNodes":
		return utils.CompressHosts(j.ReqNodes), nil
	case "ExcNodes":
		return utils.CompressHosts(j.ExcludeNodes), nil
	case "AllocNodes":
		return cmp.Or(j.SubmitHostname, "(null)"), nil
	case "BatchHost":
		if len(j.ExecutionNode) > 0 {
			return j.ExecutionNode[0], nil
		}
		return "n/a", nil
	case "BatchFlag":
		if j.Type == pb.JobType_Batch {
			return "1", nil
		}
		return "0", nil
	case "Priority":
		return fmt.Sprintf("%.14f", float64(j.Priority)/math.MaxUint32), nil
	case "PriorityLong":
		return num(j.Priority), nil
	case "StartTime":
		return o.timestamp(start), nil
	case "EndTime":
		return o.timestamp(j.EndTime), nil
	case "SubmitTime":
		return o.timestamp(j.SubmitTime), nil
	case "Deadline":
		return o.timestamp(j.DeadlineTime), nil
	case "TimeUsed":
		return duration(max(0, elapsed)), nil
	case "PendingTime":
		t, ok := util.ParseCraneTimestamp(j.SubmitTime)
		if !ok {
			return "0:00", nil
		}
		until := time.Now()
		if a, ok := util.ParseCraneTimestamp(start); ok && until.After(a) {
			until = a
		}
		return strconv.FormatInt(max(0, int64(until.Sub(t).Seconds())), 10), nil
	case "TimeLimit", "TimeLeft":
		if r.step != nil && (r.step.StepType == pb.StepType_DAEMON || r.step.StepType == pb.StepType_PRIMARY && j.Type == pb.JobType_Batch) {
			return "UNLIMITED", nil
		}
		if limit >= util.MaxJobTimeLimit {
			return "UNLIMITED", nil
		}
		if f == "TimeLeft" {
			limit = max(0, limit-elapsed)
		}
		return duration(limit), nil
	case "NumNodes":
		return num(nodes), nil
	case "NumTasks":
		if tasks == nil {
			return "N/A", nil
		}
		return num(*tasks), nil
	case "NumCPUs", "MaxCPUs", "MinCPUs", "CPUsPerTask", "cpus-per-task":
		cpus := requested.GetCpuCount()
		if (f == "NumCPUs" || f == "MaxCPUs") && status != pb.JobStatus_Pending && allocated.GetCpuCount() > 0 {
			cpus = allocated.GetCpuCount()
		}
		switch f {
		case "MinCPUs":
			if nodes > 0 {
				cpus /= float64(nodes)
			}
		case "CPUsPerTask", "cpus-per-task":
			if tasks == nil || *tasks == 0 {
				return "N/A", nil
			}
			cpus /= float64(*tasks)
		}
		if math.Trunc(cpus) != cpus {
			return "", fmt.Errorf("squeue: error: job %d has a fractional CPU count that Slurm cannot represent", j.JobId)
		}
		return strconv.FormatFloat(cpus, 'f', 0, 64), nil
	case "tres-per-task":
		value, ok := r.taskTRES()
		if !ok {
			return "", fmt.Errorf("squeue: error: CPU count per task cannot be represented for job %d", j.JobId)
		}
		if r.step == nil {
			return cmp.Or(value, "N/A"), nil
		}
		return cmp.Or(value, "(null)"), nil
	case "MinMemory":
		if j.MemoryPerCpu != nil {
			return memory(*j.MemoryPerCpu, o.noConvert), nil
		}
		return memory(requested.GetMemoryBytes()/uint64(max(1, nodes)), o.noConvert), nil
	case "Licenses", "LicensesAlloc":
		licenses := j.LicensesCount
		if f == "LicensesAlloc" {
			licenses = j.AllocatedLicenses
		}
		var result []string
		for k, n := range licenses {
			result = append(result, k+":"+num(n))
		}
		slices.Sort(result)
		return cmp.Or(strings.Join(result, ","), "(null)"), nil
	case "Dependency":
		d := j.DependencyStatus
		var parts []string
		for _, dep := range d.GetPending() {
			kind := strings.ToLower(strings.ReplaceAll(dep.Type.String(), "_", ""))
			val := kind + ":" + num(dep.JobId)
			if dep.DelaySeconds > 0 {
				val += fmt.Sprintf("+%d", dep.DelaySeconds/60)
			}
			parts = append(parts, val+"(unfulfilled)")
		}
		sep := ","
		if d.GetIsOr() {
			sep = "?"
		}
		return cmp.Or(strings.Join(parts, sep), "(null)"), nil
	case "Requeue":
		if j.RequeueEligible == nil {
			return "", fmt.Errorf("squeue: error: requeue eligibility is unavailable for job %d", j.JobId)
		}
		if j.GetRequeueEligible() {
			return "1", nil
		}
		return "0", nil
	case "RestartCnt":
		return strconv.Itoa(int(j.RequeueCount)), nil
	case "Exclusive":
		if j.Exclusive {
			return "NODE", nil
		}
		return "NO", nil
	case "OverSubscribe":
		return "NO", nil
	case "exit_code", "DerivedEC":
		status := r.waitStatus(f)
		if status == nil {
			return "", fmt.Errorf("squeue: error: %s is unavailable for job %d", f, j.JobId)
		}
		return fmt.Sprintf("%d:%d", *status>>8, *status&127), nil
	}
	return "", unsupported("field " + f)
}

// The queue reports the pending node ceiling separately from the allocation.
// A CPU-only request above one CPU/task carries no implicit node maximum.
func (r row) maxNodes() *uint32 {
	j := r.job
	if j.Status != pb.JobStatus_Pending && len(j.ExecutionNode) > 0 {
		return new(uint32(0))
	}
	size := j.SubmittedSize
	if size == nil {
		return nil
	}
	if size.MaxNodes > 0 {
		return j.MaxNodes // the effective bound after task-count validation
	}
	if n := cmp.Or(size.Tasks, j.GetTasksPerNode()); n > 0 {
		return new(n)
	}
	if j.GetCpusPerTask() > 1 {
		return new(uint32(0))
	}
	return j.MaxNodes
}

func (r row) waitStatus(field string) *uint32 {
	if field == "DerivedEC" {
		return r.job.MaxStepWaitStatus
	}
	code := r.job.ExitCode
	switch {
	case code < 256:
		return new(code << 8)
	case code > 256 && code < 320:
		return new(code - 256)
	default:
		return nil
	}
}

func memory(bytes uint64, noConvert bool) string {
	mb := bytes >> 20
	if !noConvert && mb > 0 {
		for _, u := range []struct {
			factor uint64
			suffix string
		}{{1 << 30, "P"}, {1 << 20, "T"}, {1 << 10, "G"}} {
			if mb%u.factor == 0 {
				return fmt.Sprintf("%d%s", mb/u.factor, u.suffix)
			}
		}
	}
	return fmt.Sprintf("%dM", mb)
}

// A step inherits its allocation’s per-task TRES unless it requests GRES
// independently. Batch and extern have no separate per-task request.
func (r row) taskTRES() (string, bool) {
	cpus := r.job.CpusPerTask
	if step := r.step; step != nil {
		if step.StepType == pb.StepType_DAEMON || step.StepType == pb.StepType_PRIMARY && r.job.Type == pb.JobType_Batch {
			return "", true
		}
		if step.GresPerNode != nil {
			cpus = step.CpusPerTask
		}
	}
	if cpus == nil {
		return "", true
	}
	n := cpuNumber(*cpus)
	if n == nil {
		return "", false
	}
	return fmt.Sprintf("cpu=%d", n.Number), true
}
func gresParts(g *pb.GresMap, sep string) []string {
	var parts []string
	for name, c := range g.GetNameGresMap() {
		var specified uint64
		for kind, n := range c.Specified {
			if n > 0 {
				parts = append(parts, fmt.Sprintf("gres/%s:%s%s%d", name, kind, sep, n))
				specified += n
			}
		}
		if c.Total > specified {
			parts = append(parts, fmt.Sprintf("gres/%s%s%d", name, sep, c.Total-specified))
		}
	}
	slices.Sort(parts)
	return parts
}
func (r row) ioPath(field string, expand bool) string {
	j := r.job
	meta, cwd := j.IoMeta, j.Cwd
	if s := r.step; s != nil {
		meta, cwd = s.IoMeta, s.Cwd
	}

	if r.step != nil && r.step.StepType != pb.StepType_COMMON && j.Type == pb.JobType_Batch {
		return ""
	}
	output := meta.GetOutputFilePattern()
	if output == "" && j.Type == pb.JobType_Batch {
		output = "Crane-" + num(j.JobId) + ".out"
	}
	var pattern string
	switch field {
	case "StdIn":
		pattern = cmp.Or(meta.GetInputFilePattern(), "/dev/null")
	case "StdOut":
		pattern = output
	case "StdErr":
		pattern = meta.GetErrorFilePattern()
	}
	if pattern == "" {
		return ""
	}
	if r.step != nil && (pattern == "all" || meta == nil) {
		return ""
	}
	if !path.IsAbs(pattern) {
		pattern = path.Join(cwd, pattern)
	}
	if !expand {
		return pattern
	}
	master := num(j.JobId)
	task := "4294967294"
	if a := j.ArrayTask; a != nil {
		master = num(a.ArrayJobId)
		task = num(a.TaskId)
	}
	if r.task != nil {
		task = num(*r.task)
	}
	node := ""
	if len(j.ExecutionNode) > 0 {
		node = j.ExecutionNode[0]
	}
	return strings.NewReplacer("%%", "%", "%A", master, "%a", task, "%j", num(j.JobId), "%x", j.Name, "%u", j.Username, "%N", node, "%n", "0", "%t", "0").Replace(pattern)
}

func taskRanges(ids []uint32) string {
	var groups []string
	for i := 0; i < len(ids); {
		end := i
		for end+1 < len(ids) && ids[end+1]-ids[end] == 1 {
			end++
		}
		s := num(ids[i])
		if end > i {
			s += "-" + num(ids[end])
		}
		groups = append(groups, s)
		i = end + 1
	}
	return strings.Join(groups, ",")
}

// The state table groups adjacent set bits, but spells out separated tasks.
func stateArrayBitmap(spec *pb.ArraySpec) string {
	if spec.GetStride() <= 1 {
		return util.FormatArraySpec(spec, false)
	}
	var b strings.Builder
	for i := uint64(spec.Start); i <= uint64(spec.End); i += uint64(spec.GetStride()) {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(i, 10))
	}
	return b.String()
}

// The job record contains a compact bitmap display. Small noncontiguous
// arrays are enumerated even when the submission used a stride.
func arrayRange(s *pb.ArraySpec) string {
	if s.GetStride() < 2 || s.End < s.Start {
		return util.FormatArraySpec(s, true)
	}
	stride := s.GetStride()
	last := s.End - (s.End-s.Start)%stride
	count := (last-s.Start)/stride + 1
	if count > 5 && last-s.Start > 10 {
		return fmt.Sprintf("%d-%d:%d", s.Start, last, stride) + arrayLimit(s)
	}
	ids := make([]uint32, 0, count)
	for i := uint64(s.Start); i <= uint64(last); i += uint64(stride) {
		ids = append(ids, uint32(i))
	}
	return taskRanges(ids) + arrayLimit(s)
}

func arrayLimit(s *pb.ArraySpec) string {
	if n := s.GetMaxConcurrent(); n > 0 {
		return "%" + num(n)
	}
	return ""
}

func resourceTRES(rv *pb.ResourceView, nodes uint32, billing bool) string {
	cpu := strconv.FormatFloat(rv.GetCpuCount(), 'f', -1, 64)
	parts := []string{"cpu=" + cpu, "mem=" + memory(rv.GetMemoryBytes(), false), "node=" + num(nodes)}
	if billing {
		parts = append(parts, "billing="+cpu)
	}
	return strings.Join(append(parts, gresParts(rv.GetGresMap(), "=")...), ",")
}
