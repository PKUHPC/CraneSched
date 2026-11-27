// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PKUHPC/CraneSched/go/compat/protocol/v45"
	"github.com/PKUHPC/CraneSched/go/compat/utils"
	pb "github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/util"
)

func Run(ctx context.Context, argv, environ []string, out, diag io.Writer) int {
	return execute(ctx, "/etc/crane/config.yaml", argv, environ, out, diag)
}

func execute(ctx context.Context, config string, argv, environ []string, out, diag io.Writer) int {
	env := map[string]string{}
	for _, e := range environ {
		k, v, ok := strings.Cut(e, "=")
		if ok {
			env[k] = v
		}
	}
	if err := run(ctx, config, argv, env, out, diag); err != nil {
		fmt.Fprintln(diag, err)
		if _, ok := err.(*operandError); ok {
			io.WriteString(out, usage)
		}
		return 1
	}
	return 0
}

func run(ctx context.Context, config string, argv []string, env map[string]string, out, diag io.Writer) error {
	o, err := parse(argv[1:], env, diag)
	if err != nil {
		return err
	}
	var local string
	switch o.action {
	case "help":
		local = help
	case "usage":
		local = usage
	case "version":
		local = "slurm " + v45.Release + "\n"
	case "helpstate":
		local = stateNames("\n") + "\n"
	case "helpformat", "helpFormat":
		local = o.helpFields(o.action == "helpformat")
	case "parsers":
		fmt.Fprintln(diag, "Possible data_parser plugins:")
		_, err = fmt.Fprintln(out, "data_parser/v0.0.45")
		return err
	case "autocomplete":
		local = utils.CompleteOptions(vocabulary, o.actionValue)
	}
	if o.action != "" {
		_, err = io.WriteString(out, local)
		return err
	}
	var cols []column
	var report utils.DataParserOptions
	if o.serializer != "" {
		report, err = utils.ParseDataParser("squeue", o.parser, o.parserAttached, diag)
		if err != nil {
			return unsupported(err.Error())
		}
	} else {
		cols, err = o.columns(diag)
	}
	if err != nil {
		return err
	}
	if o.verbose > 0 {
		if err := o.printOptions(out); err != nil {
			return err
		}
	}
	session, err := utils.OpenSession(ctx, config)
	if err != nil {
		return fmt.Errorf("squeue: error: %w", err)
	}
	defer session.Close()
	if o.nodes != nil {
		if err := o.loadInventory(ctx, session, true); err != nil {
			return err
		}
	}
	needCluster := !report.SpecOnly && (o.serializer != "" || slices.ContainsFunc(cols, func(c column) bool { return c.field.name == "Cluster" }))
	req := o.queueRequest()
	for {
		if !o.noHeader && (o.long || o.iterate > 0 || o.verbose > 0) {
			if _, err = fmt.Fprintln(out, time.Now().In(utils.CommandTimezone(env)).Format(time.ANSIC)); err != nil {
				return err
			}
		}
		reply, err := o.loadQueue(ctx, session, req, needCluster)
		if err != nil {
			if o.iterate == 0 {
				return err
			}
			fmt.Fprintln(diag, err)
		} else if err := o.printFrame(out, diag, argv, cols, report, reply); err != nil {
			return err
		}
		if o.iterate == 0 {
			return nil
		}
		if _, err := fmt.Fprintln(out); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(o.iterate) * time.Second):
		}
	}
}

func (o *options) loadInventory(ctx context.Context, session *utils.Session, nodes bool) error {
	var reply *pb.QueryClusterInfoReply
	err := session.Query(ctx, func(ctx context.Context, client pb.CraneCtldClient, uid uint32) error {
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		var err error
		reply, err = client.QueryClusterInfo(ctx, &pb.QueryClusterInfoRequest{Uid: &uid, IncludeNodes: nodes})
		return err
	})
	if err != nil {
		return fmt.Errorf("slurm_load_node error: %w", err)
	}
	if !reply.GetOk() {
		return fmt.Errorf("slurm_load_node error: cluster information unavailable")
	}
	o.clusterName = reply.ClusterName
	if !nodes {
		return nil
	}
	local, err := os.Hostname()
	if err != nil {
		return err
	}
	short, _, _ := strings.Cut(local, ".")
	names := make(map[string]string, len(reply.Nodes)*2)
	for _, n := range reply.Nodes {
		if n.GetHostname() == "" {
			continue
		}
		for _, alias := range []string{n.Hostname, n.NodeHostname} {
			if alias != "" && names[alias] == "" {
				names[alias] = n.Hostname
			}
		}
	}
	o.nodeNames = make(map[string]bool)
	for item := range o.nodes.Names() {
		name := item
		if strings.EqualFold(name, "localhost") {
			name = short
		}
		canonical, ok := names[name]
		if !ok {
			return fmt.Errorf("squeue: error: Invalid node name %s", item)
		}
		o.nodeNames[canonical] = true
	}
	return nil
}

func (o *options) verboseJobTable() bool {
	return o.verbose > 0 && o.serializer == "" && !o.steps && !o.onlyState
}

func (o *options) queueRequest() *pb.QueryJobsInfoRequest {
	ids := o.req.FilterJobIds
	var selectors []*pb.JobIdSelector
	switch {
	case o.onlyState:
		// Only job IDs enter the state query, even with SQUEUE_* defaults.
		selectors = ids
	case !o.steps && len(ids) == 1:
		// Fetch the allocation to distinguish an unknown ID from a display mismatch.
		selectors = []*pb.JobIdSelector{{JobId: ids[0].JobId}}
	case o.steps && o.serializer != "" || o.verboseJobTable():
		// Slurm serializes the full step snapshot and counts loaded jobs before filtering.
	default:
		return o.req
	}
	return &pb.QueryJobsInfoRequest{
		Mode: o.req.Mode, NumLimit: o.req.NumLimit, IncludePublicQueueDetails: true,
		OptionQueryStepsOnly: o.steps, FilterJobIds: selectors,
	}
}

func (o *options) loadQueue(ctx context.Context, session *utils.Session, req *pb.QueryJobsInfoRequest, needCluster bool) (*pb.QueryJobsInfoReply, error) {
	if needCluster && o.clusterName == "" {
		if err := o.loadInventory(ctx, session, false); err != nil {
			return nil, err
		}
	}
	var reply *pb.QueryJobsInfoReply
	err := session.Query(ctx, func(ctx context.Context, client pb.CraneCtldClient, uid uint32) error {
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		var e error
		reply, e = util.QueryJobsInfo(ctx, client, req)
		return e
	})
	if err != nil {
		kind := "slurm_load_jobs"
		if o.steps {
			kind = "slurm_get_job_steps"
		}
		return nil, fmt.Errorf("%s error: %w", kind, err)
	}
	if !reply.GetOk() || reply.HasMore {
		return nil, fmt.Errorf("squeue: error: incomplete queue response")
	}
	if !o.onlyState && len(list(o.jobIDs)) == 1 && len(reply.JobInfoList) == 0 && !o.steps {
		return nil, fmt.Errorf("slurm_load_jobs error: Invalid job id specified")
	}
	return reply, nil
}

func (o *options) printFrame(out, diag io.Writer, argv []string, cols []column, report utils.DataParserOptions, reply *pb.QueryJobsInfoReply) error {
	if o.verbose > 0 && !o.onlyState && o.serializer == "" {
		n := len(reply.JobInfoList)
		if !o.steps && len(o.req.FilterJobIds) != 1 && len(o.users) == 1 && o.users[0] != "0" {
			// Slurm loads a single non-root user's jobs directly.
			n = 0
			for _, j := range reply.JobInfoList {
				if num(j.Uid) == o.users[0] {
					n++
				}
			}
		}
		if o.steps {
			n = 0
			for _, j := range reply.JobInfoList {
				n += len(j.StepInfoList)
			}
		}
		if _, err := fmt.Fprintf(out, "last_update_time=%d records=%d\n", time.Now().Unix(), n); err != nil {
			return err
		}
	}
	if report.SpecOnly {
		return o.writeReport(out, report, argv, nil)
	}
	if strings.Contains(o.sort, "b") && o.serializer == "" {
		fmt.Fprintln(diag, "squeue: Invalid sort specification: b")
	}
	rows, err := o.rows(reply)
	if err != nil {
		return err
	}
	if o.serializer != "" {
		if err := o.writeReport(out, report, argv, rows); err != nil {
			return err
		}
	} else {
		if !o.noHeader {
			if err := printLine(out, cols, nil, o); err != nil {
				return err
			}
		}
		for i := range rows {
			if err := printLine(out, cols, &rows[i], o); err != nil {
				return err
			}
		}
	}
	return nil
}

func matchesJob(j *pb.JobInfo, sel *pb.JobIdSelector) bool {
	a := j.ArrayTask
	if sel.ArrayTaskId != nil {
		return a != nil && a.ArrayJobId == sel.JobId && a.TaskId == *sel.ArrayTaskId
	}
	return j.JobId == sel.JobId || a != nil && a.ArrayJobId == sel.JobId
}

func (o *options) matchesNodes(allocated string, execution []string) (bool, error) {
	if len(o.nodeNames) == 0 {
		return true, nil
	}
	if allocated == "" {
		return slices.ContainsFunc(execution, func(name string) bool { return o.nodeNames[name] }), nil
	}
	hosts, err := utils.ParseHostList(allocated)
	if err != nil {
		return false, fmt.Errorf("squeue: error: invalid allocation node list %q: %w", allocated, err)
	}
	for name := range o.nodeNames {
		if hosts.Match(name) {
			return true, nil
		}
	}
	return false, nil
}

func (o *options) matchesStep(r row) bool {
	s := r.step
	if s.Status == pb.JobStatus_Pending || o.users != nil && !slices.Contains(o.users, num(s.Uid)) ||
		len(o.req.FilterPartitions) > 0 && !slices.Contains(o.req.FilterPartitions, r.job.Partition) {
		return false
	}
	if len(o.req.FilterJobIds) > 0 && !slices.ContainsFunc(o.req.FilterJobIds, func(sel *pb.JobIdSelector) bool { return matchesJob(r.job, sel) }) {
		return false
	}
	return len(o.stepSelectors) == 0 || slices.ContainsFunc(o.stepSelectors, func(sel stepSelector) bool {
		return matchesJob(r.job, sel.job) && r.stepSuffix() == num(sel.index)
	})
}

// The v45 state parser expands the pending bitmap for every matching selector,
// even when a selector named just one member of that bitmap.
func (o *options) stateRows(reply *pb.QueryJobsInfoReply) []row {
	var rows []row
	appendJob := func(j *pb.JobInfo, sel *pb.JobIdSelector) {
		if util.IsPendingArrayParent(j) {
			spec := j.ArraySpec
			stride := max(1, spec.GetStride())
			if sel != nil {
				if sel.JobId != j.JobId {
					return
				}
				if t := sel.ArrayTaskId; t != nil && (*t < spec.Start || *t > spec.End || (*t-spec.Start)%stride != 0) {
					return
				}
			}
			for t := uint64(spec.Start); t <= uint64(spec.End); t += uint64(stride) {
				rows = append(rows, row{job: j, task: new(uint32(t))})
			}
		} else if sel == nil || matchesJob(j, sel) {
			r := row{job: j}
			if child := j.ArrayTask; child != nil {
				// v45 state IDs qualify the task with its own job record ID.
				r.task = &child.TaskId
			}
			rows = append(rows, r)
		}
	}
	if len(o.req.FilterJobIds) == 0 {
		for _, j := range reply.JobInfoList {
			appendJob(j, nil)
		}
	} else {
		for _, sel := range o.req.FilterJobIds {
			for _, j := range reply.JobInfoList {
				appendJob(j, sel)
			}
		}
	}
	return rows
}

func (o *options) rows(reply *pb.QueryJobsInfoReply) ([]row, error) {
	if o.onlyState && o.serializer != "" {
		return o.stateRows(reply), nil
	}
	// An explicitly empty list matches no jobs; the RPC treats an empty list as unrestricted.
	if !o.steps && assigned(o.raw, "reservation") && len(o.req.FilterReservations) == 0 {
		return nil, nil
	}
	filterLoadedJobs := len(o.req.FilterJobIds) == 1 || o.verboseJobTable()
	var result []row
	for _, j := range reply.JobInfoList {
		if o.steps {
			for _, step := range j.StepInfoList {
				r := row{job: j, step: step}
				if o.serializer == "" {
					if !o.matchesStep(r) {
						continue
					}
					match, err := o.matchesNodes(step.CranedList, step.ExecutionNode)
					if err != nil {
						return nil, err
					}
					if !match {
						continue
					}
				}
				result = append(result, r)
			}
			continue
		}
		// Single-job and verbose loads can return jobs outside the display selectors.
		if filterLoadedJobs && len(o.req.FilterJobIds) > 0 && !util.IsPendingArrayParent(j) &&
			!slices.ContainsFunc(o.req.FilterJobIds, func(sel *pb.JobIdSelector) bool { return matchesJob(j, sel) }) {
			continue
		}
		if o.onlyState {
			// Text display filters see only the identity and state returned by Slurm.
			j = &pb.JobInfo{JobId: j.JobId, ArrayTask: j.ArrayTask, ArraySpec: j.ArraySpec, Status: j.Status, Username: "root"}
		}
		if filterLoadedJobs && len(o.req.FilterReservations) > 0 && !slices.Contains(o.req.FilterReservations, j.Reservation) {
			continue
		}
		if len(o.req.FilterPartitions) > 0 && !slices.Contains(o.req.FilterPartitions, j.Partition) {
			continue
		}
		if !matchesLicenses(o.licenses, j.RequestedLicenses) {
			continue
		}
		if !matchesFold(o.accounts, j.Account) || !matchesFold(o.qos, j.Qos) || !matchesFold(o.names, j.Name) {
			continue
		}
		if o.users != nil && !slices.Contains(o.users, num(j.Uid)) || o.notUser == num(j.Uid) {
			continue
		}
		if len(o.states) > 0 && !slices.Contains(o.states, j.Status) {
			continue
		}
		match, err := o.matchesNodes(j.GetCranedList(), j.ExecutionNode)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		if o.over > 0 || o.under > 0 {
			t := j.ElapsedTime.GetSeconds()
			if j.Status != pb.JobStatus_Running || o.over > 0 && t < o.over || o.under > 0 && t >= o.under {
				continue
			}
		}

		if util.IsPendingArrayParent(j) && o.jobIDs != "" {
			var indexes []uint32
			allTasks := false
			for _, sel := range o.req.FilterJobIds {
				if sel.JobId != j.JobId {
					continue
				}
				if sel.ArrayTaskId == nil {
					allTasks = true
					break
				}
				i := *sel.ArrayTaskId
				spec := j.ArraySpec
				if i >= spec.Start && i <= spec.End && (i-spec.Start)%max(1, spec.GetStride()) == 0 {
					indexes = append(indexes, i)
				}
			}
			if !allTasks {
				slices.Sort(indexes)
				indexes = slices.Compact(indexes)
				if len(indexes) == 1 || o.array && o.serializer == "" {
					for _, i := range indexes {
						result = append(result, row{job: j, task: new(i)})
					}
				} else if len(indexes) > 0 {
					result = append(result, row{job: j, selection: taskRanges(indexes)})
				}
				continue
			}
		}
		if o.array && o.serializer == "" && util.IsPendingArrayParent(j) {
			spec := j.ArraySpec
			stride := max(1, spec.GetStride())
			for i := uint64(spec.Start); i <= uint64(spec.End); i += uint64(stride) {
				result = append(result, row{job: j, task: new(uint32(i))})
			}
			continue
		}
		r := row{job: j}
		if o.onlyState && util.IsPendingArrayParent(j) {
			// State records carry only the remaining bitmap, without a throttle.
			r.selection = stateArrayBitmap(j.ArraySpec)
		}
		result = append(result, r)
	}
	if o.serializer != "" {
		return result, nil
	}
	return result, o.sortRows(result)
}

func (o *options) sortRows(rows []row) error {
	// Text job output is presorted by descending start time. Jobs without
	// a start always follow jobs with a real or estimated start.
	now := time.Now().Unix()
	startTime := func(r row) int64 {
		t := r.sortNumber("StartTime")
		if t > 0 && r.job.Status == pb.JobStatus_Pending {
			return max(t, now)
		}
		return max(0, t)
	}
	if !o.steps {
		slices.SortStableFunc(rows, func(a, b row) int {
			return cmp.Compare(startTime(b), startTime(a))
		})
	}
	order := o.sort
	if !o.customSort {
		switch {
		case o.steps:
			order = "P,i"
		case o.start:
			order = "S"
		default:
			order = "P,t,-p"
		}
	}
	type key struct {
		field string
		desc  bool
	}
	var keys []key
	for len(order) > 0 {
		desc := false
		switch order[0] {
		case ',', '+':
			order = order[1:]
			continue
		case '-':
			desc = true
			order = order[1:]
			if len(order) == 0 {
				break
			}
		}
		if len(order) == 0 {
			break
		}
		if strings.HasPrefix(strings.ToLower(order), "cluster") {
			order = order[7:]
			continue
		}
		c := order[0]
		order = order[1:]
		if c == 'b' {
			continue
		}
		if o.steps && !strings.ContainsRune("iNPlSMuU", rune(c)) {
			continue
		}
		if !o.steps && !strings.ContainsRune("BcCdfDegiGhHiIjJlLmMnNOpPQStTuUvVz", rune(c)) {
			return fmt.Errorf("squeue: error: Invalid sort specification: %c", c)
		}
		var f field
		switch c {
		case 'i':
			f.name = "id"
		case 't':
			f.name = "StateCompact"
		case 'T':
			f.name = "State"
		case 'p', 'Q':
			f.name = "PriorityLong"
		case 'c', 'f', 'h', 'n', 'O':
			continue
		default:
			f = o.catalog().codes[c]
		}
		if f.name == "" {
			return fmt.Errorf("squeue: error: Invalid sort specification: %c", c)
		}
		if f.name != "id" && !supported(f.name) {
			return unsupported("sort field " + f.name)
		}
		keys = append(keys, key{f.name, desc})
	}
	// Precompute displayed keys once; return conversion failures before printing any rows.
	type item struct {
		r      row
		values []string
		index  int
	}
	items := make([]item, len(rows))
	for i, r := range rows {
		it := item{r: r, index: i}
		for _, k := range keys {
			var v string
			var err error
			switch k.field {
			case "NodeList", "Nodes":
				hosts := r.job.ExecutionNode
				if r.step != nil {
					hosts = r.step.ExecutionNode
				}
				for _, host := range hosts {
					if v == "" || utils.HostLess(host, v) {
						v = host
					}
				}
			case "id":
				v = r.id()
				if r.step != nil {
					v = r.stepID()
				}
			case "TimeUsed", "TimeLeft", "TimeLimit", "MinMemory", "SubmitTime", "StartTime", "EndTime":
				v = strconv.FormatInt(r.sortNumber(k.field), 10)
				if k.field == "StartTime" && !o.steps {
					v = strconv.FormatInt(startTime(r), 10)
				}
			default:
				v, err = r.value(k.field, o)
			}
			if err != nil {
				return err
			}
			it.values = append(it.values, v)
		}
		items[i] = it
	}
	slices.SortStableFunc(items, func(a, b item) int {
		tieOrder := 1
		for i, k := range keys {
			av, bv := a.values[i], b.values[i]
			n := strings.Compare(av, bv)
			if k.field == "NodeList" || k.field == "Nodes" {
				if av == bv {
					// In 26.05.4 an ascending hostlist pass reverses
					// the prior order of records with empty hostlists.
					if av == "" && !k.desc {
						tieOrder = -tieOrder
					}
					n = 0
				} else if av == "" {
					n = 1
				} else if bv == "" {
					n = -1
				} else if utils.HostLess(av, bv) {
					n = -1
				} else {
					n = 1
				}
			} else if k.field == "id" {
				if av != bv {
					if utils.NaturalLess(av, bv) {
						n = -1
					} else {
						n = 1
					}
				}
			} else if slices.Contains([]string{"TimeUsed", "TimeLimit", "TimeLeft", "MinMemory", "NumNodes", "NumCPUs", "PriorityLong", "UserId", "GroupId", "SubmitTime", "StartTime", "EndTime"}, k.field) {
				if ai, e := strconv.ParseInt(av, 10, 64); e == nil {
					if bi, e := strconv.ParseInt(bv, 10, 64); e == nil {
						n = cmp.Compare(ai, bi)
					}
				}
			}
			if n != 0 {
				if k.field == "StartTime" && !o.steps && (av == "0" || bv == "0") {
					if av == "0" {
						return tieOrder
					}
					return -tieOrder
				}
				if k.desc {
					return -n * tieOrder
				}
				return n * tieOrder
			}
		}
		return cmp.Compare(a.index, b.index) * tieOrder
	})
	for i := range items {
		rows[i] = items[i].r
	}
	return nil
}

func (r row) sortNumber(field string) int64 {
	j := r.job
	used, limit, start := j.ElapsedTime.GetSeconds(), j.TimeLimit.GetSeconds(), j.StartTime.GetSeconds()
	if s := r.step; s != nil {
		used, limit, start = s.ElapsedTime.GetSeconds(), s.TimeLimit.GetSeconds(), s.StartTime.GetSeconds()
	}
	if j.Status == pb.JobStatus_Pending {
		used = 0
	}
	switch field {
	case "TimeUsed":
		return used
	case "TimeLimit":
		return limit
	case "TimeLeft":
		return max(0, limit-used)
	case "StartTime":
		return start
	case "EndTime":
		return j.EndTime.GetSeconds()
	case "SubmitTime":
		return j.SubmitTime.GetSeconds()
	default:
		return int64(r.minMemory() >> 20)
	}
}

// Slurm 26.05.4 applies -L to comma-separated requested names. An OR
// expression is one token, so only its first counted alternative matches.
func matchesLicenses(filter []string, request *pb.LicenseRequest) bool {
	if len(filter) == 0 {
		return true
	}
	for _, entry := range request.GetEntries() {
		if slices.Contains(filter, entry.Name) {
			return true
		}
		if request.AnyOf {
			break
		}
	}
	return false
}

func matchesFold(filter []string, value string) bool {
	return len(filter) == 0 || slices.ContainsFunc(filter, func(s string) bool { return strings.EqualFold(s, value) })
}
