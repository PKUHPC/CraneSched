// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

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
		local = "slurm 26.05.4\n"
	case "helpstate":
		local = helpState
	case "helpformat", "helpFormat":
		local = o.helpFields(o.action == "helpformat")
	case "parsers":
		fmt.Fprintln(diag, "Possible data_parser plugins:")
		_, err = fmt.Fprintln(out, "data_parser/v0.0.45")
		return err
	case "autocomplete":
		local = completion(o.actionValue)
	}
	if o.action != "" {
		_, err = io.WriteString(out, local)
		return err
	}
	var cols []column
	var report reportOptions
	if o.serializer != "" {
		report, err = o.reportOptions(diag)
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
	for {
		if !o.noHeader && (o.long || o.iterate > 0 || o.verbose > 0) {
			if _, err = fmt.Fprintln(out, time.Now().In(utils.CommandTimezone(env)).Format(time.ANSIC)); err != nil {
				return err
			}
		}
		var reply *pb.QueryJobsInfoReply
		err = session.Query(ctx, func(ctx context.Context, client pb.CraneCtldClient, uid uint32) error {
			ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			if o.clusterName == "" && (o.serializer != "" || slices.ContainsFunc(cols, func(c column) bool { return c.field.name == "Cluster" })) {
				info, err := client.QueryClusterInfo(ctx, &pb.QueryClusterInfoRequest{Uid: &uid})
				if err != nil {
					return err
				}
				if !info.GetOk() {
					return fmt.Errorf("cluster information unavailable")
				}
				o.clusterName = info.GetClusterName()
			}
			req := o.req
			// Slurm loads a single job by its allocation ID, then applies array
			// and display filters. A missing task in an existing array is empty.
			if !o.steps && len(req.FilterJobIds) == 1 {
				req = &pb.QueryJobsInfoRequest{IncludePublicQueueDetails: req.IncludePublicQueueDetails, Mode: req.Mode, NumLimit: req.NumLimit, FilterJobIds: []*pb.JobIdSelector{{JobId: req.FilterJobIds[0].JobId}}}
			}
			var e error
			reply, e = util.QueryJobsInfo(ctx, client, req)
			return e
		})
		if err != nil {
			return fmt.Errorf("slurm_load_jobs error: %w", err)
		}
		if !reply.GetOk() || reply.HasMore {
			return fmt.Errorf("squeue: error: incomplete queue response")
		}
		if !o.onlyState && len(list(o.jobIDs)) == 1 && len(reply.JobInfoList) == 0 && !o.steps {
			return fmt.Errorf("slurm_load_jobs error: Invalid job id specified")
		}
		if o.verbose > 0 && !o.onlyState && o.serializer == "" {
			n := len(reply.JobInfoList)
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
		if report.schema {
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
func completion(q string) string {
	if q == "" {
		return ""
	}
	var result strings.Builder
	for _, op := range vocabulary {
		if op.Short != 0 {
			short := "-" + string(op.Short)
			if strings.Contains(short, q) {
				fmt.Fprintln(&result, short)
			}
		}
		s := "--" + op.Name
		if strings.Contains(s, q) {
			if op.Argument == required || op.Argument == optional {
				fmt.Fprintln(&result, s+"=")
			}
			if op.Argument == optional {
				fmt.Fprintln(&result, s+" ")
			}
			if op.Argument == none {
				fmt.Fprintln(&result, s)
			}
		}
	}
	if result.Len() > 0 {
		result.WriteByte('\n')
	}
	return result.String()
}

func (o *options) rows(reply *pb.QueryJobsInfoReply) ([]row, error) {
	var result []row
	for _, j := range reply.JobInfoList {
		if o.steps {
			for _, step := range j.StepInfoList {
				if step.Status == pb.JobStatus_Pending || o.users != nil && !slices.Contains(o.users, num(step.Uid)) {
					continue
				}
				if o.nodes != nil && !slices.ContainsFunc(step.ExecutionNode, o.nodes.Match) {
					continue
				}
				r := row{job: j, step: step}
				if o.stepIDs == "" || slices.Contains(list(o.stepIDs), r.stepID()) {
					result = append(result, r)
				}
			}
			continue
		}
		if len(o.req.FilterJobIds) == 1 && o.req.FilterJobIds[0].ArrayTaskId != nil && !util.IsPendingArrayParent(j) {
			sel := o.req.FilterJobIds[0]
			a := j.ArrayTask
			if a == nil || a.ArrayJobId != sel.JobId || a.TaskId != *sel.ArrayTaskId {
				continue
			}
		}
		if len(o.req.FilterPartitions) > 0 && !slices.Contains(o.req.FilterPartitions, j.Partition) {
			continue
		}
		if len(o.req.FilterLicenses) > 0 && !slices.ContainsFunc(o.req.FilterLicenses, func(s string) bool { _, ok := j.LicensesCount[s]; return ok }) {
			continue
		}
		if !matchesFold(o.accounts, j.Account) || !matchesFold(o.qos, j.Qos) || !matchesFold(o.names, j.Name) {
			continue
		}
		if o.users != nil && !slices.Contains(o.users, num(j.Uid)) || o.notUser == num(j.Uid) || !matchesFold(o.reservations, j.Reservation) {
			continue
		}
		if !(o.onlyState && o.serializer != "") && len(o.states) > 0 && !slices.Contains(o.states, j.Status) {
			continue
		}
		if o.nodes != nil && !slices.ContainsFunc(j.ExecutionNode, o.nodes.Match) {
			continue
		}
		if o.over > 0 || o.under > 0 {
			t := j.ElapsedTime.GetSeconds()
			if j.Status != pb.JobStatus_Running || o.over > 0 && t < o.over || o.under > 0 && t >= o.under {
				continue
			}
		}
		if o.onlyState {
			// Slurm builds text and structured output from the identity/state response.
			j = &pb.JobInfo{JobId: j.JobId, ArrayTask: j.ArrayTask, ArraySpec: j.ArraySpec, Status: j.Status, Username: "root"}
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
				if len(indexes) == 1 || o.array {
					for _, i := range indexes {
						result = append(result, row{job: j, task: new(i)})
					}
				} else if len(indexes) > 0 {
					result = append(result, row{job: j, selection: taskRanges(indexes)})
				}
				continue
			}
		}
		if o.array && util.IsPendingArrayParent(j) {
			spec := j.ArraySpec
			stride := max(1, spec.GetStride())
			for i := uint64(spec.Start); i <= uint64(spec.End); i += uint64(stride) {
				result = append(result, row{job: j, task: new(uint32(i))})
			}
			continue
		}
		result = append(result, row{job: j})
	}
	if o.serializer != "" {
		return result, nil
	}
	return result, o.sortRows(result)
}

func (o *options) sortRows(rows []row) error {
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
	}
	items := make([]item, len(rows))
	for i, r := range rows {
		it := item{r: r}
		for _, k := range keys {
			var v string
			var err error
			switch k.field {
			case "id":
				v = r.id()
				if r.step != nil {
					v = r.stepID()
				}
			case "TimeUsed", "TimeLeft", "TimeLimit", "MinMemory", "SubmitTime", "StartTime", "EndTime":
				v = strconv.FormatInt(r.sortNumber(k.field), 10)
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
		for i, k := range keys {
			av, bv := a.values[i], b.values[i]
			n := strings.Compare(av, bv)
			if k.field == "NodeList" || k.field == "Nodes" {
				if av == bv {
					n = 0
				} else if av == "" {
					n = 1
				} else if bv == "" {
					n = -1
				} else if utils.NaturalLess(av, bv) {
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
				if k.desc {
					return -n
				}
				return n
			}
		}
		return 0
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
		return int64(j.GetReqTotalResView().GetMemoryBytes() / uint64(max(1, j.NodeNum)))
	}
}

func matchesFold(filter []string, value string) bool {
	return len(filter) == 0 || slices.ContainsFunc(filter, func(s string) bool { return strings.EqualFold(s, value) })
}
