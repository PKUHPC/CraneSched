// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	"cmp"
	"io"
	"slices"
	"testing"

	pb "github.com/PKUHPC/CraneSched/go/generated/protos"
)

func TestConfiguringStateIncludesStartingJobs(t *testing.T) {
	reply := &pb.QueryJobsInfoReply{JobInfoList: []*pb.JobInfo{
		{JobId: 1, Status: pb.JobStatus_Configuring},
		{JobId: 2, Status: pb.JobStatus_Starting},
		{JobId: 3, Status: pb.JobStatus_Running},
	}}
	for _, filter := range []string{"configuring", "CF"} {
		o, err := parse([]string{"-t", filter}, nil, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := o.rows(reply)
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, r := range rows {
			ids = append(ids, r.id())
			for field, want := range map[string]string{"State": "CONFIGURING", "StateCompact": "CF"} {
				if got, err := r.value(field, o); err != nil || got != want {
					t.Errorf("job %s %s = %q, %v; want %q", r.id(), field, got, err, want)
				}
			}
		}
		slices.Sort(ids)
		if !slices.Equal(ids, []string{"1", "2"}) {
			t.Errorf("-t %s selected %v, want jobs 1 and 2", filter, ids)
		}
	}
}

func TestJobAndStepQueueFields(t *testing.T) {
	job := &pb.JobInfo{JobId: 17, Type: pb.JobType_Batch, SubmitHostname: "login", SubmittedSize: &pb.SubmittedJobSize{MaxNodes: 4}, MaxStepWaitStatus: new(uint32(7 << 8)), CpusPerTask: new(2.0), MaxNodes: new(uint32(4)), NodeNum: 2, Ntasks: new(uint32(2)), ReqTotalResView: &pb.ResourceView{CpuCount: 4}, AllocatedResView: &pb.ResourceView{CpuCount: 2}}
	o := &options{}
	check := func(r row, field, want string) {
		t.Helper()
		got, err := r.value(field, o)
		if err != nil || got != want {
			t.Fatalf("%s = %q, %v, want %q", field, got, err, want)
		}
	}
	r := row{job: job}
	check(r, "DerivedEC", "7:0")
	check(r, "AllocNodes", "login")
	check(r, "MaxCPUs", "4")
	check(r, "MaxNodes", "4")
	check(r, "tres-per-task", "cpu=2")
	report := jobReport(r, o)
	if report.DerivedExitCode.ReturnCode.Number != 7 || report.MaxCPUs.Number != 0 || report.MaxNodes.Number != 4 {
		t.Fatal(report)
	}
	job.Status = pb.JobStatus_Running
	job.ExecutionNode = []string{"n1", "n2"}
	check(r, "MaxCPUs", "2")
	check(r, "MaxNodes", "2")
	step := row{job: job, step: &pb.StepInfo{StepType: pb.StepType_COMMON, StepId: 2, CpusPerTask: new(1.0)}}
	check(step, "tres-per-task", "cpu=2")
	job.CpusPerTask = nil
	check(r, "tres-per-task", "N/A")
	check(step, "tres-per-task", "(null)")
	s := stepReport(step, o)
	if s.Array.TaskID != nil {
		t.Fatal(s.Array)
	}
	job.ArrayTask = &pb.ArrayTaskIdentity{ArrayJobId: 4, TaskId: 0}
	if x := stepReport(step, o).Array; x.TaskID == nil || *x.TaskID != 0 || x.JobID != 4 {
		t.Fatal(x)
	}
	job.ExitCode = 256 + 15
	check(r, "exit_code", "0:15")
	if x := jobReport(r, o).ExitCode; x.Signal.Name != "TERM" || x.Signal.ID.Number != 15 {
		t.Fatal(x)
	}
	job.MaxStepWaitStatus = nil
	if _, err := r.value("DerivedEC", o); err == nil || jobReport(r, o).DerivedExitCode != nil {
		t.Fatal("missing outcome was reported")
	}
}

func TestSubmittedTaskLayout(t *testing.T) {
	job := &pb.JobInfo{Status: pb.JobStatus_Pending, MaxNodes: new(uint32(1)), CpusPerTask: new(2.0)}
	r := row{job: job}
	o := &options{}
	for _, tc := range []struct {
		name      string
		size      *pb.SubmittedJobSize
		maxNodes  uint32
		ntPerNode string
	}{
		{"unknown submission", nil, 0, "N/A"},
		{"unspecified counts", &pb.SubmittedJobSize{}, 0, "0"},
		{"explicit node limit", &pb.SubmittedJobSize{MaxNodes: 4}, 1, "0"},
		{"explicit task count", &pb.SubmittedJobSize{Tasks: 1}, 1, "0"},
		{"tasks per node", &pb.SubmittedJobSize{TasksPerNode: 2}, 2, "2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job.SubmittedSize = tc.size
			if got, err := r.value("NTPerNode", o); err != nil || got != tc.ntPerNode {
				t.Errorf("NTPerNode = %q, %v; want %q", got, err, tc.ntPerNode)
			}
			report := jobReport(r, o)
			if tc.size == nil {
				if report.TasksPerNode != nil || report.MaxNodes != nil {
					t.Error("unknown submission produced task or node counts")
				}
				return
			}
			if n := report.TasksPerNode; n == nil || !n.Set || uint32(n.Number) != tc.size.TasksPerNode {
				t.Errorf("tasks_per_node = %v, want %d", n, tc.size.TasksPerNode)
			}
			if n := report.MaxNodes; n == nil || !n.Set || uint32(n.Number) != tc.maxNodes {
				t.Errorf("maximum_nodes = %v, want %d", n, tc.maxNodes)
			}
		})
	}
}

func TestStateTableArrayBitmap(t *testing.T) {
	j := &pb.JobInfo{JobId: 4591, ArraySpec: &pb.ArraySpec{Start: 1, End: 21, Stride: new(uint32(2)), MaxConcurrent: new(uint32(1))}}
	for _, tc := range []struct {
		env    map[string]string
		wantID string
	}{
		{nil, "4591_[1,3,5,7,9,11,13,15,17,19,"},
		{map[string]string{"SLURM_BITSTR_LEN": "0"}, "4591_[1,3,5,7,9,11,13,15,17,19,21]"},
	} {
		o, err := parse([]string{"--only-job-state"}, tc.env, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := o.rows(&pb.QueryJobsInfoReply{JobInfoList: []*pb.JobInfo{j}})
		if err != nil || len(rows) != 1 {
			t.Fatalf("rows = %v, error = %v", rows, err)
		}
		for field, want := range map[string]string{"JobArrayId": tc.wantID, "ArrayTaskId": "1,3,5,7,9,11,13,15,17,19,21"} {
			if got, err := rows[0].value(field, o); err != nil || got != want {
				t.Errorf("%s = %q, %v; want %q", field, got, err, want)
			}
		}
	}
}

func TestLicenseRequestAndAllocation(t *testing.T) {
	request := &pb.LicenseRequest{Entries: []*pb.LicenseRequest_Entry{
		{Name: "fluent", Count: 2}, {Name: "ansys", Count: 1},
	}}
	for _, tc := range []struct {
		name                   string
		anyOf                  bool
		granted                map[string]uint32
		wantRequest, wantGrant string
		matchSecond            bool
	}{
		{"pending alternatives", true, nil, "fluent:2|ansys:1", "", false},
		{"second alternative granted", true, map[string]uint32{"ansys": 1}, "fluent:2|ansys:1", "ansys:1", false},
		{"both required and granted", false, map[string]uint32{"ansys": 1, "fluent": 2}, "fluent:2,ansys:1", "fluent:2;ansys:1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request.AnyOf = tc.anyOf
			r := row{job: &pb.JobInfo{RequestedLicenses: request, AllocatedLicenses: tc.granted}}
			for field, want := range map[string]string{"Licenses": tc.wantRequest, "LicensesAlloc": cmp.Or(tc.wantGrant, "(null)")} {
				if got, err := r.value(field, &options{}); err != nil || got != want {
					t.Errorf("%s = %q, %v; want %q", field, got, err, want)
				}
			}
			if !matchesLicenses([]string{"fluent"}, request) || matchesLicenses([]string{"ansys"}, request) != tc.matchSecond {
				t.Errorf("unexpected -L match for request %s", tc.wantRequest)
			}
			report := jobReport(r, &options{})
			if report.Licenses != tc.wantRequest || report.LicensesAllocated != tc.wantGrant {
				t.Errorf("structured licenses = %q, %q", report.Licenses, report.LicensesAllocated)
			}
		})
	}
}
