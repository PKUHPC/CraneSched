// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"

	pb "github.com/PKUHPC/CraneSched/go/generated/protos"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestQueuePresentation(t *testing.T) {
	first := &pb.JobInfo{JobId: 7, Name: "RunLong", Partition: "cpu", Username: "alice", Status: pb.JobStatus_Running, ElapsedTime: &durationpb.Duration{Seconds: 600}}
	second := &pb.JobInfo{JobId: 19, Name: "runlong", Partition: "cpu", Username: "bob", Status: pb.JobStatus_Running, ElapsedTime: &durationpb.Duration{Seconds: 120}}
	reply := &pb.QueryJobsInfoReply{Ok: true, JobInfoList: []*pb.JobInfo{first, second}}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"case-insensitive filter and elapsed sort", []string{"-n", "RUNLONG", "-S", "M", "-h", "-o", "%i|%j|%M"}, "19|runlong|2:00\n7|RunLong|10:00\n"},
		{"state-only records", []string{"--only-job-state", "-h", "-o", "%i|%t|%j|%P|%M"}, "7|R|(null)|(null)|0:00\n19|R|(null)|(null)|0:00\n"},
		{"inclusive running-over", []string{"--running-over=10", "-h", "-O", "JobId:0|,TimeUsed:0"}, "7|10:00\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, e := parse(tc.args, nil, io.Discard)
			if e != nil {
				t.Fatal(e)
			}
			cols, e := o.columns(io.Discard)
			if e != nil {
				t.Fatal(e)
			}
			rows, e := o.rows(reply)
			if e != nil {
				t.Fatal(e)
			}
			var out bytes.Buffer
			for i := range rows {
				if e := printLine(&out, cols, &rows[i], o); e != nil {
					t.Fatal(e)
				}
			}
			if got := out.String(); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

type referenceQueue struct {
	pb.UnimplementedCraneCtldServer
	jobs []*pb.JobInfo
}

func (s *referenceQueue) QueryJobsInfo(_ context.Context, req *pb.QueryJobsInfoRequest) (*pb.QueryJobsInfoReply, error) {
	reply := &pb.QueryJobsInfoReply{Ok: true}
	for _, j := range s.jobs {
		if len(req.FilterJobIds) > 0 && !slices.ContainsFunc(req.FilterJobIds, func(id *pb.JobIdSelector) bool { return id.JobId == j.JobId }) {
			continue
		}
		if len(req.FilterPartitions) > 0 && !slices.Contains(req.FilterPartitions, j.Partition) {
			continue
		}
		reply.JobInfoList = append(reply.JobInfoList, j)
	}
	return reply, nil
}

// The golden streams were captured from squeue 26.05.4 with a held job,
// a running batch containing a numeric step, and a held 1-5:2%1 array.
// Replay the same public facts without requiring slurm.conf or a live scheduler.
func TestSlurm26054Queue(t *testing.T) {
	details := pb.QueryVisibility_QUERY_VISIBILITY_DETAILS
	job := func(id uint32, name string) *pb.JobInfo {
		return &pb.JobInfo{
			JobId: id, Name: name, Type: pb.JobType_Batch, Partition: "compute", Account: "root", Username: "root", Qos: "normal",
			Held: true, NodeNum: 1, Ntasks: new(uint32(1)), TimeLimit: &durationpb.Duration{Seconds: 900}, Cwd: "/tmp",
			RequeueEligible: new(true), ScriptPath: new(""), RequeueIfFailed: true, QueryVisibility: details, IoMeta: &pb.IoMeta{OutputFilePattern: "/tmp/sqcmp-%j.out"},
			ReqTotalResView: &pb.ResourceView{CpuCount: 1, MemoryBytes: 100 << 20},
		}
	}
	held, active, array := job(4517, "sqcmp-held"), job(4518, "sqcmp-run"), job(4519, "sqcmp-array")
	active.Held = false
	active.Status = pb.JobStatus_Running
	active.ExecutionNode = []string{"slurm03"}
	active.PendingReasonOrCranedList = &pb.JobInfo_CranedList{CranedList: "slurm03"}
	array.ArraySpec = &pb.ArraySpec{Start: 1, End: 5, Stride: new(uint32(2)), MaxConcurrent: new(uint32(1))}
	for i, kind := range []pb.StepType{pb.StepType_DAEMON, pb.StepType_PRIMARY, pb.StepType_COMMON} {
		active.StepInfoList = append(active.StepInfoList, &pb.StepInfo{JobId: 4518, StepId: uint32(i), StepType: kind, Name: "sleep",
			Status: pb.JobStatus_Running, TimeLimit: &durationpb.Duration{Seconds: 315576000000}, Ntasks: new(uint32(1)),
			ExecutionNode: []string{"slurm03"}, QueryVisibility: details})
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterCraneCtldServer(server, &referenceQueue{jobs: []*pb.JobInfo{held, active, array}})
	go server.Serve(l)
	t.Cleanup(server.Stop)
	config := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(config, []byte(fmt.Sprintf("ControlMachine: 127.0.0.1\nCraneCtldListenPort: '%d'\n", l.Addr().(*net.TCPAddr).Port)), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open("testdata/slurm-26.05.4.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		var tc struct {
			Name           string
			Args           []string
			Env            map[string]string
			Stdout, Stderr string
			RC             int
		}
		if err := json.Unmarshal(scan.Bytes(), &tc); err != nil {
			t.Fatal(err)
		}
		t.Run(tc.Name, func(t *testing.T) {
			var out, diag bytes.Buffer
			env := []string{"TZ=UTC", "LC_ALL=C"}
			for k, v := range tc.Env {
				env = append(env, k+"="+v)
			}
			rc := execute(t.Context(), config, append([]string{"squeue"}, tc.Args...), env, &out, &diag)
			if rc != tc.RC || out.String() != tc.Stdout || diag.String() != tc.Stderr {
				t.Errorf("%q: got (%d, %q, %q), want (%d, %q, %q)", tc.Args, rc, out.String(), diag.String(), tc.RC, tc.Stdout, tc.Stderr)
			}
		})
	}
	if err := scan.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestStepFiltersUseStepFacts(t *testing.T) {
	o, e := parse([]string{"-s", "42.0", "-w", "n2", "-t", "pending"}, nil, io.Discard)
	if e != nil {
		t.Fatal(e)
	}
	j := &pb.JobInfo{JobId: 42, Type: pb.JobType_Batch, Status: pb.JobStatus_Running, ExecutionNode: []string{"n1"}, StepInfoList: []*pb.StepInfo{
		{StepId: 1, StepType: pb.StepType_PRIMARY, Status: pb.JobStatus_Running, ExecutionNode: []string{"n1"}},
		{StepId: 2, StepType: pb.StepType_COMMON, Status: pb.JobStatus_Running, ExecutionNode: []string{"n2"}},
	}}
	rows, e := o.rows(&pb.QueryJobsInfoReply{JobInfoList: []*pb.JobInfo{j}})
	if e != nil || len(rows) != 1 || rows[0].stepID() != "42.0" {
		t.Fatalf("steps %v, err %v", rows, e)
	}
}

func TestJobReportKeepsKnownFalseAndFractionalBilling(t *testing.T) {
	job := &pb.JobInfo{
		Type: pb.JobType_Batch, Status: pb.JobStatus_Pending,
		RequeueEligible: new(false), ReqTotalResView: &pb.ResourceView{CpuCount: 0.5},
	}
	o, err := parse([]string{"--json", "--sort=?"}, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := o.rows(&pb.QueryJobsInfoReply{JobInfoList: []*pb.JobInfo{job}})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := o.writeReport(&buf, reportOptions{}, []string{"squeue", "--json"}, rows); err != nil {
		t.Fatal(err)
	}
	var data struct{ Jobs []map[string]json.RawMessage }
	if err := json.Unmarshal(buf.Bytes(), &data); err != nil || len(data.Jobs) != 1 {
		t.Fatalf("jobs: %v, err: %v", data.Jobs, err)
	}
	record := data.Jobs[0]
	for _, missing := range []string{"cpus", "command", "het_job_id", "submit_time"} {
		if _, ok := record[missing]; ok {
			t.Errorf("unexpected %s: %s", missing, record[missing])
		}
	}
	if string(record["requeue"]) != "false" {
		t.Errorf("requeue = %s", record["requeue"])
	}
	var billing struct {
		Set    bool
		Number float64
	}
	if err := json.Unmarshal(record["billable_tres"], &billing); err != nil || !billing.Set || billing.Number != 0.5 {
		t.Errorf("billable_tres = %s, err %v", record["billable_tres"], err)
	}
}
