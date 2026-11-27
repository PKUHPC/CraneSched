// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/PKUHPC/CraneSched/go/generated/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestQueuePresentation(t *testing.T) {
	first := &pb.JobInfo{JobId: 7, Name: "RunLong", Partition: "cpu", Username: "alice", Status: pb.JobStatus_Running, ElapsedTime: &durationpb.Duration{Seconds: 600}}
	second := &pb.JobInfo{JobId: 19, Name: "runlong", Partition: "cpu", Username: "bob", Status: pb.JobStatus_Running, ElapsedTime: &durationpb.Duration{Seconds: 120}}
	first.MemoryPerCpu = new(uint64(1 << 30))
	first.ReqTotalResView = &pb.ResourceView{MemoryBytes: 8 << 30, CpuCount: 8}
	second.ReqTotalResView = &pb.ResourceView{MemoryBytes: 2 << 30, CpuCount: 1}
	reply := &pb.QueryJobsInfoReply{Ok: true, JobInfoList: []*pb.JobInfo{first, second}}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"case-insensitive filter and elapsed sort", []string{"-n", "RUNLONG", "-S", "M", "-h", "-o", "%i|%j|%M"}, "19|runlong|2:00\n7|RunLong|10:00\n"},
		{"state-only records", []string{"--only-job-state", "-h", "-o", "%i|%t|%j|%P|%M"}, "7|R|(null)|(null)|0:00\n19|R|(null)|(null)|0:00\n"},
		{"sort by memory request per CPU or per node", []string{"-S", "m", "-o", "%i|%m"}, "7|1G\n19|2G\n"},
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

func (s *referenceQueue) QueryClusterInfo(_ context.Context, req *pb.QueryClusterInfoRequest) (*pb.QueryClusterInfoReply, error) {
	reply := &pb.QueryClusterInfoReply{Ok: true, ClusterName: "compute"}
	if req.IncludeNodes {
		reply.Nodes = []*pb.CranedInfo{{Hostname: "slurm03", NodeHostname: "host03"}, {Hostname: "slurm02"}, {Hostname: "slurm01"}}
	}
	return reply, nil
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
		if len(req.FilterReservations) > 0 && !slices.Contains(req.FilterReservations, j.Reservation) {
			continue
		}
		reply.JobInfoList = append(reply.JobInfoList, j)
	}
	return reply, nil
}

func serveQueue(t *testing.T, controller pb.CraneCtldServer) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterCraneCtldServer(server, controller)
	go server.Serve(l)
	t.Cleanup(server.Stop)
	config := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(config, []byte(fmt.Sprintf("ControlMachine: 127.0.0.1\nCraneCtldListenPort: '%d'\n", l.Addr().(*net.TCPAddr).Port)), 0600); err != nil {
		t.Fatal(err)
	}
	return config
}

func TestQueueTextOutput(t *testing.T) {
	visibility := pb.QueryVisibility_QUERY_VISIBILITY_DETAILS
	held := &pb.JobInfo{
		JobId: 7, Type: pb.JobType_Batch, Name: "compile", Partition: "cpu", Held: true,
		PendingReasonOrCranedList: &pb.JobInfo_PendingReason{PendingReason: "JobHeldAdmin"},
		TimeLimit:                 durationpb.New(15 * time.Minute), QueryVisibility: visibility,
		ReqTotalResView: &pb.ResourceView{MemoryBytes: 128 << 20}, NodeNum: 1,
		IoMeta: &pb.IoMeta{OutputFilePattern: "/tmp/build-%j.log"},
	}
	active := &pb.JobInfo{
		JobId: 8, Type: pb.JobType_Batch, Status: pb.JobStatus_Running, Reservation: "Maint01", Partition: "cpu", Uid: 1001, Username: "alice",
		PendingReasonOrCranedList: &pb.JobInfo_CranedList{CranedList: "slurm[02-03]"}, ExecutionNode: []string{"slurm02"},
		QueryVisibility: visibility,
		StepInfoList: []*pb.StepInfo{
			{StepType: pb.StepType_DAEMON, StepId: 0, Status: pb.JobStatus_Running, QueryVisibility: visibility},
			{StepType: pb.StepType_PRIMARY, StepId: 1, Status: pb.JobStatus_Running, QueryVisibility: visibility},
			{StepType: pb.StepType_COMMON, StepId: 2, QueryVisibility: visibility, Name: "worker", Status: pb.JobStatus_Running,
				Ntasks: new(uint32(3)), CranedList: "slurm[02-03]", ExecutionNode: []string{"slurm02"}, TimeLimit: durationpb.New(time.Minute)},
		},
	}
	config := serveQueue(t, &referenceQueue{jobs: []*pb.JobInfo{held, active}})
	for _, tc := range []struct {
		name           string
		args, env      []string
		stdout, stderr string
	}{
		{"literal prefix, alignment and truncation", []string{"-hj7", "-o", "job=%3j|%.9j|%t"}, nil, "job=com|  compile|PD\n", ""},
		{"held job reason and unassigned nodes", []string{"-hj7", "-o", "%r|%R|%N"}, nil, "JobHeldAdmin|(JobHeldAdmin)|\n", ""},
		{"job stream defaults and literal pattern", []string{"-hj7", "-O", "StdIn:0|,StdOut:0|,StdErr:0"}, nil, "/dev/null|/tmp/build-%j.log|(null)\n", ""},
		{"long format from environment", []string{"-hj7"}, []string{"SQUEUE_FORMAT2=Name:0|,MinMemory:0|,TimeLeft:0"}, "compile|128M|15:00\n", ""},
		{"short format wins over long environment format", []string{"-hj7"}, []string{"SQUEUE_FORMAT=id=%i", "SQUEUE_FORMAT2=Name:0"}, "id=7\n", ""},
		{"command line overrides format and state environment", []string{"-hj7", "-o", "%T", "-t", "PD"}, []string{"SQUEUE_FORMAT2=Name:0", "SQUEUE_STATES=RUNNING"}, "PENDING\n", ""},
		{"environment name filter", []string{"-hj7", "-o", "%i"}, []string{"SQUEUE_NAMES=missing"}, "", ""},
		{"environment partition filter", []string{"-hj7", "-o", "%i"}, []string{"SQUEUE_PARTITION=other"}, "", ""},
		{"state text filters on state records without a partition", []string{"--only-job-state", "-h", "-o", "%i"}, []string{"SQUEUE_PARTITION=cpu"}, "", ""},
		{"state text filters on the synthetic UID", []string{"--only-job-state", "-hj8", "-o", "%i"}, []string{"SQUEUE_USERS=0"}, "8\n", ""},
		{"pending job excluded by elapsed filter", []string{"-hj7", "--running-under=15", "-o", "%i"}, nil, "", ""},
		{"invalid job field is echoed", []string{"-hj7", "-o", "%j/%@@"}, nil, "compile/%@@\n", "squeue: error: Invalid job format specification: @\n"},
		{"job nodelist width", []string{"-j8", "-o", "%.4N|%4N"}, nil, "NODELIST|NODE\nslurm[02-03]|slur\n", ""},
		{"step nodelist width", []string{"-s8.0", "-o", "%.4N|%4N"}, nil, "NODELIST|NODE\nslurm[02-03]|slur\n", ""},
		{"job filter matches secondary node alias", []string{"-hj8", "-w", "host03", "-o", "%i"}, nil, "8\n", ""},
		{"step filter matches secondary node alias", []string{"-hs8.0", "-w", "host03", "-o", "%i"}, nil, "8.0\n", ""},
		{"reservation list", []string{"-h", "-R", `"other,Maint[01-02,04]"`, "-o", "%i|%v"}, nil, "8|Maint01\n", ""},
		{"empty reservation selects no jobs", []string{"-h", "--reservation=", "-o", "%i"}, nil, "", ""},
		{"single job in reservation", []string{"-hj8", "-R", "Maint01", "-o", "%i"}, nil, "8\n", ""},
		{"reservation case mismatch on single job", []string{"-hj8", "-R", "maint01", "-o", "%i"}, nil, "", ""},
		{"steps ignore job reservation", []string{"-hs8.0", "-R", "other", "-o", "%i"}, nil, "8.0\n", ""},
		{"job exists outside node filter", []string{"-hj8", "-w", "slurm01", "-o", "%i"}, nil, "", ""},
		{"job numeric UID with leading zeros", []string{"-hj8", "-u", "01001", "-o", "%U"}, nil, "1001\n", ""},
		{"step owner differs from job owner", []string{"-hs8.0", "-u", "0000", "-o", "%U|%u"}, nil, "0|root\n", ""},
		{"empty format code", []string{"-hj7", "-o", "%%"}, nil, "\n", ""},
		{"step uses its own short fields", []string{"-hs8.0", "-o", "%i|%j|%N|%A|%t"}, nil, "8.0|worker|slurm[02-03]|3|%t\n", "squeue: error: Invalid job step format specification: t\n"},
		{"step stream and array defaults", []string{"-hs8.0", "-O", "ArrayJobId:0|,ArrayTaskId:0|,StdIn:0|,StdOut:0|,StdErr:0"}, nil, "0|N/A|||\n", ""},
		{"batch selector uses numeric step zero", []string{"-hs8.batch", "-o", "%i"}, nil, "8.0\n", ""},
		{"extern selector uses numeric step zero", []string{"-hs8.extern", "-o", "%i"}, nil, "8.0\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, diag bytes.Buffer
			rc := execute(t.Context(), config, append([]string{"squeue"}, tc.args...), tc.env, &out, &diag)
			if rc != 0 || out.String() != tc.stdout || diag.String() != tc.stderr {
				t.Errorf("exit %d, stdout %q, stderr %q; want 0, %q, %q", rc, &out, &diag, tc.stdout, tc.stderr)
			}
		})
	}
	var out, diag bytes.Buffer
	rc := execute(t.Context(), config, []string{"squeue", "-hj999", "-R", "Maint"}, nil, &out, &diag)
	if rc != 1 || out.Len() != 0 || diag.String() != "slurm_load_jobs error: Invalid job id specified\n" {
		t.Errorf("missing job: exit %d, stdout %q, stderr %q", rc, &out, &diag)
	}
}

func TestVerboseJobSnapshot(t *testing.T) {
	public := pb.QueryVisibility_QUERY_VISIBILITY_PUBLIC
	config := serveQueue(t, &referenceQueue{jobs: []*pb.JobInfo{
		{JobId: 7, Reservation: "Maint01", Uid: 1001, QueryVisibility: public},
		{JobId: 8, Uid: 1001, QueryVisibility: public},
		{JobId: 9, Uid: 1002, QueryVisibility: public},
	}})
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"-R", "Maint01"}, "3\n7\n"},
		{[]string{"-j7,9", "-R", "Maint01"}, "3\n7\n"},
		{[]string{"-j8,9"}, "3\n8\n9\n"},
		{[]string{"-u1001", "-R", "Maint01"}, "2\n7\n"},
		{[]string{"-j8", "-R", "Maint01"}, "1\n"},
	} {
		var out, diag bytes.Buffer
		rc := execute(t.Context(), config, append([]string{"squeue", "-vh", "-o", "%i"}, tc.args...), nil, &out, &diag)
		_, records, found := strings.Cut(out.String(), " records=")
		if rc != 0 || diag.Len() != 0 || !found || records != tc.want {
			t.Errorf("%v: exit %d, records and rows %q, stderr %q", tc.args, rc, records, &diag)
		}
	}
}

func TestStateReportIgnoresDisplayFilters(t *testing.T) {
	public := pb.QueryVisibility_QUERY_VISIBILITY_PUBLIC
	config := serveQueue(t, &referenceQueue{jobs: []*pb.JobInfo{
		{JobId: 7, Partition: "cpu", QueryVisibility: public}, {JobId: 8, Partition: "cpu", QueryVisibility: public},
	}})
	var out, diag bytes.Buffer
	rc := execute(t.Context(), config, []string{"squeue", "--only-job-state", "--json", "-j7"}, []string{"SQUEUE_PARTITION=other", "SQUEUE_STATES=RUNNING"}, &out, &diag)
	var result struct {
		Jobs []struct {
			ID string `json:"job_id"`
		}
	}
	if err := json.Unmarshal(out.Bytes(), &result); rc != 0 || err != nil || diag.Len() != 0 || len(result.Jobs) != 1 || result.Jobs[0].ID != "7" {
		t.Fatalf("exit %d, jobs %v, stderr %q, decode error %v", rc, result.Jobs, &diag, err)
	}
}

func TestReservationQueryScope(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want []string
	}{
		{[]string{"-R", "old", "--reservation=Maint, other"}, []string{"Maint", "other"}},
		{[]string{"-R", "'Maint[01,03-04],other'ignored"}, []string{"Maint01", "Maint03", "Maint04", "other"}},
		{[]string{"-R", "Maint", "-j7,8", "--json", "-v"}, []string{"Maint"}},
		{[]string{"-R", "Maint", "-j8"}, nil},
		{[]string{"-R", "Maint", "-s"}, nil},
	} {
		o, err := parse(tc.args, nil, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if got := o.queueRequest().FilterReservations; !slices.Equal(got, tc.want) {
			t.Errorf("%v sent reservations %q, want %q", tc.args, got, tc.want)
		}
	}
}

func TestQueueOptionDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		args           []string
		code           int
		stdout, stderr string
	}{
		{[]string{"--usage", "--bad"}, 0, usage, ""},
		{[]string{"--bad", "--usage"}, 1, "", "squeue: unrecognized option '--bad'\nTry \"squeue --help\" for more information\n"},
		{[]string{"operand"}, 1, usage, "squeue: error: Unrecognized option: operand\n"},
		{[]string{"-j0"}, 1, "", "squeue: error: Invalid job id: 0\n"},
		{[]string{"-sinvalid"}, 1, "", "squeue: error: Invalid job_step id: invalid.??\n"},
		{[]string{"-j", "-s"}, 1, "", "squeue: error: Incompatible options --jobs and --steps\n"},
		{[]string{"-o", "%j", "-O", "Name"}, 1, "", "squeue: error: -O (--Format) is incompatible with -o (--format)\n"},
		{[]string{"-O", "Name", "-o", "%j"}, 1, "", "squeue: error: -o (--format) is incompatible with -O (--Format)\n"},
		{[]string{"--running-under=1", "--running-over=3"}, 1, "", "squeue: fatal: --running-over=00:03:00 and --running-under=00:01:00 will return 0 jobs.\n"},
		{[]string{"-t", "NODE_FAIL", "--help"}, 1, "", "squeue: error: current Crane version does not support job state NODE_FAIL\n"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var out, diag bytes.Buffer
			rc := execute(t.Context(), "", append([]string{"squeue"}, tc.args...), nil, &out, &diag)
			if rc != tc.code || out.String() != tc.stdout || diag.String() != tc.stderr {
				t.Errorf("exit %d, stdout %q, stderr %q; want %d, %q, %q", rc, &out, &diag, tc.code, tc.stdout, tc.stderr)
			}
		})
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
	o.nodeNames = map[string]bool{"n2": true}
	rows, e := o.rows(&pb.QueryJobsInfoReply{JobInfoList: []*pb.JobInfo{j}})
	if e != nil || len(rows) != 1 || rows[0].stepID() != "42.0" {
		t.Fatalf("steps %v, err %v", rows, e)
	}
}

func TestJobSortWithoutAllocatedNodes(t *testing.T) {
	pending := func(id uint32, name string) *pb.JobInfo {
		return &pb.JobInfo{JobId: id, Name: name, Status: pb.JobStatus_Pending}
	}
	array, held, active := pending(3, "array"), pending(1, "held"), pending(2, "running")
	active.Status = pb.JobStatus_Running
	active.StartTime = timestamppb.New(time.Unix(1700000000, 0))
	active.ExecutionNode = []string{"n1"}
	for _, tc := range []struct {
		order string
		want  []string
	}{
		{"N,j", []string{"running", "held", "array"}}, {"-N,j", []string{"array", "held", "running"}}, {"b", []string{"running", "array", "held"}},
	} {
		o, err := parse([]string{"-S", tc.order}, nil, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := o.rows(&pb.QueryJobsInfoReply{JobInfoList: []*pb.JobInfo{array, held, active}})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, r := range rows {
			got = append(got, r.job.Name)
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: %v, want %v", tc.order, got, tc.want)
		}
	}
}

func TestStepQueryScopes(t *testing.T) {
	active := func(id uint32) *pb.JobInfo {
		return &pb.JobInfo{JobId: id, Type: pb.JobType_Batch, Status: pb.JobStatus_Running,
			StepInfoList: []*pb.StepInfo{{StepId: 2, StepType: pb.StepType_COMMON, Status: pb.JobStatus_Running}}}
	}
	first, child := active(41), active(93)
	child.ArrayTask = &pb.ArrayTaskIdentity{ArrayJobId: 42, TaskId: 3}
	reply := &pb.QueryJobsInfoReply{JobInfoList: []*pb.JobInfo{first, child}}
	for _, tc := range []struct {
		args []string
		want []string
	}{
		{[]string{"-j41", "--steps=42.0"}, nil},
		{[]string{"--steps=42.0"}, []string{"42_3.0"}},
		{[]string{"--steps=42_3.0"}, []string{"42_3.0"}},
		{[]string{"--steps=93.0"}, []string{"42_3.0"}},
		{[]string{"-j41", "--steps=42.0", "--json"}, []string{"41.0", "42_3.0"}},
		{[]string{"-j41", "-s", "-p", "absent", "--yaml"}, []string{"41.0", "42_3.0"}},
	} {
		o, err := parse(tc.args, nil, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if req := o.queueRequest(); o.serializer != "" && (len(req.FilterJobIds) > 0 || len(req.FilterPartitions) > 0 || !req.IncludePublicQueueDetails || !req.OptionQueryStepsOnly) {
			t.Fatalf("%v: wrong request %v", tc.args, req)
		}
		rows, err := o.rows(reply)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, r := range rows {
			got = append(got, r.stepID())
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("%v: got %v, want %v", tc.args, got, tc.want)
		}
	}
}

func TestPendingArraySelection(t *testing.T) {
	j := &pb.JobInfo{JobId: 42, ArraySpec: &pb.ArraySpec{Start: 1, End: 5, Stride: new(uint32(2)), MaxConcurrent: new(uint32(1))}}
	for _, tc := range []struct {
		args []string
		want []string
	}{
		{[]string{"-j42"}, []string{"42_[1,3,5%1]"}},
		{[]string{"-j42_3,42_5"}, []string{"42_[3,5]"}},
		{[]string{"-r", "-j42_3,42_5"}, []string{"42_3", "42_5"}},
		{[]string{"-j42_2"}, nil},
		{[]string{"-r", "--json", "-j42"}, []string{"42_[1,3,5%1]"}},
		{[]string{"-r", "--json", "-j42_1,42_5"}, []string{"42_[1,5]"}},
		{[]string{"--only-job-state", "--yaml", "-j42_3"}, []string{"42_1", "42_3", "42_5"}},
		{[]string{"--only-job-state", "--json", "-j42_2"}, nil},
		{[]string{"--only-job-state", "--json", "-j42_1,42_5"}, []string{"42_1", "42_3", "42_5", "42_1", "42_3", "42_5"}},
	} {
		o, err := parse(tc.args, nil, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := o.rows(&pb.QueryJobsInfoReply{JobInfoList: []*pb.JobInfo{j}})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, r := range rows {
			got = append(got, r.id())
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("%v: %v, want %v", tc.args, got, tc.want)
		}
	}
}

type intermittentQueue struct {
	pb.UnimplementedCraneCtldServer
	calls atomic.Int32
	stop  context.CancelFunc
}

func (s *intermittentQueue) QueryJobsInfo(_ context.Context, _ *pb.QueryJobsInfoRequest) (*pb.QueryJobsInfoReply, error) {
	switch s.calls.Add(1) {
	case 1:
		return nil, status.Error(codes.Unavailable, "test outage")
	case 2:
		return &pb.QueryJobsInfoReply{Ok: true, JobInfoList: []*pb.JobInfo{{JobId: 12, QueryVisibility: pb.QueryVisibility_QUERY_VISIBILITY_PUBLIC}}}, nil
	default:
		s.stop()
		return nil, status.Error(codes.Canceled, "test finished")
	}
}

func TestIterateRecoversAfterQueryFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	c := &intermittentQueue{stop: cancel}
	var out, diag bytes.Buffer
	execute(ctx, serveQueue(t, c), []string{"squeue", "-hi1", "-o", "%i"}, nil, &out, &diag)
	if !strings.Contains(diag.String(), "test outage") || !strings.Contains(out.String(), "12\n") || c.calls.Load() < 3 {
		t.Fatalf("calls=%d, stdout=%q stderr=%q", c.calls.Load(), out.String(), diag.String())
	}
}

func TestQueueNodeInventory(t *testing.T) {
	server := &referenceQueue{jobs: []*pb.JobInfo{{JobId: 12, ExecutionNode: []string{"slurm03"}, QueryVisibility: pb.QueryVisibility_QUERY_VISIBILITY_PUBLIC}}}
	config := serveQueue(t, server)
	for _, name := range []string{"slurm03", "host03", "host[03,09]"} {
		var out, diag bytes.Buffer
		rc := execute(t.Context(), config, []string{"squeue", "-h", "-w", name, "-o", "%i"}, nil, &out, &diag)
		if name == "host[03,09]" {
			if rc != 1 || out.Len() != 0 || diag.String() != "squeue: error: Invalid node name host09\n" {
				t.Fatalf("%s: %d %q %q", name, rc, &out, &diag)
			}
		} else if rc != 0 || out.String() != "12\n" {
			t.Fatalf("%s: %d %q %q", name, rc, &out, &diag)
		}
	}
}

func TestQueueLocalOptionPrecedence(t *testing.T) {
	for _, tc := range []struct {
		args    []string
		env     []string
		message string
	}{
		{[]string{"--json", "--version"}, []string{"SLURM_JSON=unknown"}, "Unable to parse serializer"},
		{[]string{"-i3", "-ibad", "--help"}, nil, "--iterate=bad"},
		{[]string{"--only-job-state", "--reservation=", "--json"}, nil, "Option --only-job-state only works alone"},
		{[]string{"--only-job-state", "--accounts=", "operand"}, nil, "Option --only-job-state only works alone"},
		{[]string{"--only-job-state"}, []string{"SQUEUE_ALL=1"}, "Option --only-job-state only works alone"},
	} {
		var out, diag bytes.Buffer
		if rc := execute(t.Context(), "", append([]string{"squeue"}, tc.args...), tc.env, &out, &diag); rc != 1 || out.Len() != 0 || !strings.Contains(diag.String(), tc.message) {
			t.Errorf("%v: %d %q %q", tc.args, rc, &out, &diag)
		}
	}
	var out, diag bytes.Buffer
	rc := execute(t.Context(), "", []string{"squeue", "--autocomplete=-"}, nil, &out, &diag)
	if rc != 0 || strings.Contains(out.String(), "--") || !strings.Contains(out.String(), "-j\n") {
		t.Fatalf("%d %q %q", rc, &out, &diag)
	}
}
