// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	"bytes"
	"io"
	"testing"

	pb "github.com/PKUHPC/CraneSched/go/generated/protos"
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
