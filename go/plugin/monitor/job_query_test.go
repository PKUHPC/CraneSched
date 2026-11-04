// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type jobInformationClient struct {
	protos.CraneCtldClient
	ctx     context.Context
	request *protos.QueryJobsInfoRequest
	reply   *protos.QueryJobsInfoReply
	err     error
	calls   int
}

func (c *jobInformationClient) QueryJobsInfo(ctx context.Context, req *protos.QueryJobsInfoRequest, _ ...grpc.CallOption) (*protos.QueryJobsInfoReply, error) {
	c.ctx = ctx
	c.request = req
	c.calls++
	return c.reply, c.err
}

func TestGetJobInformationWithClientPublicSummary(t *testing.T) {
	callerUID := uint32(os.Getuid())
	job := &protos.JobInfo{
		JobId:                     42,
		Uid:                       callerUID + 1,
		QueryVisibility:           protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC,
		PendingReasonOrCranedList: &protos.JobInfo_CranedList{CranedList: "node[01-02]"},
	}
	wantJob := proto.Clone(job)
	client := &jobInformationClient{
		reply: &protos.QueryJobsInfoReply{Ok: true, JobInfoList: []*protos.JobInfo{job}},
	}

	start := time.Now()
	jobs, err := getJobInformationWithClient(context.Background(), client, []uint32{42, 7})
	end := time.Now()
	if err != nil {
		t.Fatalf("PUBLIC summary query failed: %v", err)
	}
	if client.calls != 1 || client.request == nil {
		t.Fatalf("expected one job query, got calls=%d request=%v", client.calls, client.request)
	}
	if client.request.Uid == nil || client.request.GetUid() != callerUID {
		t.Errorf("caller UID must be present and equal process UID %d, got %v", callerUID, client.request.Uid)
	}
	wantRequest := &protos.QueryJobsInfoRequest{
		Uid:                        proto.Uint32(callerUID),
		FilterJobIds:               []*protos.JobIdSelector{{JobId: 42}, {JobId: 7}},
		OptionIncludeCompletedJobs: true,
	}
	if !proto.Equal(client.request, wantRequest) {
		t.Errorf("job query request = %v, want %v", client.request, wantRequest)
	}
	deadline, ok := client.ctx.Deadline()
	if !ok || deadline.Before(start.Add(10*time.Second)) || deadline.After(end.Add(10*time.Second)) {
		t.Errorf("job query deadline = %v (present=%v), want a 10-second timeout", deadline, ok)
	}
	if !errors.Is(client.ctx.Err(), context.Canceled) {
		t.Errorf("job query context not canceled on return: %v", client.ctx.Err())
	}
	if len(jobs) != 1 || !proto.Equal(jobs[0], wantJob) {
		t.Fatalf("PUBLIC summary changed: got %v, want %v", jobs, wantJob)
	}
}

func TestGetJobInformationWithClientPreservesParentDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := &jobInformationClient{reply: &protos.QueryJobsInfoReply{Ok: true}}
	if _, err := getJobInformationWithClient(ctx, client, []uint32{42}); err != nil {
		t.Fatal(err)
	}
	want, _ := ctx.Deadline()
	got, ok := client.ctx.Deadline()
	if !ok || !got.Equal(want) {
		t.Fatalf("job query deadline = %v (present=%v), want parent deadline %v", got, ok, want)
	}
}

func TestGetJobInformationWithClientErrors(t *testing.T) {
	publicJob := &protos.JobInfo{JobId: 42, QueryVisibility: protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC}
	for _, tc := range []struct {
		name   string
		reply  *protos.QueryJobsInfoReply
		rpcErr error
		code   codes.Code
	}{
		{
			name:   "RPC failure",
			rpcErr: status.Error(codes.Unavailable, "ctld unavailable"),
			code:   codes.Unavailable,
		},
		{
			name:  "business failure",
			reply: &protos.QueryJobsInfoReply{Ok: false, JobInfoList: []*protos.JobInfo{publicJob}},
			code:  codes.Internal,
		},
		{
			name:  "missing visibility",
			reply: &protos.QueryJobsInfoReply{Ok: true, JobInfoList: []*protos.JobInfo{publicJob, {JobId: 7}}},
			code:  codes.Internal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &jobInformationClient{reply: tc.reply, err: tc.rpcErr}
			jobs, err := getJobInformationWithClient(context.Background(), client, []uint32{42, 7})
			if jobs != nil || status.Code(err) != tc.code {
				t.Fatalf("jobs=%v err=%v, want nil jobs and %s", jobs, err, tc.code)
			}
			if !strings.Contains(err.Error(), "failed to query jobs info from CraneCtld") {
				t.Errorf("missing job query error context: %v", err)
			}
			if tc.rpcErr != nil && !errors.Is(err, tc.rpcErr) {
				t.Errorf("RPC error was not preserved: %v", err)
			}
		})
	}
}
