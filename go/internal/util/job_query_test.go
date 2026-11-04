// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package util

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"encoding/json"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type jobQueryClient struct {
	protos.CraneCtldClient
	request *protos.QueryJobsInfoRequest
	reply   *protos.QueryJobsInfoReply
	err     error
}

func (c *jobQueryClient) QueryJobsInfo(_ context.Context, request *protos.QueryJobsInfoRequest, _ ...grpc.CallOption) (*protos.QueryJobsInfoReply, error) {
	c.request = request
	return c.reply, c.err
}

func TestQueryJobsInfoCallerAndFilters(t *testing.T) {
	request := &protos.QueryJobsInfoRequest{Uid: proto.Uint32(12345), FilterUsers: []string{"some-other-user"}, NumLimit: 7, OptionIncludeCompletedJobs: true, FilterJobIds: []*protos.JobIdSelector{{JobId: 42, Steps: []uint32{1}}}}
	original := proto.Clone(request)
	client := &jobQueryClient{reply: &protos.QueryJobsInfoReply{Ok: true, HasMore: true}}
	reply, err := QueryJobsInfo(context.Background(), client, request)
	if err != nil {
		t.Fatal(err)
	}
	if client.request.Uid == nil || client.request.GetUid() != uint32(os.Getuid()) {
		t.Fatalf("caller UID not supplied: %v", client.request.Uid)
	}
	expected := proto.Clone(request).(*protos.QueryJobsInfoRequest)
	expected.Uid = proto.Uint32(uint32(os.Getuid()))
	if !proto.Equal(expected, client.request) || !proto.Equal(original, request) {
		t.Fatal("query filters or original request changed")
	}
	if !reply.HasMore {
		t.Fatal("pagination flag lost")
	}
}

func TestQueryJobsInfoRejectsInvalidResponses(t *testing.T) {
	public := protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC
	details := protos.QueryVisibility_QUERY_VISIBILITY_DETAILS
	for _, tc := range []struct {
		name   string
		reply  *protos.QueryJobsInfoReply
		rpcErr error
		code   codes.Code
	}{
		{"nil reply", nil, nil, codes.Internal},
		{"business failure", &protos.QueryJobsInfoReply{}, nil, codes.Internal},
		{"missing visibility", &protos.QueryJobsInfoReply{Ok: true, JobInfoList: []*protos.JobInfo{{}}}, nil, codes.Internal},
		{"nil job", &protos.QueryJobsInfoReply{Ok: true, JobInfoList: []*protos.JobInfo{nil}}, nil, codes.Internal},
		{"unknown visibility", &protos.QueryJobsInfoReply{Ok: true, JobInfoList: []*protos.JobInfo{{QueryVisibility: 99}}}, nil, codes.Internal},
		{"missing step visibility", &protos.QueryJobsInfoReply{Ok: true, JobInfoList: []*protos.JobInfo{{QueryVisibility: details, StepInfoList: []*protos.StepInfo{{}}}}}, nil, codes.Internal},
		{"nil step", &protos.QueryJobsInfoReply{Ok: true, JobInfoList: []*protos.JobInfo{{QueryVisibility: details, StepInfoList: []*protos.StepInfo{nil}}}}, nil, codes.Internal},
		{"step exceeds parent", &protos.QueryJobsInfoReply{Ok: true, JobInfoList: []*protos.JobInfo{{QueryVisibility: public, StepInfoList: []*protos.StepInfo{{QueryVisibility: details}}}}}, nil, codes.Internal},
		{"bad identity", nil, status.Error(codes.Unauthenticated, "bad identity"), codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reply, err := QueryJobsInfo(context.Background(), &jobQueryClient{reply: tc.reply, err: tc.rpcErr}, &protos.QueryJobsInfoRequest{})
			if reply != nil || status.Code(err) != tc.code {
				t.Fatalf("reply=%v err=%v", reply, err)
			}
		})
	}
}

func TestQueryJobsInfoMixedViewsAndJSON(t *testing.T) {
	public := protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC
	details := protos.QueryVisibility_QUERY_VISIBILITY_DETAILS
	source := &protos.QueryJobsInfoReply{Ok: true, JobInfoList: []*protos.JobInfo{
		{JobId: 1, QueryVisibility: public, StepInfoList: []*protos.StepInfo{{StepId: 1, QueryVisibility: public}}},
		{JobId: 2, QueryVisibility: details, CmdLine: "owner-command", StepInfoList: []*protos.StepInfo{{StepId: 1, QueryVisibility: public}}},
	}}
	reply, err := QueryJobsInfo(context.Background(), &jobQueryClient{reply: source}, &protos.QueryJobsInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	slurm, err := FormatSlurmJobsJSON(reply)
	if err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{FmtJson.FormatReply(reply), slurm} {
		var parsed struct {
			Jobs []map[string]json.RawMessage `json:"job_info_list"`
		}
		if err := json.Unmarshal([]byte(output), &parsed); err != nil {
			t.Fatal(err)
		}
		if len(parsed.Jobs) != 2 || string(parsed.Jobs[0]["cmd_line"]) != `""` || string(parsed.Jobs[0]["env"]) != `{}` {
			t.Fatalf("public JSON types changed: %s", output)
		}
		if string(parsed.Jobs[1]["cmd_line"]) != `"owner-command"` {
			t.Fatal("owner details lost")
		}
	}
}

func TestPrivateJobFieldAndRequiredDetails(t *testing.T) {
	for _, visibility := range []protos.QueryVisibility{0, 1, 2, 99} {
		for _, value := range []string{"", "owner-command"} {
			want := RestrictedJobField
			if visibility == protos.QueryVisibility_QUERY_VISIBILITY_DETAILS {
				want = value
			}
			if got := PrivateJobField(value, visibility); got != want {
				t.Fatalf("visibility=%v got=%q want=%q", visibility, got, want)
			}
		}
		err := RequireJobDetails(visibility)
		if (err == nil) != (visibility == protos.QueryVisibility_QUERY_VISIBILITY_DETAILS) {
			t.Fatalf("visibility=%v err=%v", visibility, err)
		}
	}
}
