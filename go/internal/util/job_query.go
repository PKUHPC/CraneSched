// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package util

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"os"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const RestrictedJobField = "<restricted>"

// QueryJobsInfo supplies the caller, not the user selected by query filters.
// The server authenticates that declaration and applies the access policy.
func QueryJobsInfo(ctx context.Context, client protos.CraneCtldClient, request *protos.QueryJobsInfoRequest) (*protos.QueryJobsInfoReply, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "job query request is required")
	}
	req := proto.Clone(request).(*protos.QueryJobsInfoRequest)
	req.Uid = proto.Uint32(uint32(os.Getuid()))
	reply, err := client.QueryJobsInfo(ctx, req)
	if err != nil {
		return nil, err
	}
	if reply == nil || !reply.Ok {
		return nil, status.Error(codes.Internal, "failed to query jobs")
	}
	for _, job := range reply.JobInfoList {
		if job == nil || !validQueryVisibility(job.QueryVisibility) {
			return nil, status.Error(codes.Internal, "job query returned invalid visibility")
		}
		for _, step := range job.StepInfoList {
			if step == nil || !validQueryVisibility(step.QueryVisibility) ||
				(job.QueryVisibility == protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC &&
					step.QueryVisibility == protos.QueryVisibility_QUERY_VISIBILITY_DETAILS) {
				return nil, status.Error(codes.Internal, "job query returned invalid step visibility")
			}
		}
	}
	return reply, nil
}

func validQueryVisibility(visibility protos.QueryVisibility) bool {
	return visibility == protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC ||
		visibility == protos.QueryVisibility_QUERY_VISIBILITY_DETAILS
}

// PrivateJobField is for text only; JSON retains the server's original types.
func PrivateJobField(value string, visibility protos.QueryVisibility) string {
	if visibility != protos.QueryVisibility_QUERY_VISIBILITY_DETAILS {
		return RestrictedJobField
	}
	return value
}

func RequireJobDetails(visibility protos.QueryVisibility) error {
	if visibility != protos.QueryVisibility_QUERY_VISIBILITY_DETAILS {
		return NewCraneErr(ErrorBackend, "Job details are restricted; an authenticated owner or administrator is required")
	}
	return nil
}
