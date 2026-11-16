// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package ccontrol

import (
	"context"
	"strings"
	"testing"

	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/testutil"
	"google.golang.org/grpc"
)

type nodeUpdateClient struct {
	protos.CraneCtldClient
	request *protos.ModifyCranedStateRequest
}

func (c *nodeUpdateClient) ModifyNode(_ context.Context, request *protos.ModifyCranedStateRequest, _ ...grpc.CallOption) (*protos.ModifyCranedStateReply, error) {
	c.request = request
	return &protos.ModifyCranedStateReply{ModifiedNodes: request.CranedIds}, nil
}

func TestUpdateNodeReason(t *testing.T) {
	oldStub, oldNode, oldJSON := stub, FlagNodeName, FlagJson
	t.Cleanup(func() { stub, FlagNodeName, FlagJson = oldStub, oldNode, oldJSON })
	FlagJson = false
	for _, tc := range []struct {
		command string
		state   protos.CranedControlState
		reason  string
		err     string
	}{
		{"update nodeName=n01 state=drain reason=maintenance", protos.CranedControlState_CRANE_DRAIN, "maintenance", ""},
		// Keep these cases sequential: an omitted reason must not reuse the
		// preceding command's value in an interactive session.
		{"update nodeName=n01 state=drain", 0, "", "You must specify a reason"},
		{"update nodeName=n01 state=drain reason=''", 0, "", "You must specify a reason"},
		{"update nodeName=n01 state=resume", protos.CranedControlState_CRANE_NONE, "", ""},
		{"update NodeName=n01 State=RESUME Reason=unused", protos.CranedControlState_CRANE_NONE, "", ""},
		{"update nodeName=n01 state=off", protos.CranedControlState_CRANE_POWEROFF, "", ""},
		{"update nodeName=n01 reason=maintenance", 0, "", "missing required fields: state"},
		{"update nodeName=n01 state=invalid", 0, "", "Invalid state given"},
	} {
		t.Run(tc.command, func(t *testing.T) {
			client := &nodeUpdateClient{}
			stub = client
			command, err := CControlParser.ParseString("", tc.command)
			if err != nil {
				t.Fatal(err)
			}
			_, err = testutil.CaptureStdout(t, func() error { return executeUpdateCommand(command) })
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) || client.request != nil {
					t.Fatalf("err=%v request=%v; want local error containing %q", err, client.request, tc.err)
				}
				return
			}
			request := client.request
			if err != nil || request == nil {
				t.Fatalf("err=%v request=%v", err, request)
			}
			if request.NewState != tc.state || request.Reason != tc.reason || len(request.CranedIds) != 1 || request.CranedIds[0] != "n01" {
				t.Fatalf("request=%v; want state=%v reason=%q node=n01", request, tc.state, tc.reason)
			}
		})
	}
}
