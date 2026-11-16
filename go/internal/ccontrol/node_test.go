// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package ccontrol

import (
	"context"
	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/testutil"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type nodeClient struct {
	protos.CraneCtldClient
	created     *protos.CreateNodesRequest
	deleted     *protos.DeleteNodesRequest
	modified    *protos.ModifyCranedStateRequest
	deleteReply *protos.DeleteNodesReply
}

func (c *nodeClient) CreateNodes(_ context.Context, request *protos.CreateNodesRequest, _ ...grpc.CallOption) (*protos.CreateNodesReply, error) {
	c.created = request
	return &protos.CreateNodesReply{}, nil
}

func (c *nodeClient) DeleteNodes(_ context.Context, request *protos.DeleteNodesRequest, _ ...grpc.CallOption) (*protos.DeleteNodesReply, error) {
	c.deleted = request
	return c.deleteReply, nil
}

func (c *nodeClient) ModifyNode(_ context.Context, request *protos.ModifyCranedStateRequest, _ ...grpc.CallOption) (*protos.ModifyCranedStateReply, error) {
	c.modified = request
	return &protos.ModifyCranedStateReply{}, nil
}

func useNodeClient(t *testing.T) *nodeClient {
	t.Helper()
	oldStub, oldJSON := stub, FlagJson
	t.Cleanup(func() { stub, FlagJson = oldStub, oldJSON })
	client := &nodeClient{}
	stub, FlagJson = client, false
	return client
}

func runNodeCommand(t *testing.T, input string, execute func(*CControlCommand) error) (string, error) {
	t.Helper()
	command, err := ParseCControlCommand(input)
	if err != nil {
		t.Fatalf("parse %q: %v", input, err)
	}
	return testutil.CaptureStdout(t, func() error { return execute(command) })
}

func TestCreateNodeBuildsFutureDefinitions(t *testing.T) {
	client := useNodeClient(t)
	for _, tc := range []struct {
		input string
		want  []*protos.DynamicNodeDefinition
	}{
		{
			input: "create node NodeName=f[1-2] State=future CPUs=8 RealMemory=16G Sockets=2 Partition=cpu,gpu Features=a,b",
			want: []*protos.DynamicNodeDefinition{
				{Name: "f1", Cpu: 8, MemoryBytes: 16 << 30, Sockets: 2, Features: []string{"a", "b"}, Partitions: []string{"cpu", "gpu"}},
				{Name: "f2", Cpu: 8, MemoryBytes: 16 << 30, Sockets: 2, Features: []string{"a", "b"}, Partitions: []string{"cpu", "gpu"}},
			},
		},
		{
			// Sockets defaults to 1 and RealMemory to MiB.
			input: "create node nodename=f3 state=FUTURE cpus=4 realmemory=2048 partition=cpu",
			want: []*protos.DynamicNodeDefinition{
				{Name: "f3", Cpu: 4, MemoryBytes: 2048 << 20, Sockets: 1, Partitions: []string{"cpu"}},
			},
		},
	} {
		if _, err := runNodeCommand(t, tc.input, executeCreateCommand); err != nil {
			t.Fatalf("%q: %v", tc.input, err)
		}
		if !slices.EqualFunc(client.created.Nodes, tc.want, func(got, want *protos.DynamicNodeDefinition) bool {
			return proto.Equal(got, want)
		}) {
			t.Fatalf("%q: got %v want %v", tc.input, client.created.Nodes, tc.want)
		}
	}
}

func TestCreateNodeRejectsInvalidDefinitionsLocally(t *testing.T) {
	client := useNodeClient(t)
	const valid = "State=FUTURE CPUs=4 RealMemory=8G Partition=cpu"
	for input, reason := range map[string]string{
		"create node f1 " + valid:                                                           "Specify nodes with NodeName",
		"create node NodeName=f1 State=FUTURE CPUs=4 RealMemory=8G":                         "Missing node attribute: partition",
		"create node NodeName=f1 State=IDLE CPUs=4 RealMemory=8G Partition=cpu":             "Only State=FUTURE",
		"create node NodeName=f1 Sockets=4 State=FUTURE CPUs=6 RealMemory=8G Partition=cpu": "divisible by Sockets",
		"create node NodeName=f1 Gres=gpu:1 " + valid:                                       "Unknown node attribute: Gres",
		"create node NodeName=f1 CPUs=8 " + valid:                                           "Duplicate node attribute: CPUs",
		"create node NodeName=f1 State=FUTURE CPUs=4 RealMemory=8G Partition=cpu,cpu":       "Invalid Partition list",
	} {
		_, err := runNodeCommand(t, input, executeCreateCommand)
		if err == nil || !strings.Contains(err.Error(), reason) {
			t.Errorf("%q: got %v, want %q", input, err, reason)
		}
	}
	if client.created != nil {
		t.Fatalf("invalid definitions were sent: %v", client.created)
	}
}

func TestDeleteNodeReportsEachNode(t *testing.T) {
	client := useNodeClient(t)
	client.deleteReply = &protos.DeleteNodesReply{
		DeletedNodes: []string{"f1"}, NotDeletedNodes: []string{"f2"},
		NotDeletedReasons: []string{"Node is online. Stop its craned first."},
	}
	output, err := runNodeCommand(t, "delete node NodeName=f[1-2]", executeDeleteCommand)
	if !slices.Equal(client.deleted.NodeNames, []string{"f1", "f2"}) {
		t.Fatalf("deleted %v", client.deleted.NodeNames)
	}
	if !strings.Contains(output, "Node f1 deleted successfully.") ||
		err == nil || !strings.Contains(err.Error(), "Node f2 was not deleted: Node is online.") {
		t.Fatalf("output=%q err=%v", output, err)
	}

	// Attributes on delete must not change how other entities are named.
	command, err := ParseCControlCommand("delete reservation r1")
	if err != nil || command.GetID() != "r1" {
		t.Fatalf("reservation id=%q err=%v", command.GetID(), err)
	}
	if _, err = runNodeCommand(t, "delete reservation Name=r1", executeDeleteCommand); err == nil {
		t.Fatal("reservation attributes accepted")
	}
}

func TestResetNodeToFutureNeedsNoReason(t *testing.T) {
	client := useNodeClient(t)
	if _, err := testutil.CaptureStdout(t, func() error { return ChangeNodeState("f[1-2]", "FUTURE", "") }); err != nil {
		t.Fatal(err)
	}
	if client.modified.NewState != protos.CranedControlState_CRANE_RESET_TO_FUTURE ||
		!slices.Equal(client.modified.CranedIds, []string{"f1", "f2"}) {
		t.Fatalf("unexpected request: %v", client.modified)
	}
}
