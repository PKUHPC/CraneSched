package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"encoding/json"
	"net"
	"reflect"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func clusterFixture() *protos.QueryClusterInfoReply {
	return &protos.QueryClusterInfoReply{
		Ok: true,
		Partitions: []*protos.PartitionInfo{
			{Name: "empty", State: protos.PartitionState_PARTITION_DOWN},
			{Name: "batch", IsDefault: true, State: protos.PartitionState_PARTITION_UP, TotalNodes: 3, Hostlist: "n[1-3]"},
			{Name: "shared", State: protos.PartitionState_PARTITION_UP, TotalNodes: 2, Hostlist: "n[1-2]"},
		},
		Nodes: []*protos.CranedInfo{
			{Hostname: "n2", PartitionNames: []string{"batch", "shared"}, ResourceState: protos.CranedResourceState_CRANE_IDLE, PowerState: protos.CranedPowerState_CRANE_POWER_IDLE},
			{Hostname: "n1", PartitionNames: []string{"batch", "shared"}, ResourceState: protos.CranedResourceState_CRANE_IDLE, PowerState: protos.CranedPowerState_CRANE_POWER_IDLE},
			{Hostname: "n3", PartitionNames: []string{"batch"}, ResourceState: protos.CranedResourceState_CRANE_DOWN, PowerState: protos.CranedPowerState_CRANE_POWER_IDLE, StateReason: "offline"},
		},
	}
}

func TestNativeGroupingAndEmptyPartitions(t *testing.T) {
	reply := clusterFixture()
	before := proto.Clone(reply)
	got := FindTableDataByReply(reply)
	want := [][]string{
		{"batch*", "up", "2", "idle[power_idle]", "n[1-2]"},
		{"batch*", "up", "1", "down[failed]", "n3"},
		{"shared", "up", "2", "idle[power_idle]", "n[1-2]"},
		{"empty", "down", "0", "n/a", ""},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if !proto.Equal(before, reply) {
		t.Fatal("grouping mutated native records")
	}
	reply.Nodes[0].StateReason = "maintenance"
	rows := FlattenReplyData(reply)
	if len(rows) != 6 || rows[0].CranedListRegex != "n1" || rows[1].Reason != "maintenance" {
		t.Fatalf("reason grouping/order: %+v", rows)
	}
}

func TestClientStateFiltersKeepPartitionFacts(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	fixture := &clusterQueryController{requests: make(chan *protos.QueryClusterInfoRequest, 1)}
	protos.RegisterCraneCtldServer(server, fixture)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	config := &util.Config{ControlMachine: host, CraneCtldListenPort: port}
	oldStates, oldResponding, oldDown := FlagFilterCranedStates, FlagFilterRespondingOnly, FlagFilterDownOnly
	t.Cleanup(func() {
		FlagFilterCranedStates, FlagFilterRespondingOnly, FlagFilterDownOnly = oldStates, oldResponding, oldDown
	})
	for _, tc := range []struct {
		name             string
		states           []string
		responding, down bool
		want             []string
	}{
		{name: "default", want: []string{"n2", "n1", "n3"}},
		{name: "responding", responding: true, want: []string{"n2", "n1"}},
		{name: "down", down: true, want: []string{"n3"}},
		{name: "explicit_overrides_default", states: []string{"idle"}, down: true, want: []string{"n2", "n1"}},
		{name: "cross_dimension_and", states: []string{"idle", "drain"}, want: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			FlagFilterCranedStates, FlagFilterRespondingOnly, FlagFilterDownOnly = tc.states, tc.responding, tc.down
			reply, err := queryClusterInfo(config)
			if err != nil {
				t.Fatal(err)
			}
			req := <-fixture.requests
			if !req.IncludeNodes || !req.IncludePartitions || req.IncludeReservations || req.IncludeAllocationCleanup || req.Uid != nil {
				t.Fatalf("unexpected query scope: %+v", req)
			}
			got := []string{}
			for _, n := range reply.Nodes {
				got = append(got, n.Hostname)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
			if reply.Partitions[1].TotalNodes != 3 || reply.Partitions[1].Hostlist != "n[1-3]" {
				t.Fatal("filtered partition facts")
			}
			// Native JSON uses protobuf serialization and has no summary facade.
			raw := util.FmtJson.FormatReply(reply)
			var decoded map[string]json.RawMessage
			if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded["nodes"] == nil || strings.Contains(raw, "craned_lists") || strings.Contains(raw, "batch*") {
				t.Fatal(raw)
			}
			if len(tc.want) == 0 && string(decoded["nodes"]) != "[]" {
				t.Fatal(raw)
			}
		})
	}
	FlagFilterCranedStates = []string{"invalid"}
	if _, err := parseStateFilter(); err == nil {
		t.Fatal("invalid state accepted")
	}
}

type clusterQueryController struct {
	protos.UnimplementedCraneCtldServer
	requests chan *protos.QueryClusterInfoRequest
}

func (s *clusterQueryController) QueryClusterInfo(_ context.Context, req *protos.QueryClusterInfoRequest) (*protos.QueryClusterInfoReply, error) {
	s.requests <- req
	return clusterFixture(), nil
}

func TestExistingSlurmOutputUsesNativeRows(t *testing.T) {
	got, err := formatSlurmClusterJSON(clusterFixture())
	if err != nil {
		t.Fatal(err)
	}
	var reply slurmClusterOutput
	if err := json.Unmarshal([]byte(got), &reply); err != nil {
		t.Fatal(err)
	}
	if !reply.OK || reply.Partitions[0].NodeLists == nil || len(reply.Partitions[0].NodeLists) != 0 ||
		reply.Partitions[1].Name != "batch*" || reply.Partitions[1].NodeLists[0].NodeList != "n[1-2]" ||
		reply.Partitions[1].NodeLists[1].Reason != "offline" {
		t.Fatal(got)
	}
}
