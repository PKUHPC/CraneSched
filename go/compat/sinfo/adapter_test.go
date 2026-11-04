// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"CraneFrontEnd/generated/protos"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func adapterFixture() *protos.QueryClusterInfoReply {
	allocation := &protos.ResourceInNodeV3{CpuCount: 1, MemoryBytes: 1 << 30}
	return &protos.QueryClusterInfoReply{
		Ok: true, ClusterName: "example", ObservedAt: timestamppb.New(time.Unix(100, 0)),
		Nodes: []*protos.CranedInfo{{
			Hostname: "n1", NodeHostname: "host1", NodeAddr: "192.0.2.1", CranedVersion: "Crane-test",
			PartitionNames: []string{"batch"},
			ResourceState:  protos.CranedResourceState_CRANE_ALLOC,
			ResTotal:       &protos.ResourceInNodeV3{CpuCount: 4, MemoryBytes: 1 << 30},
			ResAlloc:       allocation, Responding: true, HasRegistered: true, Port: 1234,
			CleanupResources: &protos.ResourceInNodeV3{},
		}},
		Partitions: []*protos.PartitionInfo{{
			Name: "batch", Hostlist: "n1", TotalNodes: 1, State: protos.PartitionState_PARTITION_DOWN,
			IsDefault: true, Priority: 5,
		}},
	}
}

func TestCompletingAllocationProjection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		active float64
		drain  bool
		want   string
	}{
		{"only_cleanup", 0, false, "comp|0/4/0/4|1/0/0/1|0\n"},
		{"other_partial_allocation", 1, false, "comp|1/3/0/4|1/0/0/1|0\n"},
		{"other_full_allocation", 4, false, "alloc+|4/0/0/4|1/0/0/1|0\n"},
		{"draining_cleanup", 0, true, "drng|0/0/4/4|1/0/0/1|0\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reply := adapterFixture()
			reply.Nodes[0].CleanupPending = true
			reply.Nodes[0].ResAlloc.CpuCount = 4

			reply.Nodes[0].CleanupResources = &protos.ResourceInNodeV3{CpuCount: 4 - tc.active, MemoryBytes: 1 << 30}
			if tc.drain {
				reply.Nodes[0].ControlState = protos.CranedControlState_CRANE_DRAIN
			}
			o, err := parseCommand([]string{"sinfo", "-h", "-t", "completing", "-O", "StateCompact:0|,CPUsState:0|,NodeAIOT:0|,AllocMem:0"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			s, err := adaptSnapshot(reply, o.Query)
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err = newTextRenderer().table(&out, buildRows(s, o), o); err != nil {
				t.Fatal(err)
			}
			if out.String() != tc.want {
				t.Fatalf("got %q want %q", out.String(), tc.want)
			}
		})
	}
}

func TestMissingAllocationProjectionIsNotIdle(t *testing.T) {
	reply := adapterFixture()
	reply.Nodes[0].CleanupResources = nil
	if _, err := adaptSnapshot(reply, query{}); err == nil || !strings.Contains(err.Error(), "allocation cleanup") {
		t.Fatalf("got %v", err)
	}
}

func TestPowerTransitionsDoNotBecomeNodeFailures(t *testing.T) {
	for _, tc := range []struct {
		power protos.CranedPowerState
		want  string
	}{
		{protos.CranedPowerState_CRANE_POWER_SLEEPING, "idle~"},
		{protos.CranedPowerState_CRANE_POWER_POWEREDOFF, "idle~"},
		{protos.CranedPowerState_CRANE_POWER_TO_SLEEPING, "idle%"},
		{protos.CranedPowerState_CRANE_POWER_POWERING_OFF, "idle%"},
		{protos.CranedPowerState_CRANE_POWER_WAKING_UP, "idle#"},
		{protos.CranedPowerState_CRANE_POWER_POWERING_ON, "idle#"},
	} {
		reply := adapterFixture()
		reply.Nodes[0].Responding = false
		reply.Nodes[0].ResAlloc.CpuCount = 0

		reply.Nodes[0].PowerState = tc.power
		s, err := adaptSnapshot(reply, query{})
		if err != nil {
			t.Fatal(err)
		}
		noRespond := tc.want == "idle#"
		if s.Nodes[0].State.compact() != tc.want || s.Nodes[0].State.NoRespond != noRespond {
			t.Fatalf("%s: %+v", tc.power, s.Nodes[0].State)
		}
	}
	reply := adapterFixture()
	reply.Nodes[0].Responding = false
	s, err := adaptSnapshot(reply, query{})
	if err != nil || s.Nodes[0].State.compact() != "down*" {
		t.Fatalf("unplanned outage: %+v, %v", s, err)
	}
	reply.Nodes[0].HasRegistered = false
	s, err = adaptSnapshot(reply, query{})
	if err != nil || s.Nodes[0].State.compact() != "unk*" {
		t.Fatalf("initial registration: %+v, %v", s, err)
	}
}

func TestGRESOrderAndIndexBuffer(t *testing.T) {
	order := []*protos.GresResource{
		{Name: "fpga", Type: "z", Devices: []string{"/dev/f2", "/dev/f0"}},
		{Name: "fpga", Type: "a", Devices: []string{"/dev/f1"}},
		{Name: "gpu", Type: "a", Devices: []string{"/dev/g10"}},
		{Name: "gpu", Type: "z", Devices: []string{"/dev/g2"}},
	}
	total := &protos.DedicatedResourceInNode{NameTypeMap: map[string]*protos.DeviceTypeSlotsMap{}}
	for _, resource := range order {
		if total.NameTypeMap[resource.Name] == nil {
			total.NameTypeMap[resource.Name] = &protos.DeviceTypeSlotsMap{TypeSlotsMap: map[string]*protos.Slots{}}
		}
		total.NameTypeMap[resource.Name].TypeSlotsMap[resource.Type] = &protos.Slots{Slots: resource.Devices}
	}
	configured, used, err := projectGRES(total, total, order)
	if err != nil || configured != "fpga:z:2,fpga:a:1,gpu:z:1,gpu:a:1" || used != "fpga:z:2(IDX:0-1),fpga:a:1(IDX:2),gpu:z:1(IDX:0),gpu:a:1(IDX:1)" {
		t.Fatalf("configured=%q used=%q err=%v", configured, used, err)
	}
	if _, _, err := projectGRES(total, nil, nil); err == nil {
		t.Fatal("lost configuration order was silently invented")
	}
	indices := make([]int, 32)
	parts := make([]string, len(indices))
	for i := range indices {
		indices[i] = i * 2
		parts[i] = fmt.Sprint(indices[i])
	}
	want := strings.Join(parts, ",")[:63]
	if got := gresIndices(indices); got != want {
		t.Fatalf("index buffer got %q want %q", got, want)
	}
}

func TestAdapterKeepsSchedulerFacts(t *testing.T) {
	o, err := parseCommand([]string{"sinfo", "-h", "-o", "%P|%a|%l|%t|%C|%m|%v"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := adaptSnapshot(adapterFixture(), o.Query)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = newTextRenderer().table(&out, buildRows(s, o), o); err != nil {
		t.Fatal(err)
	}
	// Native CRANE_ALLOC may mean memory exhaustion. Slurm state follows CPU
	// allocations; partition administrative availability does not follow liveness.
	want := "batch*|up|infinite|mix|1/3/0/4|1024|Crane-test\n"
	if out.String() != want {
		t.Fatalf("%q != %q", out.String(), want)
	}
}

func TestNativePartitionMembershipOrderAndTotals(t *testing.T) {
	reply := adapterFixture()
	first := reply.Nodes[0]
	first.Hostname, first.PartitionNames = "n2", []string{"short", "batch"}
	second := adapterFixture().Nodes[0]
	second.ResTotal.CpuCount, second.ResTotal.MemoryBytes = 8, 2<<30
	reply.Nodes = append(reply.Nodes, second)
	reply.Partitions[0].Hostlist, reply.Partitions[0].TotalNodes = "n[1-2]", 2
	reply.Partitions = append(reply.Partitions,
		&protos.PartitionInfo{Name: "short", Hostlist: "n2", TotalNodes: 1},
		&protos.PartitionInfo{Name: "empty"})
	s, err := adaptSnapshot(reply, query{})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []struct {
		name         string
		nodes        []string
		cpus, memory uint64
	}{
		{"batch", []string{"n2", "n1"}, 12, 3072},
		{"short", []string{"n2"}, 4, 1024},
		{"empty", nil, 0, 0},
	} {
		got := s.Partitions[i]
		if got.Name != want.name || !slices.Equal(got.Nodes, want.nodes) || got.TotalCPUs != want.cpus || got.TotalMemory != want.memory || got.ConfiguredNodes != reply.Partitions[i].Hostlist {
			t.Fatalf("partition %s: %+v", want.name, got)
		}
	}
	// Missing membership must not be reconstructed from presentation text.
	reply.Nodes[0].PartitionNames = nil
	if _, err := adaptSnapshot(reply, query{}); err == nil || !strings.Contains(err.Error(), "nodes missing") {
		t.Fatalf("accepted incomplete partition membership: %v", err)
	}
}

func TestFractionalCPUProjection(t *testing.T) {
	for _, cpus := range []float64{.25, 1.0 / 256, 4 - 1.0/256} {
		t.Run(fmt.Sprint(cpus), func(t *testing.T) {
			reply := adapterFixture()
			reply.Nodes[0].ResAlloc.CpuCount = cpus

			o, err := parseCommand([]string{"sinfo", "-h", "-t", "allocated", "-o", "%t|%N"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			s, err := adaptSnapshot(reply, o.Query)
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err = newTextRenderer().table(&out, buildRows(s, o), o); err != nil {
				t.Fatal(err)
			}
			if out.String() != "mix|n1\n" {
				t.Fatal(out.String())
			}
			o.Fields, _ = parseFormat("%C", false)
			out.Reset()
			err = newTextRenderer().table(&out, buildRows(s, o), o)
			if err == nil || !strings.Contains(err.Error(), "fractional") || out.Len() != 0 {
				t.Fatalf("out=%q err=%v", out.String(), err)
			}
		})
	}
}

func TestMissingTopologyIsNotInvented(t *testing.T) {
	o, err := parseCommand([]string{"sinfo", "-O", "SocketCoreThread"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := adaptSnapshot(adapterFixture(), o.Query)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := newTextRenderer().table(&out, buildRows(s, o), o); err == nil || out.Len() != 0 {
		t.Fatalf("out=%q err=%v", out.String(), err)
	}
}

func TestAbsentTelemetryIsNotAConfiguredValue(t *testing.T) {
	for _, field := range []string{"CPUsLoad", "FreeMem"} {
		o, err := parseCommand([]string{"sinfo", "-O", field}, nil)
		if err != nil {
			t.Fatal(err)
		}
		s, err := adaptSnapshot(adapterFixture(), o.Query)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := newTextRenderer().table(&out, buildRows(s, o), o); err == nil || out.Len() != 0 || !strings.Contains(err.Error(), "observation") {
			t.Fatalf("%s out=%q err=%v", field, &out, err)
		}
	}
}

func TestStructuredImplicitCapabilitiesDoNotBecomeZeroValues(t *testing.T) {
	for _, kind := range []string{"json", "yaml"} {
		o, err := parseCommand([]string{"sinfo", "--" + kind, "-o", "%N"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		s, err := adaptSnapshot(adapterFixture(), o.Query)
		if err != nil {
			t.Fatal(err)
		}
		var out, diagnostics bytes.Buffer
		err = renderStructured(&out, &diagnostics, []string{"sinfo"}, buildRows(s, o), s, o, newTextRenderer().UserName)
		if err == nil || !strings.Contains(err.Error(), "field Disk") || out.Len() != 0 {
			t.Fatalf("%s out=%q err=%v", kind, &out, err)
		}
		o.DataParser, o.HasDataParser = "v0.0.45+spec_only", true
		if err := renderStructured(&out, &diagnostics, []string{"sinfo"}, nil, s, o, newTextRenderer().UserName); err != nil || out.Len() == 0 {
			t.Fatalf("schema-only output: %v", err)
		}
	}
}

func TestGRESIndicesSpanTypesAndDoNotDependOnMapOrder(t *testing.T) {
	resources := func(types map[string][]string) *protos.DedicatedResourceInNode {
		m := &protos.DeviceTypeSlotsMap{TypeSlotsMap: map[string]*protos.Slots{}}
		for name, slots := range types {
			m.TypeSlotsMap[name] = &protos.Slots{Slots: slots}
		}
		return &protos.DedicatedResourceInNode{NameTypeMap: map[string]*protos.DeviceTypeSlotsMap{"gpu": m}}
	}
	total := resources(map[string][]string{"a": {"/dev/gpu2", "/dev/gpu0"}, "b": {"/dev/gpu11", "/dev/gpu1", "/dev/gpu10"}})
	used := resources(map[string][]string{"a": {"/dev/gpu2"}, "b": {"/dev/gpu11", "/dev/gpu10", "/dev/gpu1"}})
	order := []*protos.GresResource{
		{Name: "gpu", Type: "a", Devices: []string{"/dev/gpu2", "/dev/gpu0"}},
		{Name: "gpu", Type: "b", Devices: []string{"/dev/gpu11", "/dev/gpu1", "/dev/gpu10"}},
	}
	_, got, err := projectGRES(total, used, order)
	if err != nil || got != "gpu:a:1(IDX:2),gpu:b:3(IDX:1,3-4)" {
		t.Fatalf("got %q err=%v", got, err)
	}
	used.NameTypeMap["gpu"].TypeSlotsMap["a"].Slots = []string{"/dev/missing"}
	if _, _, err = projectGRES(total, used, order); err == nil {
		t.Fatal("unknown allocated device was represented as a valid index")
	}
}

func TestGRESDataErrorOnlyAffectsRequestsForGRES(t *testing.T) {
	reply := adapterFixture()
	reply.Nodes[0].ResAlloc.Gres = &protos.DedicatedResourceInNode{NameTypeMap: map[string]*protos.DeviceTypeSlotsMap{
		"gpu": {TypeSlotsMap: map[string]*protos.Slots{"a": {Slots: []string{"/dev/unknown"}}}},
	}}
	o, err := parseCommand([]string{"sinfo", "-h", "-o", "%N"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := adaptSnapshot(reply, o.Query)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = newTextRenderer().table(&out, buildRows(s, o), o); err != nil || out.String() != "n1\n" {
		t.Fatalf("out=%q err=%v", &out, err)
	}
	o.Fields, _ = parseFormat("GresUsed", true)
	out.Reset()
	if err = newTextRenderer().table(&out, buildRows(s, o), o); err == nil || out.Len() != 0 {
		t.Fatalf("out=%q err=%v", &out, err)
	}
}

type fixtureController struct {
	protos.UnimplementedCraneCtldServer
	request chan *protos.QueryClusterInfoRequest
}

func (s *fixtureController) QueryClusterInfo(_ context.Context, req *protos.QueryClusterInfoRequest) (*protos.QueryClusterInfoReply, error) {
	s.request <- req
	return adapterFixture(), nil
}

func TestCraneSourceUsesCallerAndSharedQuery(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	fixture := &fixtureController{request: make(chan *protos.QueryClusterInfoRequest, 1)}
	protos.RegisterCraneCtldServer(server, fixture)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte(fmt.Sprintf("ControlMachine: %s\nCraneCtldListenPort: '%s'\n", host, port)), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	source, err := openCraneSource(ctx, configPath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	s, err := source.Load(ctx, query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Nodes) != 1 || s.Nodes[0].Name != "n1" {
		t.Fatalf("%+v", s)
	}
	select {
	case req := <-fixture.request:
		if req.Uid == nil || req.GetUid() != uint32(os.Getuid()) || !req.IncludeNodes || !req.IncludePartitions || !req.IncludeReservations || !req.IncludeAllocationCleanup {
			t.Fatalf("%+v", req)
		}
	default:
		t.Fatal("request not observed")
	}
	if _, err := source.Load(ctx, query{ReservationsOnly: true}); err != nil {
		t.Fatal(err)
	}
	req := <-fixture.request
	if req.IncludeNodes || req.IncludePartitions || req.IncludeAllocationCleanup || !req.IncludeReservations {
		t.Fatalf("reservation-only request: %+v", req)
	}
}

func TestNativeOutagePresentation(t *testing.T) {
	for _, tc := range []struct {
		name, reason, want string
		responding, outage bool
	}{
		{"admin", "maintenance", "maintenance", true, false},
		{"down", "maintenance", "maintenance : Not responding", false, true},
		{"reconnected_drained", "maintenance", "maintenance : Not responding", true, true},
		{"no_admin_reason", "", "Not responding", false, true},
		{"already_suffixed", "maintenance : Not responding", "maintenance : Not responding", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reply := adapterFixture()
			raw := reply.Nodes[0]
			uid := uint32(1000)
			raw.StateReason, raw.StateReasonUid = tc.reason, &uid
			raw.StateReasonTime = timestamppb.New(time.Unix(10, 0))
			raw.Responding = tc.responding
			if tc.outage {
				raw.Outage = &protos.NodeOutage{Uid: 0, Time: timestamppb.New(time.Unix(20, 0))}
			}
			s, err := adaptSnapshot(reply, query{})
			if err != nil {
				t.Fatal(err)
			}
			n := s.Nodes[0]
			wantUID, wantTime := uint32(1000), int64(10)
			if tc.outage {
				wantUID, wantTime = 0, 20
			}
			if n.Reason != tc.want || n.ReasonUID == nil || *n.ReasonUID != wantUID || n.ReasonTime.Unix() != wantTime {
				t.Fatalf("got %q, %v, %v", n.Reason, n.ReasonUID, n.ReasonTime)
			}
			if raw.StateReason != tc.reason || *raw.StateReasonUid != 1000 {
				t.Fatal("mutated native reason")
			}
		})
	}
}

func TestNativeTopologyAndTelemetryPresentation(t *testing.T) {
	for _, sockets := range []uint32{0, 2, 3} {
		reply := adapterFixture()
		raw := reply.Nodes[0]
		raw.ConfiguredSockets = sockets
		raw.NodeTopoInfo = &protos.NodeTopoInfo{Sockets: 8, CoresPerSocket: 64, ThreadsPerCore: 2}
		load := uint32(731)
		raw.CpuLoad, raw.Responding = &load, false
		s, err := adaptSnapshot(reply, query{})
		if err != nil {
			t.Fatal(err)
		}
		n := s.Nodes[0]
		if sockets == 2 {
			if n.Sockets != 2 || n.Cores != 2 || n.Threads != 1 {
				t.Fatalf("topology: %+v", n)
			}
		} else if n.Sockets != 0 {
			t.Fatal("invented topology")
		}
		if n.CPULoad == nil || *n.CPULoad != 0 || *raw.CpuLoad != 731 {
			t.Fatal("lost telemetry sample or down-node projection")
		}
	}
}

func TestReservationProjectionUsesAllocationOrderAndControllerTime(t *testing.T) {
	for _, seconds := range []int64{99, 100, 109, 110} {
		reply := adapterFixture()
		reply.ObservedAt = timestamppb.New(time.Unix(seconds, 0))
		makeReservation := func(name string, order uint64, nodes ...string) *protos.ReservationInfo {
			return &protos.ReservationInfo{ReservationName: name, CreationOrder: order,
				StartTime: timestamppb.New(time.Unix(100, 0)), Duration: durationpb.New(10 * time.Second),
				CranedRegex: "candidate[1-99]", AllocatedNodes: nodes}
		}
		reply.Reservations = []*protos.ReservationInfo{makeReservation("first_by_name", 2, "n4", "n3"), makeReservation("first_created", 1, "n1")}
		s, err := adaptSnapshot(reply, query{})
		if err != nil {
			t.Fatal(err)
		}
		if s.Reservations[0].Name != "first_created" || s.Reservations[0].Nodes != "n1" || s.Reservations[1].Nodes != "n[3-4]" {
			t.Fatalf("reservations: %+v", s.Reservations)
		}
		if s.Nodes[0].State.Reserved != (seconds >= 100 && seconds < 110) {
			t.Fatalf("interval at %d: %+v", seconds, s.Nodes[0])
		}
		if reply.Reservations[0].CreationOrder != 2 {
			t.Fatal("sorted input in place")
		}
		only, err := adaptSnapshot(reply, query{ReservationsOnly: true})
		if err != nil || len(only.Nodes) != 0 || len(only.Reservations) != 2 {
			t.Fatalf("reservation-only: %+v %v", only, err)
		}
	}
}

func TestCleanupSubtractionPreservesPrecisionAndGRES(t *testing.T) {
	reply := adapterFixture()
	raw := reply.Nodes[0]
	raw.ResAlloc.CpuCount = 66.0 / 256 // Exact native scheduler precision.
	raw.CleanupResources.CpuCount = 1.0 / 256
	resources := func(slots ...string) *protos.DedicatedResourceInNode {
		return &protos.DedicatedResourceInNode{NameTypeMap: map[string]*protos.DeviceTypeSlotsMap{
			"gpu": {TypeSlotsMap: map[string]*protos.Slots{"a": {Slots: slots}}},
		}}
	}
	raw.ResTotal.Gres = resources("/dev/g0", "/dev/g1")
	raw.ResAlloc.Gres = resources("/dev/g0", "/dev/g1")
	raw.CleanupResources.Gres = resources("/dev/g0")
	raw.GresOrder = []*protos.GresResource{{Name: "gpu", Type: "a", Devices: []string{"/dev/g0", "/dev/g1"}}}
	active, err := activeAllocation(raw)
	if err != nil || active.CpuCount != 65.0/256 {
		t.Fatalf("active=%v err=%v", active, err)
	}
	s, err := adaptSnapshot(reply, query{})
	if err != nil || s.Nodes[0].GresUsed != "gpu:a:1(IDX:1)" {
		t.Fatalf("snapshot=%+v err=%v", s, err)
	}
	if len(raw.ResAlloc.Gres.NameTypeMap["gpu"].TypeSlotsMap["a"].Slots) != 2 {
		t.Fatal("mutated scheduler holdings")
	}
	raw.CleanupResources.Gres = resources("/dev/missing")
	s, err = adaptSnapshot(reply, query{})
	if err != nil || s.Nodes[0].GresIssue == nil {
		t.Fatalf("GRES error must remain field-specific: %+v %v", s, err)
	}
	raw.CleanupResources.CpuCount = 1
	if _, err = adaptSnapshot(reply, query{}); err == nil {
		t.Fatal("accepted cleanup exceeding holdings")
	}
	raw.CleanupResources.CpuCount = 0
	raw.CleanupResources.MemoryBytes = 2 << 30
	if _, err = adaptSnapshot(reply, query{}); err == nil {
		t.Fatal("accepted memory underflow")
	}
}

func TestSnapshotRejectsIncompleteFacts(t *testing.T) {
	if _, err := adaptSnapshot(nil, query{}); err == nil {
		t.Fatal("accepted missing reply")
	}
	for _, change := range []func(*protos.QueryClusterInfoReply){
		func(r *protos.QueryClusterInfoReply) { r.ObservedAt = nil },
		func(r *protos.QueryClusterInfoReply) { r.Nodes[0].ResAlloc = nil },
		func(r *protos.QueryClusterInfoReply) { r.Nodes[0].Outage = &protos.NodeOutage{} },
		func(r *protos.QueryClusterInfoReply) { r.Reservations = []*protos.ReservationInfo{nil} },
		func(r *protos.QueryClusterInfoReply) { r.Partitions = []*protos.PartitionInfo{nil} },
	} {
		r := adapterFixture()
		change(r)
		if _, err := adaptSnapshot(r, query{}); err == nil {
			t.Fatal("accepted incomplete facts")
		}
	}
}

func TestReservationDurationValidation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		duration *durationpb.Duration
		wantErr  string
	}{
		{"positive", durationpb.New(time.Hour), ""},
		{"zero", durationpb.New(0), ""},
		{"negative_seconds", durationpb.New(-time.Second), "negative duration"},
		{"negative_nanos", durationpb.New(-time.Nanosecond), "negative duration"},
		{"invalid", &durationpb.Duration{Seconds: 1, Nanos: -1}, "time interval"},
	} {
		for _, only := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reservations_only=%t", tc.name, only), func(t *testing.T) {
				reply := adapterFixture()
				start := reply.ObservedAt.AsTime()
				reply.Reservations = []*protos.ReservationInfo{{
					ReservationName: "maintenance", StartTime: timestamppb.New(start),
					Duration: tc.duration, CreationOrder: 1,
				}}
				s, err := adaptSnapshot(reply, query{ReservationsOnly: only})
				if tc.wantErr != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), "maintenance") {
						t.Fatalf("got %v, want reservation error containing %q", err, tc.wantErr)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if len(s.Reservations) != 1 || !s.Reservations[0].End.Equal(start.Add(tc.duration.AsDuration())) {
					t.Fatalf("unexpected reservation interval: %+v", s.Reservations)
				}
			})
		}
	}
}

func TestStateReasonTimestampValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		stamp   *timestamppb.Timestamp
		wantErr bool
	}{
		{"missing", nil, false},
		{"valid", timestamppb.New(time.Unix(100, 123)), false},
		{"epoch", timestamppb.New(time.Unix(0, 0)), false},
		{"out_of_range", &timestamppb.Timestamp{Seconds: 253402300800}, true},
		{"negative_nanos", &timestamppb.Timestamp{Nanos: -1}, true},
		{"excess_nanos", &timestamppb.Timestamp{Nanos: 1000000000}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reply := adapterFixture()
			reply.Nodes[0].StateReasonTime = tc.stamp
			s, err := adaptSnapshot(reply, query{})
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "n1") || !strings.Contains(err.Error(), "state reason time") {
					t.Fatalf("got %v, want invalid state reason time for n1", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var want time.Time
			if tc.stamp != nil {
				want = tc.stamp.AsTime()
			}
			if got := s.Nodes[0].ReasonTime; !got.Equal(want) {
				t.Fatalf("got reason time %v, want %v", got, want)
			}
		})
	}
}
