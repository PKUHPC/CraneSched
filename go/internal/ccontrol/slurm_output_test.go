package ccontrol

import (
	"strings"
	"testing"
	"time"

	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewSlurmNodeInfoKeepsNodeIdentitiesDistinct(t *testing.T) {
	node := &protos.CranedInfo{
		Hostname:     "crnd1",
		NodeHostname: "host01.example.com",
		NodeAddr:     "10.0.0.1",
	}

	got, err := newSlurmNodeInfo(node)
	if err != nil {
		t.Fatalf("newSlurmNodeInfo returned error: %v", err)
	}
	if got.Hostname != node.Hostname {
		t.Errorf("Hostname = %q, want canonical NodeName %q", got.Hostname, node.Hostname)
	}
	if got.NodeHostname != node.NodeHostname {
		t.Errorf("NodeHostname = %q, want %q", got.NodeHostname, node.NodeHostname)
	}
	if got.NodeAddr != node.NodeAddr {
		t.Errorf("NodeAddr = %q, want %q", got.NodeAddr, node.NodeAddr)
	}
}

func TestNativeCPUTextRoundsExactQueryValues(t *testing.T) {
	node := &protos.CranedInfo{
		ResTotal: &protos.ResourceInNodeV3{CpuCount: 4},
		ResAlloc: &protos.ResourceInNodeV3{CpuCount: 32.0 / 256},
		ResAvail: &protos.ResourceInNodeV3{CpuCount: 992.0 / 256},
	}
	if got := formatCpuInfo(node); got != "CPU=4.00 AllocCPU=0.13 FreeCPU=3.88" {
		t.Fatal(got)
	}
	partition := &protos.PartitionInfo{
		ResTotal: &protos.ResourceView{CpuCount: 4.125},
		ResAlloc: &protos.ResourceView{CpuCount: 0.125},
		ResAvail: &protos.ResourceView{CpuCount: 4},
	}
	if got := formatCpuResources(partition); got != "TotalCPU=4.13 AvailCPU=4.00 AllocCPU=0.13" {
		t.Fatal(got)
	}
}

func TestReservationFlexSlurmQueryFormat(t *testing.T) {
	reservation := &protos.ReservationInfo{
		ReservationName: "class", Flex: true,
		StartTime: timestamppb.New(time.Unix(100, 0)), Duration: durationpb.New(time.Hour),
		ResTotal: &protos.ResourceView{}, ResAvail: &protos.ResourceView{}, ResAlloc: &protos.ResourceView{},
	}
	compat, err := formatSlurmReservationsJSON(&protos.QueryReservationInfoReply{Ok: true, ReservationInfoList: []*protos.ReservationInfo{reservation}})
	if err != nil || !strings.Contains(compat, `"flex":true`) {
		t.Fatalf("compat JSON=%s err=%v", compat, err)
	}
}
