package ccontrol

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type reservationFlexClient struct {
	protos.CraneCtldClient
	requests []*protos.CreateReservationRequest
}

func (c *reservationFlexClient) CreateReservation(_ context.Context, request *protos.CreateReservationRequest, _ ...grpc.CallOption) (*protos.CreateReservationReply, error) {
	c.requests = append(c.requests, request)
	return &protos.CreateReservationReply{Ok: true}, nil
}

func TestReservationFlexCreateOptions(t *testing.T) {
	oldStub, oldJSON := stub, FlagJson
	oldFlex, oldName := FlagReservationFlex, FlagReservationName
	oldStart, oldDuration := FlagStartTime, FlagDuration
	oldNodes, oldPartition := FlagNodes, FlagPartitionName
	oldAccount, oldUser, oldCount := FlagAccount, FlagUser, FlagNodeNum
	t.Cleanup(func() {
		stub, FlagJson = oldStub, oldJSON
		FlagReservationFlex, FlagReservationName = oldFlex, oldName
		FlagStartTime, FlagDuration = oldStart, oldDuration
		FlagNodes, FlagPartitionName = oldNodes, oldPartition
		FlagAccount, FlagUser, FlagNodeNum = oldAccount, oldUser, oldCount
	})
	client := &reservationFlexClient{}
	stub, FlagJson = client, false
	FlagNodes, FlagPartitionName, FlagUser, FlagNodeNum = "", "", "", 0
	for _, tc := range []struct {
		option string
		want   bool
	}{
		{"--flex", true}, {"", false}, {"flex=true", true},
		{"--flex=false", false}, {"flex=false", false},
	} {
		args := []string{"create", "reservation", "class", "startTime=2030-01-01T10:00:00", "duration=02:00:00", "account=students", "nodes=n1"}
		if tc.option != "" {
			args = append(args, tc.option)
		}
		command, err := CControlParser.ParseString("", getCmdStringByArgs(preParseGlobalFlags(args)))
		if err != nil {
			t.Fatal(err)
		}
		_, err = testutil.CaptureStdout(t, func() error { return executeCreateReservationCommand(command) })
		if err != nil {
			t.Fatalf("option=%q: %v", tc.option, err)
		}
		request := client.requests[len(client.requests)-1]
		if request.Flex != tc.want || request.ReservationName != "class" || request.DurationSeconds != 7200 {
			t.Fatalf("option=%q request=%v", tc.option, request)
		}
	}
	command, err := CControlParser.ParseString("", "create reservation bad startTime=2030-01-01T10:00:00 duration=02:00:00 account=students nodes=n1 flex=invalid")
	if err != nil {
		t.Fatal(err)
	}
	before := len(client.requests)
	if err := executeCreateReservationCommand(command); err == nil || len(client.requests) != before {
		t.Fatalf("invalid option reached server: err=%v", err)
	}
}

func TestReservationFlexQueryFormats(t *testing.T) {
	reservation := &protos.ReservationInfo{
		ReservationName: "class", Flex: true,
		StartTime: timestamppb.New(time.Unix(100, 0)), Duration: durationpb.New(time.Hour),
		ResTotal: &protos.ResourceView{}, ResAvail: &protos.ResourceView{}, ResAlloc: &protos.ResourceView{},
	}
	text := formatReservationDetails(reservation)
	if !strings.Contains(text, "Flex=true") || !strings.Contains(text, "State=EXPIRED") {
		t.Fatalf("unexpected reservation text: %s", text)
	}
	native, err := protojson.Marshal(reservation)
	if err != nil || !strings.Contains(string(native), `"flex":true`) {
		t.Fatalf("native JSON=%s err=%v", native, err)
	}
	compat, err := formatSlurmReservationsJSON(&protos.QueryReservationInfoReply{Ok: true, ReservationInfoList: []*protos.ReservationInfo{reservation}})
	if err != nil || !strings.Contains(compat, `"flex":true`) {
		t.Fatalf("compat JSON=%s err=%v", compat, err)
	}
}
