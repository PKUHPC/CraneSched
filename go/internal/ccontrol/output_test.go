package ccontrol

import (
	"strings"
	"testing"
	"time"

	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
}
