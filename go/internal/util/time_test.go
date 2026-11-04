package util

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestParseCraneTimestamp(t *testing.T) {
	valid := timestamppb.New(time.Unix(1_700_000_000, 0))
	if got, ok := ParseCraneTimestamp(valid); !ok || got.Unix() != valid.Seconds {
		t.Fatalf("ParseCraneTimestamp() = (%v, %v)", got, ok)
	}

	for _, timestamp := range []*timestamppb.Timestamp{
		nil,
		{},
		{Seconds: MaxJobTimeStamp},
		{Seconds: valid.Seconds, Nanos: int32(time.Second)},
	} {
		if _, ok := ParseCraneTimestamp(timestamp); ok {
			t.Fatalf("ParseCraneTimestamp(%v) unexpectedly succeeded", timestamp)
		}
	}
}

func TestValidDurationSeconds(t *testing.T) {
	for _, test := range []struct {
		duration *durationpb.Duration
		want     int64
		ok       bool
	}{
		{duration: durationpb.New(0), want: 0, ok: true},
		{duration: durationpb.New(5 * time.Second), want: 5, ok: true},
		{duration: durationpb.New(-time.Second), ok: false},
		{duration: nil, ok: false},
	} {
		got, ok := ValidDurationSeconds(test.duration)
		if got != test.want || ok != test.ok {
			t.Errorf("ValidDurationSeconds(%v) = (%d, %v), want (%d, %v)",
				test.duration, got, ok, test.want, test.ok)
		}
	}
}
