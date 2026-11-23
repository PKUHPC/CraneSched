package util

import (
	"strings"
	"syscall"
	"testing"

	"github.com/PKUHPC/CraneSched/go/generated/protos"
)

func TestParseSignalParamString(t *testing.T) {
	signals, err := ParseSignalParamString(" sigusr1 , B:17@0, R:34@65535, 64 ")
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		number  int32
		seconds uint32
		flag    protos.Signal_SignalFlag
	}{
		{int32(syscall.SIGUSR1), 60, protos.Signal_NONE},
		{17, 0, protos.Signal_BATCH_ONLY},
		{34, 65535, protos.Signal_RESERVATION_OVERLAP},
		{64, 60, protos.Signal_NONE},
	}
	if len(signals) != len(want) {
		t.Fatalf("got %d signals, want %d", len(signals), len(want))
	}
	for i, signal := range signals {
		if signal.SignalNumber != want[i].number || signal.SignalTime != want[i].seconds ||
			signal.SignalFlag != want[i].flag {
			t.Errorf("signal %d = %v, want %+v", i, signal, want[i])
		}
	}
}

func TestParseSignalParamStringRejectsInvalidInput(t *testing.T) {
	for _, tt := range []struct {
		input string
		why   string
	}{
		{"", "empty signal"},
		{"  ", "empty signal"},
		{",USR1", "empty signal"},
		{"USR1, ,TERM", "empty signal"},
		{"USR1,", "empty signal"},
		{"0", "signal number"},
		{"32", "signal number"},
		{"33", "signal number"},
		{"65", "signal number"},
		{"999@3", "signal number"},
		{"4294967306", "signal name or number"}, // Would narrow to 10.
		{"18446744073709551616", "signal name or number"},
		{"BOGUS", "signal name or number"},
		{"USR1@65536", "signal time"},
		{"USR1@4294967296", "signal time"}, // Would narrow to 0.
		{"USR1@18446744073709551616", "signal time"},
		{"USR1@", "signal format"},
		{"USR1@-1", "signal format"},
		{"USR1@1.5", "signal format"},
		{"USR1@1@2", "signal format"},
		{"-10@2", "signal format"},
		{"X:USR1@1", "signal format"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			signals, err := ParseSignalParamString(tt.input)
			if err == nil || !strings.Contains(err.Error(), tt.why) || signals != nil {
				t.Errorf("for %q got (%v, %v), want no signals and an error containing %q", tt.input, signals, err, tt.why)
			}
		})
	}
}
