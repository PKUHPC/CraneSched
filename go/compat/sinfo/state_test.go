// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"slices"
	"testing"
)

func TestStateProjections(t *testing.T) {
	for _, tc := range []struct {
		state                   nodeState
		long, compact, complete string
	}{
		{nodeState{Base: "idle"}, "idle", "idle", "IDLE"},
		{nodeState{Base: "idle", NoRespond: true, Power: "powered_down"}, "idle~", "idle~", "IDLE+POWERED_DOWN+NOT_RESPONDING"},
		{nodeState{Base: "mixed", Drain: true, NoRespond: true, Maintenance: true}, "draining$", "drng$", "MIXED+DRAIN+MAINTENANCE+NOT_RESPONDING"},
		{nodeState{Base: "idle", Maintenance: true}, "maint", "maint", "IDLE+MAINTENANCE"},
		{nodeState{Base: "allocated", Completing: true}, "allocated+", "alloc+", "ALLOCATED+COMPLETING"},
		{nodeState{Base: "idle", Reserved: true}, "reserved", "resv", "IDLE+RESERVED"},
		{nodeState{Base: "mixed", Planned: true}, "mixed-", "mix-", "MIXED+PLANNED"},
	} {
		got := []string{tc.state.long(), tc.state.compact(), tc.state.complete()}
		if want := []string{tc.long, tc.compact, tc.complete}; !slices.Equal(got, want) {
			t.Errorf("state %+v: got %q, want %q", tc.state, got, want)
		}
	}
	for _, value := range []string{"err", "(idle)", " idle", "!!down"} {
		if _, err := parseStates(value, false); err == nil {
			t.Errorf("accepted %q", value)
		}
	}
}
