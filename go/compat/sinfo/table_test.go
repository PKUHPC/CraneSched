// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func renderTest(t *testing.T, s *snapshot, args ...string) (string, string) {
	t.Helper()
	o, err := parseCommand(append([]string{"sinfo"}, args...), nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := buildRows(s, o)
	sortRows(rows, o)
	var out bytes.Buffer
	if err := newTextRenderer().table(&out, rows, o); err != nil {
		t.Fatal(err)
	}
	return out.String(), strings.Join(o.Diagnostics, "")
}

func TestReservationRendering(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	r := newTextRenderer()
	var out bytes.Buffer
	if err := r.reservations(&out, nil, true, now); err != nil {
		t.Fatal(err)
	}
	if out.String() != "No reservations in the system\n" {
		t.Fatal(out.String())
	}
	out.Reset()
	if err := r.reservations(&out, []reservation{{Name: "maint", Nodes: "n[1-2]", Start: now, End: now.Add(time.Minute)}}, true, now); err != nil {
		t.Fatal(err)
	}
	want := "maint        ACTIVE  2026-01-02T03:04:05  2026-01-02T03:05:05     00:01:00  n[1-2]\n"
	if out.String() != want {
		t.Fatalf("%q != %q", out.String(), want)
	}
}

func TestNumericRendering(t *testing.T) {
	for _, tc := range []struct {
		n    uint64
		text string
	}{
		{0, "0"}, {1023, "1023"}, {1024, "1K"}, {1536, "1.50K"}, {2048, "2K"}, {2560, "2.50K"}, {1024 * 1024, "1M"},
	} {
		if got := unitNumber(tc.n, false); got != tc.text {
			t.Errorf("%d: %q != %q", tc.n, got, tc.text)
		}
	}
	if got := unitNumber(1536, true); got != "1536" {
		t.Fatal(got)
	}
}

func TestReservationTextEndSecondRemainsActive(t *testing.T) {
	start := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	res := []reservation{{Name: "maint", Start: start, End: start.Add(time.Minute)}}
	for _, offset := range []time.Duration{0, 500 * time.Millisecond, time.Second} {
		var out bytes.Buffer
		if err := newTextRenderer().reservations(&out, res, true, res[0].End.Add(offset)); err != nil {
			t.Fatal(err)
		}
		inactive := strings.Contains(out.String(), "INACTIVE")
		if inactive != (offset >= time.Second) {
			t.Fatalf("offset=%s out=%q", offset, &out)
		}
	}
}

func TestPartitionAndReservationDurations(t *testing.T) {
	if got := secondsString(60); got != "1:00" {
		t.Errorf("partition duration = %q, want 1:00", got)
	}
	if got := reservationDuration(60); got != "00:01:00" {
		t.Errorf("reservation duration = %q, want 00:01:00", got)
	}
}
