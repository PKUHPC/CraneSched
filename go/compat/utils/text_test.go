// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package utils

import (
	"io"
	"strings"
	"testing"
	"time"
)

func TestTimeFormatter(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 34, 56, 0, time.UTC)
	cases := []struct {
		format string
		t      time.Time
		want   string
	}{
		{"standard", now, "2026-06-04T12:34:56"},
		{"%a %e %b %Y %T %z", now, "Thu  4 Jun 2026 12:34:56 +0000"},
		{"%5d|%_5d|%05d", now, "00004|    4|00004"},
		{"%Ea|%Ob|%_6e", now, "%Ea|Jun|     4"},
		{"%#c|%^P", now, "Thu Jun  4 12:34:56 2026|pm"},
		{"%3z|%-z", now, "  +0000|+0"},
		{"relative", now.Add(-24 * time.Hour), "Ystday 12:34"},
		{"relative", now, "12:34:56"},
		{"relative", now.Add(24 * time.Hour), "Tomorr 12:34"},
		{"relative", now.Add(2 * 24 * time.Hour), "Sat 12:34"},
		{"relative", now.Add(8 * 24 * time.Hour), "12 Jun 12:34"},
		{"%F", time.Unix(0, 0), "Unknown"},
		{"%F", time.Unix(0xfffffffe, 0), "None"},
		{"%F", time.Unix(0xffffffff, 0), "Unknown"},
		{"%300Y", now, strings.Repeat("#", 255)},
	}
	for _, tc := range cases {
		if got := TimeFormatter("sinfo", tc.format, now, io.Discard)(tc.t); got != tc.want {
			t.Errorf("%q: %q != %q", tc.format, got, tc.want)
		}
	}
}

func TestCommandTimezone(t *testing.T) {
	for _, tc := range []struct{ zone, instant, want string }{
		{"", "2026-06-04T12:34:56Z", "2026-06-04T12:34:56 +0000 UTC"},
		{"UTC-8", "2026-06-04T12:34:56Z", "2026-06-04T20:34:56 +0800 UTC"},
		{"CST-8", "2026-06-04T12:34:56Z", "2026-06-04T20:34:56 +0800 CST"},
		{"<+0530>-5:30", "2026-06-04T12:34:56Z", "2026-06-04T18:04:56 +0530 +0530"},
		{"Asia/Shanghai", "2026-06-04T12:34:56Z", "2026-06-04T20:34:56 +0800 CST"},
		{":/usr/share/zoneinfo/Asia/Shanghai", "2026-06-04T12:34:56Z", "2026-06-04T20:34:56 +0800 CST"},
		{"EST5EDT,M3.2.0,M11.1.0", "2026-03-08T06:59:59Z", "2026-03-08T01:59:59 -0500 EST"},
		{"EST5EDT,M3.2.0,M11.1.0", "2026-03-08T07:00:00Z", "2026-03-08T03:00:00 -0400 EDT"},
		{"EST5EDT,M3.2.0,M11.1.0", "2026-11-01T05:59:59Z", "2026-11-01T01:59:59 -0400 EDT"},
		{"EST5EDT,M3.2.0,M11.1.0", "2026-11-01T06:00:00Z", "2026-11-01T01:00:00 -0500 EST"},
		{"AEST-10AEDT,M10.1.0,M4.1.0/3", "2026-01-01T00:00:00Z", "2026-01-01T11:00:00 +1100 AEDT"},
		{"AEST-10AEDT,M10.1.0,M4.1.0/3", "2026-07-01T00:00:00Z", "2026-07-01T10:00:00 +1000 AEST"},
	} {
		t.Run(tc.zone+tc.instant, func(t *testing.T) {
			instant, err := time.Parse(time.RFC3339, tc.instant)
			if err != nil {
				t.Fatal(err)
			}
			got := instant.In(CommandTimezone(map[string]string{"TZ": tc.zone})).Format("2006-01-02T15:04:05 -0700 MST")
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
