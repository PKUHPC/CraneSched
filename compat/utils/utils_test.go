// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package utils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOptionScanner(t *testing.T) {
	specs := []OptionSpec{
		{"help", 'h', NoArgument}, {"header", 0, NoArgument},
		{"output", 'o', RequiredArgument}, {"optional", 'j', OptionalArgument},
		{"cluster", 'M', RequiredArgument}, {"clusters", 'M', RequiredArgument},
	}
	for _, tc := range []struct {
		name  string
		args  []string
		posix bool
		want  []Option
		err   string
	}{
		{"repeated", []string{"-oone", "--output", "two"}, false, []Option{{"output", "one", true}, {"output", "two", false}}, ""},
		{"short_cluster", []string{"-hoabc"}, false, []Option{{"help", "", false}, {"output", "abc", true}}, ""},
		{"optional_empty", []string{"--optional="}, false, []Option{{"optional", "", true}}, ""},
		{"optional_separate", []string{"--optional", "value", "-h"}, false, []Option{{"optional", "", false}, {"help", "", false}}, ""},
		{"optional_short", []string{"-jvalue", "-j", "operand"}, false, []Option{{"optional", "value", true}, {"optional", "", false}}, ""},
		{"terminator", []string{"--", "--invalid"}, false, nil, ""},
		{"posix", []string{"operand", "--invalid"}, true, nil, ""},
		{"permutation", []string{"operand", "-h"}, false, []Option{{"help", "", false}}, ""},
		{"same_alias", []string{"--cl=x"}, false, []Option{{"cluster", "x", true}}, ""},
		{"ambiguous", []string{"--he"}, false, nil, "option '--he' is ambiguous; possibilities: '--help' '--header'"},
		{"unknown", []string{"--unknown=x"}, false, nil, "unrecognized option '--unknown=x'"},
		{"missing_long", []string{"--output"}, false, nil, "option '--output' requires an argument"},
		{"missing_short", []string{"-o"}, false, nil, "option requires an argument -- 'o'"},
		{"unexpected_value", []string{"--help=x"}, false, nil, "option '--help' doesn't allow an argument"},
		{"unknown_short", []string{"-x"}, false, nil, "invalid option -- 'x'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scanner := NewOptionScanner(tc.args, specs, tc.posix)
			var got []Option
			var diagnostic string
			for {
				option, err := scanner.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					diagnostic = err.Error()
					break
				}
				got = append(got, option)
			}
			if !reflect.DeepEqual(got, tc.want) || diagnostic != tc.err {
				t.Fatalf("options=%#v error=%q; want %#v %q", got, diagnostic, tc.want, tc.err)
			}
		})
	}
	// Stop after help without inspecting a later bad flag, even in one cluster.
	for _, args := range [][]string{{"--help", "--bad"}, {"-hx"}} {
		scanner := NewOptionScanner(args, specs, false)
		if option, err := scanner.Next(); err != nil || option.Name != "help" {
			t.Fatalf("%v: %#v %v", args, option, err)
		}
		if _, err := scanner.Next(); err == nil || err == io.EOF {
			t.Fatalf("missing deferred error: %v", args)
		}
	}
}

func TestReadQueryRetryPolicy(t *testing.T) {
	unauthenticated := status.Error(codes.Unauthenticated, "expired")
	certificate := status.Error(codes.Unavailable, "certificate expired")
	unavailable := status.Error(codes.Unavailable, "connection refused")
	refreshFailure := errors.New("certificate refresh failed")
	for _, tc := range []struct {
		name                   string
		tls                    bool
		first, refresh, second error
		calls, refreshes       int
		want                   error
	}{
		{"success", true, nil, nil, nil, 1, 0, nil},
		{"no_tls", false, unauthenticated, nil, nil, 1, 0, unauthenticated},
		{"transport", true, unavailable, nil, nil, 1, 0, unavailable},
		{"renew_identity", true, unauthenticated, nil, nil, 2, 1, nil},
		{"renew_transport", true, certificate, nil, nil, 2, 1, nil},
		{"refresh_failure", true, certificate, refreshFailure, nil, 1, 1, refreshFailure},
		{"only_once", true, certificate, nil, certificate, 2, 1, certificate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, refreshes := 0, 0
			err := retryQuery(context.Background(), tc.tls, func() error {
				calls++
				if calls == 1 {
					return tc.first
				}
				return tc.second
			}, func(context.Context) error { refreshes++; return tc.refresh })
			if !errors.Is(err, tc.want) || calls != tc.calls || refreshes != tc.refreshes {
				t.Fatalf("err=%v calls=%d refreshes=%d", err, calls, refreshes)
			}
		})
	}
	for _, phase := range []string{"before_query", "after_query", "after_refresh"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls, refreshes := 0, 0
			if phase == "before_query" {
				cancel()
			}
			err := retryQuery(ctx, true, func() error {
				calls++
				if phase == "after_query" {
					cancel()
				}
				return certificate
			}, func(context.Context) error { refreshes++; cancel(); return nil })
			wantCalls, wantRefreshes := 1, 0
			if phase == "before_query" {
				wantCalls = 0
			}
			if phase == "after_refresh" {
				wantRefreshes = 1
			}
			if !errors.Is(err, context.Canceled) || calls != wantCalls || refreshes != wantRefreshes {
				t.Fatalf("err=%v calls=%d refreshes=%d", err, calls, refreshes)
			}
		})
	}
}

func TestHostLists(t *testing.T) {
	for _, tc := range []struct {
		input   string
		yes, no []string
	}{
		{"n[4294967296]", []string{"n4294967296"}, []string{"n0"}},
		{"n[1-]", []string{"n1"}, []string{"n01"}},
		{"n[+1]", []string{"n01"}, []string{"n1"}},
		{"n[ 1]", []string{"n01"}, []string{"n1"}},
		{"n[\t01]", []string{"n001"}, []string{"n01"}},
		{"n[1-+2]", []string{"n1", "n2"}, []string{"n02"}},
		{"n[1- 2]", []string{"n1", "n2"}, []string{"n02"}},
		{"n[1a-2]", []string{"n01", "n02"}, []string{"n1"}},
		{"n[1-65536]", []string{"n1", "n65536"}, []string{"n65537"}},
		{"n[01-03]", []string{"n01", "n03"}, []string{"n1", "n04"}},
		{"n[1-03]", []string{"n1", "n3"}, []string{"n01", "n03"}},
		{"n[001-3]", []string{"n001", "n003"}, []string{"n1", "n004"}},
		{"n[09-10]", []string{"n09", "n10"}, []string{"n9", "n010"}},
		{"r[1-2]n[03,05-07]", []string{"r1n03", "r2n06"}, []string{"r3n03", "r1n04"}},
		{"n[1,11][1,11]z", []string{"n11z", "n111z", "n1111z"}, []string{"n1z", "n11111z", "n111y"}},
		{"n[01,11][1,11]z", []string{"n011z", "n0111z", "n111z", "n1111z"}, []string{"n11z", "n0011z", "n11111z"}},
		{"n[1,11]1[1,11]z", []string{"n111z", "n1111z", "n11111z"}, []string{"n11z", "n111111z"}},
		{"n[1,11][1,11]x,n[1,11][1,11]y", []string{"n111y", "n11x"}, []string{"n111z"}},
		{", n1,, n2 ,", []string{"n1", "n2"}, []string{"n3"}},
	} {
		t.Run(tc.input, func(t *testing.T) {
			h, err := ParseHostList(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range tc.yes {
				if !h.Match(n) {
					t.Errorf("missing %s", n)
				}
			}
			for _, n := range tc.no {
				if h.Match(n) {
					t.Errorf("unexpected %s", n)
				}
			}
		})
	}
	for _, input := range []string{"n[]", "n[1,]", "n[3-1]", "n[1-65537]", "n[18446744073709551616]", "n[1", "n1]"} {
		if _, err := ParseHostList(input); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
	if got := CompressHosts([]string{"n03", "n01", "n02", "n02", "x"}); got != "n[01-03],x" {
		t.Fatal(got)
	}
	for input, want := range map[string]string{
		"n09,n10": "n[09-10]", "n9,n10": "n[9-10]", "n009,n010,n011": "n[009-011]",
		"n1,n01,n02,n2": "n[1-2,01-02]", "r1n01,r2n01,r1n02,r2n02": "r1n[01-02],r2n[01-02]",
		"n1x,n2x": "n1x,n2x", "n1x2,n1x3,n2x2,n2x3": "n1x[2-3],n2x[2-3]",
		"n9,n09,n10": "n[9,09-10]", "n1,n01,n10,n010": "n[1,01,10,010]",
		"n01,n2,n10": "n[2,01,10]", "n99,n001": "n[99,001]", "n099,n100,n101": "n[099-101]",
	} {
		if got := CompressHosts(strings.Split(input, ",")); got != want {
			t.Errorf("%q: got %q, want %q", input, got, want)
		}
	}
}

func BenchmarkHostListMatch(b *testing.B) {
	for _, segments := range []int{4, 8, 12} {
		for _, match := range []bool{false, true} {
			b.Run(fmt.Sprintf("segments=%d/match=%t", segments, match), func(b *testing.B) {
				h, err := ParseHostList("n" + strings.Repeat("[1,11]", segments) + "x")
				if err != nil {
					b.Fatal(err)
				}
				name := "n" + strings.Repeat("1", segments+segments/2)
				if match {
					name += "x"
				} else {
					name += "y"
				}
				b.ReportAllocs()
				for b.Loop() {
					if got := h.Match(name); got != match {
						b.Fatalf("got match=%t, want %t", got, match)
					}
				}
			})
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

func TestStructuredEncoderBytes(t *testing.T) {
	v := struct {
		List   []string `json:"list"`
		Value  string   `json:"value"`
		Nested struct {
			Empty string `json:"empty"`
			Count uint64 `json:"count"`
		} `json:"nested"`
	}{}
	v.Value = "/a<b>"
	v.Nested.Count = 2
	var out bytes.Buffer
	if err := WriteReport(&out, v, "json", SerializerOptions{}, false); err != nil {
		t.Fatal(err)
	}
	want := "{\n  \"list\": [\n  ],\n  \"value\": \"\\/a<b>\",\n  \"nested\": {\n    \"empty\": \"\",\n    \"count\": 2\n  }\n}\n"
	if out.String() != want {
		t.Fatalf("%q != %q", out.String(), want)
	}
	out.Reset()
	if err := WriteReport(&out, v, "yaml", SerializerOptions{}, false); err != nil {
		t.Fatal(err)
	}
	want = "%YAML 1.1\n--- !!map\n!!str list: !!seq []\n!!str value: !!str /a<b>\n!!str nested: !!map\n  !!str empty: !!str\n  !!str count: !!int 2\n...\n\n"
	if out.String() != want {
		t.Fatalf("%q != %q", out.String(), want)
	}
	out.Reset()
	if err := WriteReport(&out, v, "json", SerializerOptions{Compact: true}, false); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "{\"list\":[],\"value\":\"\\/a<b>\",\"nested\":{\"empty\":\"\",\"count\":2}}\n" {
		t.Fatal(got)
	}
}
