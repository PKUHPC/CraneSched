// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package utils

import (
	"fmt"
	"strings"
	"testing"
)

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
