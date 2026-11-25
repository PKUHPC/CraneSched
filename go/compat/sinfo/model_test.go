// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"testing"
)

func testSnapshot() *snapshot {
	return &snapshot{
		Nodes: []node{
			{Name: "n01", Hostname: "n01", Address: "n01", State: nodeState{Base: "idle"}, CPUs: 4, Memory: 3072, Sockets: 1, Cores: 4, Threads: 1},
			{Name: "n02", Hostname: "n02", Address: "n02", State: nodeState{Base: "idle"}, CPUs: 4, Memory: 3072, Sockets: 1, Cores: 4, Threads: 1},
			{Name: "n03", Hostname: "n03", Address: "n03", State: nodeState{Base: "idle"}, CPUs: 2, Memory: 2048, Sockets: 1, Cores: 2, Threads: 1},
		},
		Partitions: []partition{
			{Name: "compute", Nodes: []string{"n01", "n02", "n03"}, Available: "up", TimeLimit: durationLimit{Infinite: true}},
			{Name: "batch", Nodes: []string{"n01", "n02", "n03"}, Default: true, Available: "up", TimeLimit: durationLimit{Infinite: true}},
			{Name: "short", Nodes: []string{"n01"}, Available: "up", TimeLimit: durationLimit{Seconds: 300}},
		},
	}
}

func TestGroupingSelectionAndOrdering(t *testing.T) {
	cases := []struct {
		name string
		args []string
		out  string
	}{
		{"deduplicate", []string{"-h", "-o", "%D|%N"}, "3|n[01-03]\n"},
		{"counts", []string{"-h", "-o", "%F|%C|%N"}, "0/3/0/3|0/10/0/10|n[01-03]\n"},
		{"partition", []string{"-h", "-o", "%P|%t|%N"}, "compute|idle|n[01-03]\nbatch*|idle|n[01-03]\nshort|idle|n01\n"},
		{"inexact", []string{"-h", "-o", "%P|%c|%N"}, "compute|2+|n[01-03]\nbatch*|2+|n[01-03]\nshort|4|n01\n"},
		{"exact", []string{"-h", "-e", "-o", "%P|%c|%N"}, "compute|4|n[01-02]\ncompute|2|n03\nbatch*|4|n[01-02]\nbatch*|2|n03\nshort|4|n01\n"},
		{"alphabetic", []string{"-h", "-S", "P", "-o", "%P"}, "batch*\ncompute\nshort\n"},
		{"descending", []string{"-h", "-S", "-P", "-o", "%P"}, "short\ncompute\nbatch*\n"},
		{"no_nodes", []string{"-h", "-n", "missing", "-o", "%P|%D|%t|%N"}, "compute|0|n/a|\nbatch*|0|n/a|\nshort|0|n/a|\n"},
		{"no_partition", []string{"-h", "-p", "missing"}, ""},
		{"empty_state", []string{"-h", "-t", "", "-o", "%N"}, ""},
		{"all_state", []string{"-h", "-t", "all", "-o", "%N"}, "n[01-03]\n"},
		{"and", []string{"-h", "-t", "idle,down+idle", "-o", "%N"}, ""},
		{"or", []string{"-h", "-t", "IdL,down", "-o", "%N"}, "n[01-03]\n"},
		{"sticky_and", []string{"-h", "-t", "idle+down", "-t", "idle,down", "-o", "%N"}, ""},
		{"nodes", []string{"-h", "-N", "-o", "%N|%P"}, "n01|compute\nn01|batch*\nn01|short\nn02|compute\nn02|batch*\nn03|compute\nn03|batch*\n"},
		{"truncated_default", []string{"-h", "-o", "%3P"}, "com\nba*\nsho\n"},
		{"uncollected_hostname", []string{"-Nh", "-p", "compute", "-S", "-n", "-o", "%N"}, "n01\nn02\nn03\n"},
		{"uncollected_address", []string{"-Nh", "-p", "compute", "-S", "-o", "-o", "%N"}, "n01\nn02\nn03\n"},
		{"collected_hostname", []string{"-Nh", "-p", "compute", "-S", "-n", "-o", "%N|%n"}, "n03|n03\nn02|n02\nn01|n01\n"},
		{"collected_address", []string{"-Nh", "-p", "compute", "-S", "-o", "-O", "NodeList:0|,NodeAddr:0"}, "n03|n03\nn02|n02\nn01|n01\n"},
		{"reasons", []string{"-R"}, "REASON               USER      TIMESTAMP           NODELIST\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := renderTest(t, testSnapshot(), tc.args...)
			if out != tc.out || err != "" {
				t.Fatalf("got (%q,%q), want %q", out, err, tc.out)
			}
		})
	}
}

func TestNodeAndCPUClassification(t *testing.T) {
	s := testSnapshot()
	s.Nodes[0].State.Base = "mixed"
	s.Nodes[0].AllocCPUs = 1
	s.Nodes[1].State.Drain = true
	s.Nodes[2].State.Base = "down"
	out, _ := renderTest(t, s, "-h", "-o", "%F|%C")
	if out != "1/0/2/3|1/3/6/10\n" {
		t.Fatal(out)
	}
	s.Nodes[0].Reason = "warm"
	s.Nodes[1].Reason = "cold"
	s.Nodes[2].Reason = "cold"
	out, _ = renderTest(t, s, "-h", "-o", "%E|%D|%N")
	if out != "cold|2|n[02-03]\nwarm|1|n01\n" {
		t.Fatal(out)
	}
}

func TestHostPrefixOrdering(t *testing.T) {
	s := testSnapshot()
	s.Nodes = nil
	s.Partitions = s.Partitions[:1]
	s.Partitions[0].Nodes = []string{"r02n02", "r2n01", "r002n01", "r02n01", "r10n01", "r0n01"}
	for _, name := range s.Partitions[0].Nodes {
		s.Nodes = append(s.Nodes, node{Name: name, State: nodeState{Base: "idle"}})
	}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"default_node_order", []string{"-N"}, "r0n01\nr002n01\nr02n01\nr02n02\nr2n01\nr10n01\n"},
		{"explicit_node_order", []string{"-N", "-S", "N"}, "r0n01\nr002n01\nr02n01\nr02n02\nr2n01\nr10n01\n"},
		{"reverse_node_order", []string{"-N", "-S", "-N"}, "r10n01\nr2n01\nr02n02\nr02n01\nr002n01\nr0n01\n"},
		{"compressed_list", nil, "r0n01,r002n01,r02n[01-02],r2n01,r10n01\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"-h", "-p", "compute", "-o", "%N"}, tc.args...)
			got, diagnostic := renderTest(t, s, args...)
			if got != tc.want || diagnostic != "" {
				t.Fatalf("stdout=%q stderr=%q; want stdout=%q", got, diagnostic, tc.want)
			}
		})
	}
}
