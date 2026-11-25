// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"strings"
	"testing"

	"github.com/PKUHPC/CraneSched/go/generated/protos"
)

func TestFormatBehavior(t *testing.T) {
	cases := []struct {
		long            bool
		input, out, err string
	}{
		{false, "%08N!", "n01     !\n", ""},
		{false, "%.09N!", "      n01!\n", ""},
		{false, "%010N!", "n01       !\n", ""},
		{false, " lead\t%N 'x' # // \" y ", " lead\tn01 'x' # // \" y \n", ""},
		{false, "节点=%N 尾", "节点=n01 尾\n", ""},
		{false, "", "\n", ""}, {false, "abc \t", "abc \t\n", ""},
		{false, "%", "\n", ""}, {false, "%%", "\n", ""}, {false, "%%N", "n01\n", ""}, {false, "%N%", "n01\n", ""},
		{false, "prefix:%Q!%N", "prefix:%Q!n01\n", "Invalid node format specification: Q\n"},
		{false, "%09", "0        \n", ""},
		{false, "%..2N", "%..2N\n", "Invalid node format specification: .\n"},
		{false, "%2N!", "n0!\n", ""}, {false, "%.2N!", "n0!\n", ""},
		{true, "NodeList:09!", "n01      !\n", ""},
		{true, "NodeList:.09!", "      n01!\n", ""},
		{true, "NodeList:010!", "n01       !\n", ""},
		{true, ",,nOdElIsT:09!,,", "n01      !\n", ""},
		{true, " NodeList:09!", "!\n", "sinfo: error: Invalid job format specification:  NodeList\n"},
		{true, "NodeList :09!", "!\n", "sinfo: error: Invalid job format specification: NodeList \n"},
		{true, "NodeList:", "n01\n", ""},
		{true, "NodeList:tail", "n01tail\n", ""},
		{true, "NotAField,NodeList:09!", "n01      !\n", "sinfo: error: Invalid job format specification: NotAField\n"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			flag := "-o"
			if tc.long {
				flag = "-O"
			}
			out, err := renderTest(t, testSnapshot(), "-h", "-p", "compute", "-n", "n01", flag, tc.input)
			if out != tc.out || err != tc.err {
				t.Fatalf("got (%q,%q), want (%q,%q)", out, err, tc.out, tc.err)
			}
		})
	}
}

func TestMissingTopologyIsNotInvented(t *testing.T) {
	o, err := parseCommand([]string{"sinfo", "-O", "SocketCoreThread"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := adaptSnapshot(adapterFixture(), o.Query)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := newTextRenderer().table(&out, buildRows(s, o), o); err == nil || out.Len() != 0 {
		t.Fatalf("out=%q err=%v", out.String(), err)
	}
}

func TestAbsentTelemetryIsNotAConfiguredValue(t *testing.T) {
	for _, field := range []string{"CPUsLoad", "FreeMem"} {
		o, err := parseCommand([]string{"sinfo", "-O", field}, nil)
		if err != nil {
			t.Fatal(err)
		}
		s, err := adaptSnapshot(adapterFixture(), o.Query)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := newTextRenderer().table(&out, buildRows(s, o), o); err == nil || out.Len() != 0 || !strings.Contains(err.Error(), "observation") {
			t.Fatalf("%s out=%q err=%v", field, &out, err)
		}
	}
}

func TestGRESDataErrorOnlyAffectsRequestsForGRES(t *testing.T) {
	reply := adapterFixture()
	reply.Nodes[0].ResAlloc.Gres = &protos.DedicatedResourceInNode{NameTypeMap: map[string]*protos.DeviceTypeSlotsMap{
		"gpu": {TypeSlotsMap: map[string]*protos.Slots{"a": {Slots: []string{"/dev/unknown"}}}},
	}}
	o, err := parseCommand([]string{"sinfo", "-h", "-o", "%N"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := adaptSnapshot(reply, o.Query)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = newTextRenderer().table(&out, buildRows(s, o), o); err != nil || out.String() != "n1\n" {
		t.Fatalf("out=%q err=%v", &out, err)
	}
	o.Fields, _ = parseFormat("GresUsed", true)
	out.Reset()
	if err = newTextRenderer().table(&out, buildRows(s, o), o); err == nil || out.Len() != 0 {
		t.Fatalf("out=%q err=%v", &out, err)
	}
}
