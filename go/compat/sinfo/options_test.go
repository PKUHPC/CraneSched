// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"strings"
	"testing"
)

func TestOptionOrdering(t *testing.T) {
	for _, tc := range []struct {
		args   []string
		env    map[string]string
		action string
		err    string
	}{
		{[]string{"--version", "--unknown"}, nil, "version", ""},
		{[]string{"--unknown", "--version"}, nil, "", "sinfo: unrecognized option '--unknown'\nTry \"sinfo --help\" for more information\n"},
		{[]string{"--help"}, map[string]string{"SINFO_ALL": "", "SINFO_PARTITION": ""}, "", "sinfo: error: Conflicting options, SINFO_ALL and SINFO_PARTITION, specified. Please choose one or the other.\n"},
		{[]string{"operand", "-V"}, nil, "version", ""},
		{[]string{"operand", "-V"}, map[string]string{"POSIXLY_CORRECT": ""}, "", ""},
		{[]string{"--", "-V"}, nil, "", ""},
		{[]string{"--form=x"}, nil, "", ""},
		{[]string{"--hel"}, nil, "", "sinfo: option '--hel' is ambiguous; possibilities: '--help' '--helpformat' '--helpFormat' '--helpstate'\nTry \"sinfo --help\" for more information\n"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			o, err := parseCommand(append([]string{"sinfo"}, tc.args...), tc.env)
			got := ""
			if err != nil {
				got = err.Error()
			}
			if got != tc.err {
				t.Fatalf("error = %q, want %q", got, tc.err)
			}
			if err == nil && o.Action != tc.action {
				t.Fatalf("action = %q, want %q", o.Action, tc.action)
			}
		})
	}
	o, err := parseCommand([]string{"sinfo", "-O", "NodeList", "-o", "%N", "--json", "v0.0.42"}, nil)
	if err != nil || !o.LongFormat || o.Format != "%N" || o.DataParser != "" {
		t.Fatalf("%+v %v", o, err)
	}
	o, err = parseCommand([]string{"sinfo", "-hi03junk", "-pnamed"}, nil)
	if err != nil || !o.NoHeader || o.Iterate != 3 || o.Partition != "named" {
		t.Fatalf("%+v %v", o, err)
	}
}

func TestActions(t *testing.T) {
	for _, tc := range []struct{ action, value, want string }{
		{"version", "", "slurm 26.05.4\n"},
		{"usage", "", "Usage: sinfo [-abdelNRrsTv] [-i seconds] [-t states] [-p partition] [-n nodes]\n             [-S fields] [-o format] [-O Format] [--federation] [--local]\n"},
		{"autocomplete", "node", "--nodes=\n\n"},
		{"autocomplete", "", ""},
		{"autocomplete", "not-an-option", ""},
		{"helpformat", "", "%S    %a    %V    %Y    %c    %O    %C    %L    \n%d    %f    %b    %e    %G    %G    %g    %B    \n%m    %o    %A    %F    %n    %N    %D    %h    \n%P    %R    %M    %I    %p    %E    %i    %r    \n%s    %X    %z    %t    %T    %Z    %l    %H    \n%u    %U    %v    %w    \n"},
	} {
		t.Run(tc.action+tc.value, func(t *testing.T) {
			var out bytes.Buffer
			if err := writeAction(&out, &plan{options: options{Action: tc.action, ActionValue: tc.value}}); err != nil {
				t.Fatal(err)
			}
			if out.String() != tc.want {
				t.Fatalf("%q != %q", out.String(), tc.want)
			}
		})
	}
	var out bytes.Buffer
	if err := writeAction(&out, &plan{options: options{Action: "helpFormat"}}); err != nil {
		t.Fatal(err)
	}
	if len(strings.Fields(out.String())) != 49 || strings.Contains(out.String(), "Share") {
		t.Fatal(out.String())
	}
}

func TestParsersConcurrent(t *testing.T) {
	for _, tc := range []struct {
		node, otherNode, pattern string
		state, otherState        string
		flag, format, field      string
	}{
		{"r02n3", "gpu01", "r[01-02]n[1-4]", "idle", "down", "-o", "%.09N", "NodeList"},
		{"gpu01", "r02n3", "gpu[01-02]", "down", "idle", "-O", "Partition:12", "Partition"},
	} {
		t.Run(tc.state, func(t *testing.T) {
			t.Parallel()
			o, err := parseCommand([]string{"sinfo", "-n", tc.pattern, "-t", tc.state, tc.flag, tc.format}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !o.NodeFilter.Match(tc.node) || o.NodeFilter.Match(tc.otherNode) {
				t.Errorf("wrong node selection for %q", tc.pattern)
			}
			if !o.StateFilter.match(nodeState{Base: tc.state}, 0) || o.StateFilter.match(nodeState{Base: tc.otherState}, 0) {
				t.Errorf("wrong state selection for %q", tc.state)
			}
			if len(o.Fields) != 1 || o.Fields[0].Field == nil || o.Fields[0].Field.Name != tc.field {
				t.Errorf("format %q: got %+v, want %s", tc.format, o.Fields, tc.field)
			}
		})
	}
}

func TestDecimalPrefixOverflow(t *testing.T) {
	if got := decimalPrefix("4294967297"); got != 1 {
		t.Fatal(got)
	}
	if got := decimalPrefix("99999999999999999999999"); got != -1 {
		t.Fatal(got)
	}
}
