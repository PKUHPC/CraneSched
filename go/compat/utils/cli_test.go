// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package utils

import (
	"io"
	"reflect"
	"testing"
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
		{"missing_long_prefix", []string{"--out"}, false, nil, "option '--output' requires an argument"},
		{"missing_alias_prefix", []string{"--cl"}, false, nil, "option '--cluster' requires an argument"},
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
