// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package utils supplies shared mechanisms for standalone compatibility commands.
// Command semantics and protocol models remain with their respective packages.
package utils

import (
	"fmt"
	"io"
	"strings"
)

// Argument specifies how a GNU-style option consumes its value.
type Argument byte

const (
	NoArgument Argument = iota
	RequiredArgument
	OptionalArgument
)

// OptionSpec order determines the order of ambiguous-option diagnostics.
// Long aliases may share a short option and argument mode.
type OptionSpec struct {
	Name     string
	Short    byte
	Argument Argument
}

// Option preserves the distinction between no value and an explicitly empty one.
type Option struct {
	Name     string
	Value    string
	Attached bool
}

// OptionScanner scans lazily so that commands can stop on help/version before
// later arguments are inspected. It never decides a command's option semantics.
type OptionScanner struct {
	args  []string
	specs []OptionSpec
	posix bool
	short string
}

// NewOptionScanner takes arguments excluding argv[0]. The caller owns process
// naming, diagnostic prefixes, environment defaults and handling of operands.
func NewOptionScanner(args []string, specs []OptionSpec, posix bool) *OptionScanner {
	return &OptionScanner{args: args, specs: specs, posix: posix}
}

// Next returns io.EOF at the end of options. Errors contain only the GNU
// diagnostic body; callers add their command's prefix and help hint.
func (s *OptionScanner) Next() (Option, error) {
	for len(s.short) == 0 {
		if len(s.args) == 0 {
			return Option{}, io.EOF
		}
		arg := s.args[0]
		s.args = s.args[1:]
		if arg == "--" {
			s.args = nil
			return Option{}, io.EOF
		}
		if arg == "-" || !strings.HasPrefix(arg, "-") {
			if s.posix {
				s.args = nil
				return Option{}, io.EOF
			}
			continue
		}
		if strings.HasPrefix(arg, "--") {
			name, value, attached := strings.Cut(arg[2:], "=")
			spec, err := s.longOption(name, arg)
			if err != nil {
				return Option{}, err
			}
			if spec.Argument == NoArgument && attached {
				return Option{}, fmt.Errorf("option '--%s' doesn't allow an argument", spec.Name)
			}
			if spec.Argument == RequiredArgument && !attached {
				if len(s.args) == 0 {
					return Option{}, fmt.Errorf("option '%s' requires an argument", arg)
				}
				value, s.args = s.args[0], s.args[1:]
			}
			return Option{Name: spec.Name, Value: value, Attached: attached}, nil
		}
		s.short = arg[1:]
	}
	key := s.short[0]
	s.short = s.short[1:]
	for _, spec := range s.specs {
		if spec.Short == 0 || spec.Short != key {
			continue
		}
		value, attached := "", false
		if spec.Argument != NoArgument {
			if len(s.short) != 0 {
				value, attached, s.short = s.short, true, ""
			} else if spec.Argument == RequiredArgument {
				if len(s.args) == 0 {
					return Option{}, fmt.Errorf("option requires an argument -- '%c'", key)
				}
				value, s.args = s.args[0], s.args[1:]
			}
		}
		return Option{Name: spec.Name, Value: value, Attached: attached}, nil
	}
	return Option{}, fmt.Errorf("invalid option -- '%c'", key)
}

func (s *OptionScanner) longOption(name, arg string) (OptionSpec, error) {
	var matches []OptionSpec
	for _, spec := range s.specs {
		if spec.Name == name {
			return spec, nil
		}
		if name != "" && strings.HasPrefix(spec.Name, name) {
			matches = append(matches, spec)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		same := matches[0].Short != 0
		var names []string
		for _, spec := range matches {
			same = same && spec.Short == matches[0].Short && spec.Argument == matches[0].Argument
			names = append(names, "'--"+spec.Name+"'")
		}
		if same {
			return matches[0], nil
		}
		return OptionSpec{}, fmt.Errorf("option '%s' is ambiguous; possibilities: %s", arg, strings.Join(names, " "))
	}
	return OptionSpec{}, fmt.Errorf("unrecognized option '%s'", arg)
}
