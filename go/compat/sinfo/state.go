// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

const stateNames = "unknown,down,idle,allocated,mixed,future,drain,drained,draining,no_respond,reserved,planned,blocked,cloud,completing,powering_down,powered_down,power_down,powering_up,power_up,inval,fail,maint,reboot,reboot^,external,dynamic_future,dynamic_norm"

type stateSyntax struct {
	Pieces []*statePiece `parser:"@@*"`
}
type statePiece struct {
	Separator string `parser:"@Separator"`
	Term      string `parser:"| @Term"`
}

var stateParser = participle.MustBuild[stateSyntax](participle.Lexer(lexer.MustSimple([]lexer.SimpleRule{
	{Name: "Separator", Pattern: `[,&+]+`}, {Name: "Term", Pattern: `[^,&+]+`},
})))

type stateTerm struct {
	Name   string
	Negate bool
}
type stateExpression struct {
	Terms     []stateTerm
	AllOf     bool
	AllStates bool
	Enabled   bool
}

// Ordered aliases matter: Slurm accepts prefixes and chooses the first match.
var stateAliases = []struct {
	name    string
	aliases []string
}{
	{"unknown", []string{"unk", "unknown"}}, {"down", []string{"down"}},
	{"idle", []string{"idle"}}, {"allocated", []string{"alloc", "allocated"}},
	{"mixed", []string{"mix", "mixed"}},
	{"future", []string{"futr", "future"}}, {"blocked", []string{"blocked", "block"}},
	{"inval", []string{"invalid_reg", "inval"}}, {"planned", []string{"planned", "plnd"}},
	{"drain", []string{"drain"}}, {"drained", []string{"drained"}},
	{"reserved", []string{"resv", "reserved"}}, {"draining", []string{"draining", "drng"}},
	{"dynamic_future", []string{"dynamic_future"}}, {"dynamic_norm", []string{"dynamic_norm"}},
	{"completing", []string{"comp", "completing"}}, {"no_respond", []string{"no_respond"}},
	{"power_up", []string{"power_up", "pow_up"}}, {"powering_down", []string{"powring_dn", "powering_down"}},
	{"powered_down", []string{"powered_dn", "powered_down"}}, {"power_down", []string{"power_down", "pow_dn"}},
	{"powering_up", []string{"powering_up"}}, {"fail", []string{"fail"}},
	{"maint", []string{"maint", "maintenance"}}, {"reboot", []string{"boot", "reboot", "reboot_requested"}},
	{"reboot^", []string{"boot^", "reboot^", "reboot_issued"}}, {"cloud", []string{"cloud"}},
	{"external", []string{"external"}},
}

func parseStates(input string, stickyAnd bool) (stateExpression, error) {
	result := stateExpression{AllOf: stickyAnd, Enabled: true}
	if strings.EqualFold(input, "all") {
		result.AllStates = true
		return result, nil
	}
	ast, err := stateParser.ParseString("", input)
	if err != nil {
		return result, err
	}
	for _, piece := range ast.Pieces {
		if piece.Separator != "" {
			result.AllOf = result.AllOf || strings.ContainsAny(piece.Separator, "+&")
			continue
		}
		text := piece.Term
		term := stateTerm{}
		if strings.HasPrefix(text, "!") || strings.HasPrefix(text, "~") {
			term.Negate = true
			text = text[1:]
		}
		lower := strings.ToLower(text)
		for _, entry := range stateAliases {
			for _, alias := range entry.aliases {
				if strings.HasPrefix(alias, lower) {
					term.Name = entry.name
					break
				}
			}
			if term.Name != "" {
				break
			}
		}
		if term.Name == "" {
			return result, optionError(fmt.Sprintf("Bad state string: \"%s\"\nsinfo: error: valid states: %s", text, stateNames))
		}
		result.Terms = append(result.Terms, term)
	}
	return result, nil
}

type nodeState struct {
	Base                                                         string
	Drain, NoRespond, Completing, Reserved, Maintenance, Planned bool
	Invalid, Fail, Blocked, RebootRequested, RebootIssued        bool
	Cloud, External, DynamicFuture, DynamicNormal                bool
	Power                                                        string
}

func (s nodeState) long() string {
	name, suffix := s.display()
	return name + suffix
}
func (s nodeState) compact() string {
	name, suffix := s.display()
	short := map[string]string{"unknown": "unk", "allocated": "alloc", "mixed": "mix", "future": "futr", "draining": "drng", "drained": "drain", "completing": "comp", "reserved": "resv", "blocked": "block", "planned": "plnd", "reboot": "boot", "failing": "failg"}
	if value, ok := short[name]; ok {
		name = value
	}
	return name + suffix
}

// Name selection and suffix precedence are separate: a pending power operation
// takes precedence over a stale heartbeat, while idle maintenance is a name.
func (s nodeState) display() (string, string) {
	busy := s.Base == "allocated" || s.Base == "mixed"
	responseSuffix := ""
	if s.NoRespond {
		responseSuffix = "*"
	}
	switch {
	case s.Invalid:
		return "inval", ""
	case s.Maintenance && !s.Drain && !busy && s.Base != "down":
		return "maint", responseSuffix
	case (s.RebootRequested || s.RebootIssued) && !busy:
		if s.RebootIssued {
			return "reboot", "^"
		}
		return "reboot", responseSuffix
	}
	suffix := responseSuffix
	if power, ok := map[string]string{"powering_up": "#", "powering_down": "%", "powered_down": "~", "power_down": "!"}[s.Power]; ok {
		suffix = power
	}
	if s.RebootRequested {
		suffix = "@"
	}
	if s.RebootIssued {
		suffix = "^"
	}
	if s.Maintenance {
		suffix = "$"
	}
	name := s.Base
	switch {
	case s.Drain:
		name = "drained"
		if busy || s.Completing {
			name = "draining"
		}
	case s.Fail:
		name = "fail"
		if busy || s.Completing {
			name = "failing"
		}
		suffix = responseSuffix
	case s.Base == "down":
	case s.Base == "allocated":
		if s.Completing && suffix == "" {
			suffix = "+"
		}
	case s.Completing:
		name = "completing"
	case s.Base == "idle" && suffix == "":
		switch {
		case s.Reserved:
			name = "reserved"
		case s.Blocked:
			name = "blocked"
		case s.Planned:
			name = "planned"
		}
	case s.Base == "mixed" && suffix == "" && s.Planned:
		suffix = "-"
	case s.Base == "unknown":
		suffix = responseSuffix
	}
	return name, suffix
}
func (s nodeState) complete() string {
	parts := []string{strings.ToUpper(s.Base)}
	for _, flag := range []struct {
		set  bool
		name string
	}{{s.Blocked, "BLOCKED"}, {s.Cloud, "CLOUD"}, {s.Completing, "COMPLETING"}, {s.Drain, "DRAIN"}, {s.DynamicFuture, "DYNAMIC_FUTURE"}, {s.DynamicNormal, "DYNAMIC_NORM"}, {s.External, "EXTERNAL"}, {s.Invalid, "INVALID_REG"}, {s.Fail, "FAIL"}, {s.Maintenance, "MAINTENANCE"}, {s.Power == "power_down", "POWER_DOWN"}, {s.Power == "power_up", "POWER_UP"}, {s.Power == "powered_down", "POWERED_DOWN"}, {s.RebootRequested, "REBOOT_REQUESTED"}, {s.RebootIssued, "REBOOT_ISSUED"}, {s.Reserved, "RESERVED"}, {s.NoRespond, "NOT_RESPONDING"}, {s.Planned, "PLANNED"}, {s.Power == "powering_up", "POWERING_UP"}, {s.Power == "powering_down", "POWERING_DOWN"}} {
		if flag.set {
			parts = append(parts, flag.name)
		}
	}
	return strings.Join(parts, "+")
}
func (s nodeState) matches(name string, allocated uint64) bool {
	switch name {
	case "allocated":
		return allocated != 0
	case "drain":
		return s.Drain
	case "drained":
		return s.Drain && s.Base != "allocated" && s.Base != "mixed" && !s.Completing
	case "draining":
		return s.Drain && (s.Base == "allocated" || s.Base == "mixed" || s.Completing)
	case "no_respond":
		return s.NoRespond
	case "completing":
		return s.Completing
	case "reserved":
		return s.Reserved
	case "maint":
		return s.Maintenance
	case "planned":
		return s.Planned
	case "fail":
		return s.Fail
	case "inval":
		return s.Invalid
	case "blocked":
		return s.Blocked
	case "reboot":
		return s.RebootRequested
	case "reboot^":
		return s.RebootIssued
	case "cloud":
		return s.Cloud
	case "external":
		return s.External
	case "dynamic_future":
		return s.DynamicFuture
	case "dynamic_norm":
		return s.DynamicNormal
	case "powered_down", "powering_down", "power_down", "power_up", "powering_up":
		return s.Power == name
	default:
		return s.Base == name
	}
}

func (s nodeState) order() uint32 {
	value := map[string]uint32{"unknown": 0, "down": 1, "idle": 2, "allocated": 3, "error": 4, "mixed": 5, "future": 6}[s.Base]
	flags := []struct {
		set bool
		bit uint
	}{{s.External, 4}, {s.Reserved, 5}, {s.Cloud, 7}, {s.Drain, 9}, {s.Completing, 10}, {s.NoRespond, 11}, {s.Power == "powered_down", 12}, {s.Fail, 13}, {s.Power == "powering_up", 14}, {s.Maintenance, 15}, {s.RebootRequested, 16}, {s.Power == "powering_down", 18}, {s.DynamicFuture, 19}, {s.RebootIssued, 20}, {s.Planned, 21}, {s.Invalid, 22}, {s.Power == "power_down", 23}, {s.Power == "power_up", 24}, {s.DynamicNormal, 26}, {s.Blocked, 27}}
	for _, f := range flags {
		if f.set {
			value |= 1 << f.bit
		}
	}
	return value
}
func (e stateExpression) match(s nodeState, allocated uint64) bool {
	if e.AllStates {
		// Every base state participates in "all". Their union matches every
		// node, but their intersection is empty when an earlier -t enabled AND.
		return !e.AllOf
	}
	if len(e.Terms) == 0 {
		return !e.Enabled
	}
	for _, term := range e.Terms {
		match := s.matches(term.Name, allocated) != term.Negate
		if e.AllOf && !match {
			return false
		}
		if !e.AllOf && match {
			return true
		}
	}
	return e.AllOf
}
