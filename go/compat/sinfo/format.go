// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type fieldSpec struct {
	Name       string
	Short      byte
	Header     string
	Group      string
	Numeric    bool
	Hidden     bool
	Capability string
}

// Names and headings are the public format vocabulary. Capability identifies
// a real product feature, not whether an implementation happens to exist yet.
var fieldSpecs = []fieldSpec{
	{Name: "AllocMem", Header: "ALLOCMEM", Group: "allocmem", Numeric: true},
	{Name: "AllocNodes", Short: 'S', Header: "ALLOCNODES", Capability: "partition allocation-node restriction"},
	{Name: "Available", Short: 'a', Header: "AVAIL", Group: "available"},
	{Name: "Cluster", Short: 'V', Header: "CLUSTER"},
	{Name: "Comment", Header: "COMMENT", Group: "comment", Capability: "node comments"},
	{Name: "Cores", Short: 'Y', Header: "CORES", Group: "cores", Numeric: true},
	{Name: "CPUs", Short: 'c', Header: "CPUS", Group: "cpus", Numeric: true},
	{Name: "CPUsLoad", Short: 'O', Header: "CPU_LOAD", Group: "load", Numeric: true},
	{Name: "CPUsState", Short: 'C', Header: "CPUS(A/I/O/T)", Group: "cpus", Numeric: true},
	{Name: "DefaultTime", Short: 'L', Header: "DEFAULTTIME", Group: "defaulttime", Capability: "partition default time limit"},
	{Name: "Disk", Short: 'd', Header: "TMP_DISK", Group: "disk", Numeric: true, Capability: "node temporary disk capacity"},
	{Name: "Extra", Header: "EXTRA", Group: "extra", Capability: "node extra attributes"},
	{Name: "Features", Short: 'f', Header: "AVAIL_FEATURES", Group: "features", Capability: "node features"},
	{Name: "features_act", Short: 'b', Header: "ACTIVE_FEATURES", Group: "activefeatures", Capability: "active node features"},
	{Name: "FreeMem", Short: 'e', Header: "FREE_MEM", Group: "freemem", Numeric: true},
	{Name: "Gres", Short: 'G', Header: "GRES", Group: "gres"},
	{Name: "GresUsed", Short: 'G', Header: "GRES_USED", Group: "gresused"},
	{Name: "Groups", Short: 'g', Header: "GROUPS", Group: "groups", Capability: "partition Unix-group restriction"},
	{Name: "MaxCPUsPerNode", Short: 'B', Header: "MAX_CPUS_PER_NODE", Group: "maxcpus", Capability: "partition per-node CPU limit"},
	{Name: "Memory", Short: 'm', Header: "MEMORY", Group: "memory", Numeric: true},
	{Name: "NodeAddr", Short: 'o', Header: "NODE_ADDR", Group: "address"},
	{Name: "NodeAI", Short: 'A', Header: "NODES(A/I)"},
	{Name: "NodeAIOT", Short: 'F', Header: "NODES(A/I/O/T)"},
	{Name: "NodeHost", Short: 'n', Header: "HOSTNAMES", Group: "hostname"},
	{Name: "NodeList", Short: 'N', Header: "NODELIST"},
	{Name: "Nodes", Short: 'D', Header: "NODES"},
	{Name: "OverSubscribe", Short: 'h', Header: "OVERSUBSCRIBE", Capability: "partition oversubscription policy"},
	{Name: "Partition", Short: 'P', Header: "PARTITION", Group: "partition"},
	{Name: "PartitionName", Short: 'R', Header: "PARTITION", Group: "partition"},
	{Name: "Port", Header: "PORT", Group: "port"},
	{Name: "PreemptMode", Short: 'M', Header: "PREEMPT_MODE", Group: "preempt"},
	{Name: "PriorityJobFactor", Short: 'I', Header: "PRIO_JOB_FACTOR", Group: "priorityfactor"},
	{Name: "PriorityTier", Short: 'p', Header: "PRIO_TIER", Group: "prioritytier", Capability: "partition priority tier"},
	{Name: "Reason", Short: 'E', Header: "REASON", Group: "reason"},
	{Name: "Reservation", Short: 'i', Header: "RESERVATION", Group: "reservation"},
	{Name: "Root", Short: 'r', Header: "ROOT", Group: "root", Capability: "root-only partitions"},
	{Name: "Share", Short: 'h', Header: "OVERSUBSCRIBE", Group: "oversubscribe", Hidden: true, Capability: "partition oversubscription policy"},
	{Name: "Size", Short: 's', Header: "JOB_SIZE", Group: "size", Capability: "partition job-size limit"},
	{Name: "Sockets", Short: 'X', Header: "SOCKETS", Group: "sockets", Numeric: true},
	{Name: "SocketCoreThread", Short: 'z', Header: "S:C:T", Group: "topology", Numeric: true},
	{Name: "StateCompact", Short: 't', Header: "STATE", Group: "state"},
	{Name: "StateComplete", Header: "STATECOMPLETE", Group: "statecomplete"},
	{Name: "StateLong", Short: 'T', Header: "STATE", Group: "state"},
	{Name: "Threads", Short: 'Z', Header: "THREADS", Group: "threads", Numeric: true},
	{Name: "Time", Short: 'l', Header: "TIMELIMIT", Group: "time"},
	{Name: "TimeStamp", Short: 'H', Header: "TIMESTAMP", Group: "reasontime"},
	{Name: "User", Short: 'u', Header: "USER", Group: "reasonuser"},
	{Name: "UserLong", Short: 'U', Header: "USER", Group: "reasonuser"},
	{Name: "Version", Short: 'v', Header: "VERSION", Group: "version"},
	{Name: "Weight", Short: 'w', Header: "WEIGHT", Group: "weight", Numeric: true, Capability: "node scheduling weight"},
}

type formatItem struct {
	Field           *fieldSpec
	Width           int
	Right           bool
	Suffix, Literal string
}

type shortFormatSyntax struct {
	Right  bool   `parser:"@Dot?"`
	Width  string `parser:"@Number?"`
	Code   string `parser:"@Code?"`
	Suffix string `parser:"@Suffix?"`
}

var shortFormatParser = participle.MustBuild[shortFormatSyntax](participle.Lexer(lexer.MustStateful(lexer.Rules{
	"Root": {
		{Name: "Dot", Pattern: `\.`, Action: lexer.Push("Width")},
		{Name: "Number", Pattern: `[0-9]+`, Action: lexer.Push("Code")},
		{Name: "Code", Pattern: `(?s).`, Action: lexer.Push("Suffix")},
	},
	"Width": {
		{Name: "Number", Pattern: `[0-9]+`, Action: lexer.Push("Code")},
		{Name: "Code", Pattern: `(?s).`, Action: lexer.Push("Suffix")},
	},
	"Code": {
		{Name: "Code", Pattern: `(?s).`, Action: lexer.Push("Suffix")},
	},
	"Suffix": {{Name: "Suffix", Pattern: `(?s).+`}},
})))

type longFormatSyntax struct {
	Name string          `parser:"@Name?"`
	Tail *longFormatTail `parser:"@@?"`
}
type longFormatTail struct {
	Colon string `parser:"@Colon"`
	Raw   string `parser:"@Tail?"`
}

var longFormatParser = participle.MustBuild[longFormatSyntax](participle.Lexer(lexer.MustStateful(lexer.Rules{
	"Root": {
		{Name: "Name", Pattern: `[^:]+`},
		{Name: "Colon", Pattern: `:`, Action: lexer.Push("Tail")},
	},
	"Tail": {{Name: "Tail", Pattern: `(?s).+`}},
})))

func fieldByName(name string) *fieldSpec {
	for i := range fieldSpecs {
		if strings.EqualFold(fieldSpecs[i].Name, name) {
			return &fieldSpecs[i]
		}
	}
	return nil
}
func fieldByCode(code byte) *fieldSpec {
	if code == 0 {
		return nil
	}
	for i := range fieldSpecs {
		if fieldSpecs[i].Short == code {
			return &fieldSpecs[i]
		}
	}
	return nil
}

func parseFormat(input string, long bool) ([]formatItem, []string) {
	var items []formatItem
	var diagnostics []string
	if !long {
		first := strings.IndexByte(input, '%')
		if first < 0 {
			return []formatItem{{Literal: input}}, nil
		}
		if first > 0 {
			items = append(items, formatItem{Literal: input[:first]})
			input = input[first:]
		}
		if strings.EqualFold(input, "%all") {
			for _, code := range "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" {
				if f := fieldByCode(byte(code)); f != nil {
					suffix := "|"
					if code >= 'A' && code <= 'Z' {
						suffix = " |"
					}
					items = append(items, formatItem{Field: f, Suffix: suffix})
				}
			}
			if len(items) != 0 {
				items[len(items)-1].Suffix = strings.TrimSuffix(items[len(items)-1].Suffix, "|")
			}
			return items, nil
		}
		for _, raw := range strings.Split(input, "%") {
			if raw == "" {
				continue
			}
			ast, err := shortFormatParser.ParseString("", raw)
			if err != nil {
				items = append(items, formatItem{Literal: "%" + raw})
				diagnostics = append(diagnostics, "Invalid node format specification: \x00\n")
				continue
			}
			var rawWidth uint32
			for _, digit := range ast.Width {
				rawWidth = rawWidth*10 + uint32(digit-'0')
			}
			width := int(int32(rawWidth))
			// An omitted code matches the first long-only registry entry.
			// This is observable for inputs such as "%09".
			f := &fieldSpecs[0]
			if ast.Code != "" {
				f = fieldByCode(ast.Code[0])
			}
			if len(ast.Code) > 1 {
				f = nil
			}
			item := formatItem{Field: f, Width: width, Right: ast.Right, Suffix: ast.Suffix}
			if f == nil {
				item = formatItem{Literal: "%" + raw}
				diagnostics = append(diagnostics, "Invalid node format specification: "+string([]byte{ast.Code[0]})+"\n")
			}
			items = append(items, item)
		}
		return items, diagnostics
	}
	for _, raw := range strings.Split(input, ",") {
		if raw == "" {
			continue
		}
		ast, err := longFormatParser.ParseString("", raw)
		if err != nil {
			diagnostics = append(diagnostics, "sinfo: error: Invalid job format specification: "+raw+"\n")
			continue
		}
		item := formatItem{Field: fieldByName(ast.Name), Width: 20}
		if ast.Tail != nil {
			item.Right, item.Width, item.Suffix = parseLongTail(ast.Tail.Raw)
		}
		if strings.EqualFold(ast.Name, "all") {
			all, _ := parseFormat("%all", false)
			items = append(items, all...)
			item.Suffix = ""
		}
		if item.Field == nil {
			diagnostics = append(diagnostics, "sinfo: error: Invalid job format specification: "+ast.Name+"\n")
		}
		items = append(items, item)
	}
	return items, diagnostics
}

func parseLongTail(raw string) (bool, int, string) {
	right := strings.HasPrefix(raw, ".")
	if right {
		raw = raw[1:]
	}
	start := len(raw) - len(strings.TrimLeft(raw, " \t\n\r\v\f"))
	end := start
	if end < len(raw) && (raw[end] == '+' || raw[end] == '-') {
		end++
	}
	digits := end
	for end < len(raw) && raw[end] >= '0' && raw[end] <= '9' {
		end++
	}
	if end == digits {
		return right, 0, raw
	}
	n, _ := strconv.ParseInt(raw[start:end], 10, 64)
	return right, int(int32(n)), raw[end:]
}

func column(text string, width int, right bool, cut bool) string {
	if width <= 0 {
		return text
	}
	if (cut || !right) && len(text) > width {
		text = text[:width]
	}
	if len(text) >= width {
		return text
	}
	pad := strings.Repeat(" ", width-len(text))
	if right {
		return pad + text
	}
	return text + pad
}

// Validate facts only after querying, in the same order for each output mode.
func validateProjection(rows []*row, o *plan) error {
	needsTopology, needsIntegralCPUs := false, o.Serialization != ""
	needsGRES := o.Serialization != ""
	needsLoad := o.Serialization != "" || strings.ContainsRune(o.Sort, 'O')
	needsFreeMemory := o.Serialization != "" || strings.ContainsRune(o.Sort, 'e')
	for _, item := range o.Fields {
		if item.Field == nil {
			continue
		}
		switch item.Field.Name {
		case "CPUsLoad":
			needsLoad = true
		case "FreeMem":
			needsFreeMemory = true
		case "Gres", "GresUsed":
			needsGRES = true
		case "CPUsState":
			needsIntegralCPUs = true
		case "Sockets", "Cores", "Threads", "SocketCoreThread":
			needsTopology = true
		}
	}
	needsTopology = needsTopology || o.Serialization != ""
	for _, r := range rows {
		for _, n := range r.Nodes {
			if n.TelemetryExpected && needsLoad && n.CPULoad == nil {
				return fmt.Errorf("controller has no CPU load observation for registered node %q", n.Name)
			}
			if n.TelemetryExpected && needsFreeMemory && n.FreeMemory == nil {
				return fmt.Errorf("controller has no free-memory observation for registered node %q", n.Name)
			}
			if needsGRES && n.GresIssue != nil {
				return fmt.Errorf("node %q: %w", n.Name, n.GresIssue)
			}
			if needsIntegralCPUs && n.FractionalAllocation {
				return fmt.Errorf("node %q has fractional CPU allocations that Slurm sinfo cannot represent", n.Name)
			}
			if needsTopology && (n.Sockets == 0 || n.Cores == 0 || n.Threads == 0) {
				return fmt.Errorf("controller has no representable scheduling CPU topology for node %q", n.Name)
			}
		}
	}
	return nil
}

func validateStructuredProjection(rows []*row, snap *snapshot, o *plan) error {
	// The v45 schema implicitly requests these fields,
	// even when -o selects a smaller text/grouping vocabulary. An absent
	// Crane feature must not turn into a plausible zero or empty JSON value.
	for _, field := range []string{"Disk", "Weight", "Features", "features_act", "Comment", "Extra", "MaxCPUsPerNode", "AllocNodes", "Groups", "DefaultTime", "OverSubscribe", "Root", "Size", "PriorityTier"} {
		if capability := snap.UnsupportedFields[field]; capability != "" {
			return unsupported("--"+o.Serialization+" field "+field, capability)
		}
	}
	if err := validateProjection(rows, o); err != nil {
		return err
	}
	return nil
}
