// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/PKUHPC/CraneSched/go/compat/protocol/v45"
	"github.com/PKUHPC/CraneSched/go/compat/utils"
)

func reportLimit(l durationLimit) v45.Number[v45.Int32] {
	return v45.Numeric[v45.Int32](l.Seconds/60, l.Unset, l.Infinite)
}

func orderedNames(names []string) []string {
	unique := make(map[string]bool)
	result := make([]string, 0, len(names))
	for _, name := range names {
		if name != "" && !unique[name] {
			result = append(result, name)
			unique[name] = true
		}
	}
	sort.Slice(result, func(i, j int) bool { return utils.HostLess(result[i], result[j]) })
	return result
}

func structuredRow(r *row, o *plan, userName func(uint32) (string, bool)) v45.SinfoRecord {
	nodes, cpus := r.counts()
	result := v45.SinfoRecord{
		Nodes: v45.SinfoNodes{
			Allocated: v45.Int32(nodes.Alloc), Idle: v45.Int32(nodes.Idle),
			Other: v45.Int32(nodes.Other), Total: v45.Int32(nodes.Total),
			Nodes: orderedNames(r.names("")),
		},
		CPUs:      v45.SinfoCPUs{Total: v45.Int32(cpus.Total)},
		Cluster:   r.Cluster,
		Partition: structuredPartition(r.Partition),
	}
	if !slices.ContainsFunc(r.Nodes, func(n *node) bool { return n.FractionalAllocation }) {
		result.CPUs.Allocated = new(v45.Int32(cpus.Alloc))
		result.CPUs.Idle = new(v45.Int32(cpus.Idle))
		result.CPUs.Other = new(v45.Int32(cpus.Other))
	}
	if len(r.Nodes) == 0 {
		return result
	}
	n := r.Nodes[0]
	result.Port = new(v45.Int32(n.Port))
	result.Node = &v45.SinfoNode{State: strings.Split(n.State.complete(), "+")}
	result.Reservation = new(n.Reservation)
	result.Reason = &v45.SinfoReason{Description: n.Reason}
	minMemory, maxMemory := r.minmax("memory")
	result.Memory = &v45.SinfoMemory{
		Minimum: minMemory, Maximum: maxMemory, Allocated: r.Nodes[len(r.Nodes)-1].AllocMemory,
	}
	for _, field := range o.Fields {
		if field.Field == nil {
			continue
		}
		switch field.Field.Group {
		case "address":
			result.Nodes.Addresses = orderedNames(r.names("address"))
		case "hostname":
			result.Nodes.Hostnames = orderedNames(r.names("hostname"))
		}
	}
	if !n.ReasonTime.IsZero() {
		result.Reason.Time = new(n.ReasonTime.Unix())
	}
	if n.ReasonUID != nil {
		name, ok := userName(*n.ReasonUID)
		if !ok {
			name = strconv.FormatUint(uint64(*n.ReasonUID), 10)
		}
		result.ReasonSetByUser = new(name)
	}
	rangeOf := func(group string) (v45.Int32, v45.Int32) {
		lo, hi := r.minmax(group)
		return v45.Int32(lo), v45.Int32(hi)
	}
	lo, hi := rangeOf("cpus")
	result.CPUs.Minimum, result.CPUs.Maximum = new(lo), new(hi)
	if !slices.ContainsFunc(r.Nodes, func(n *node) bool { return n.CPULoad == nil }) {
		lo, hi := rangeOf("load")
		result.CPUs.Load = &v45.SinfoCPUsLoad{Minimum: lo, Maximum: hi}
	}
	if !slices.ContainsFunc(r.Nodes, func(n *node) bool { return n.Sockets == 0 || n.Cores == 0 || n.Threads == 0 }) {
		lo, hi := rangeOf("sockets")
		result.Sockets = &v45.SinfoSockets{Minimum: lo, Maximum: hi}
		lo, hi = rangeOf("cores")
		result.Cores = &v45.SinfoCores{Minimum: lo, Maximum: hi}
		lo, hi = rangeOf("threads")
		result.Threads = &v45.SinfoThreads{Minimum: lo, Maximum: hi}
	}
	if !slices.ContainsFunc(r.Nodes, func(n *node) bool { return n.GresIssue != nil }) {
		result.GRES = &v45.SinfoGRES{Total: n.Gres, Used: n.GresUsed}
	}
	if !slices.ContainsFunc(r.Nodes, func(n *node) bool { return n.FreeMemory == nil }) {
		lo, hi := r.minmax("freemem")
		result.Memory.Free = &v45.SinfoMemoryFree{Minimum: v45.Numeric[uint64](lo, false, false), Maximum: v45.Numeric[uint64](hi, false, false)}
	}
	return result
}

func structuredPartition(p *partition) v45.PartitionInfo {
	configured := p.ConfiguredNodes
	if configured == "" {
		configured = utils.CompressHosts(p.Nodes)
	}
	tres := fmt.Sprintf("cpu=%d,mem=%s,node=%d,billing=%d", p.TotalCPUs, memoryTRES(p.TotalMemory), len(p.Nodes), p.TotalCPUs)
	result := v45.PartitionInfo{
		Nodes:    v45.PartitionNodes{Configured: configured, Total: v45.Int32(len(p.Nodes))},
		Accounts: v45.PartitionAccounts{Allowed: strings.Join(p.AllowedAccounts, ","), Deny: strings.Join(p.DeniedAccounts, ",")},
		TRES:     v45.PartitionTRES{Configured: tres}, CPUs: v45.PartitionCPUs{Total: v45.Int32(p.TotalCPUs)},
		Defaults:  v45.PartitionDefaults{PartitionMemoryPerCPU: v45.Numeric[uint64](p.DefaultMemCPU, p.DefaultMemCPU == 0, false), PartitionMemoryPerNode: v45.Numeric[uint64](p.DefaultMemNode, p.DefaultMemNode == 0, false)},
		Partition: v45.PartitionState{State: []string{strings.ToUpper(p.Available)}},
		Maximums:  v45.PartitionMaximums{Time: reportLimit(p.TimeLimit), PartitionMemoryPerCPU: v45.Numeric[uint64](p.MaxMemCPU, p.MaxMemCPU == 0, false), PartitionMemoryPerNode: v45.Numeric[uint64](p.MaxMemNode, p.MaxMemNode == 0, false)},
		Name:      p.Name, Priority: v45.PartitionPriority{JobFactor: v45.Int32(p.PriorityFactor)},
	}
	if p.PreemptMode == "OFF" {
		result.PreemptMode = []string{"DISABLED"}
	} else if p.PreemptMode != "" {
		result.PreemptMode = []string{p.PreemptMode}
	}
	if p.Default {
		result.Flags = append(result.Flags, "DEFAULT")
	}
	if p.Hidden {
		result.Flags = append(result.Flags, "HIDDEN")
	}
	if p.RootOnly {
		result.Flags = append(result.Flags, "ROOT_ONLY")
	}
	return result
}

func memoryTRES(mib uint64) string {
	// TRES starts in MiB (unlike the table's unit-less exact conversion).
	value, units := float64(mib), "MGTPE"
	i := 0
	for value >= 1024 && i < len(units)-1 {
		value /= 1024
		i++
	}
	if value == math.Trunc(value) {
		return fmt.Sprintf("%.0f%c", value, units[i])
	}
	return fmt.Sprintf("%.2f%c", value, units[i])
}

func renderStructured(w, diagnostics io.Writer, argv []string, rows []*row, snap *snapshot, o *plan, userName func(uint32) (string, bool)) error {
	if strings.EqualFold(o.DataParser, "list") {
		fmt.Fprintln(diagnostics, "Possible data_parser plugins:")
		_, err := io.WriteString(w, "data_parser/v0.0.45\n")
		return err
	}
	if _, err := io.WriteString(diagnostics, o.ParserWarnings); err != nil {
		return err
	}
	if o.Parser.SpecOnly {
		return utils.WriteSchema[v45.SinfoResponse](w, o.Serialization, o.Serializer)
	}
	entries := make([]v45.SinfoRecord, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, structuredRow(row, o, userName))
	}
	report := v45.SinfoResponse{Sinfo: entries, Meta: v45.NewMeta(argv, o.Parser.Plugin, snap.Cluster)}
	return utils.WriteReport(w, report, o.Serialization, o.Serializer, o.Parser.Complex)
}
