// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"CraneFrontEnd/compat/protocol/v45"
	"CraneFrontEnd/compat/utils"
	"fmt"
	"io"
	"math"
	"os"
	"os/user"
	"sort"
	"strconv"
	"strings"
)

func parseSerializerEnvironment(env map[string]string) (map[string]utils.SerializerOptions, error) {
	settings := make(map[string]utils.SerializerOptions)
	for _, kind := range []string{"json", "yaml"} {
		flags := utils.SerializerOptions{Compact: env["SLURM_"+strings.ToUpper(kind)] != ""}
		for _, flag := range strings.Split(env["SLURM_"+strings.ToUpper(kind)], ",") {
			switch strings.ToLower(flag) {
			case "":
			case "compact":
				flags.Compact = true
			case "pretty":
				flags.Compact = false
			case "complex":
				flags.Complex = true
			case "no_tag":
				flags.NoTag = true
			default:
				return nil, &commandError{fmt.Sprintf("sinfo: fatal: Unable to parse serializer %q flags: Invalid argument\n", env["SLURM_"+strings.ToUpper(kind)])}
			}
		}
		settings[kind] = flags
	}
	return settings, nil
}

type schemaOptions struct {
	Complex  bool
	SpecOnly bool
	Plugin   string
}

func selectSchema(o *plan, diagnostics io.Writer) (schemaOptions, error) {
	version, flags, _ := strings.Cut(o.DataParser, "+")
	if !o.HasDataParser {
		version = "v0.0.45"
	}
	version = strings.TrimPrefix(version, "data_parser/")
	result := schemaOptions{Plugin: "data_parser/" + version}
	if strings.Contains(o.DataParser, "+") {
		result.Plugin += "+" + flags
	}
	if version != "v0.0.45" {
		return result, unsupported("--"+o.Serialization, fmt.Sprintf("data parser %q (supported: v0.0.45)", version))
	}
	for _, flag := range strings.Split(flags, "+") {
		switch strings.ToLower(flag) {
		case "", "fast", "minimize_refs":
		case "spec_only":
			result.SpecOnly = true
		case "complex":
			result.Complex = true
		case "prefer_refs":
			fmt.Fprintln(diagnostics, "sinfo: data_parser/v0.0.45 ignoring default flag FLAG_PREFER_REFS")
		default:
			fmt.Fprintf(diagnostics, "sinfo: warning: data_parser/v0.0.45 ignoring unknown flag %s\n", flag)
		}
	}
	return result, nil
}

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
	n := &node{}
	if len(r.Nodes) != 0 {
		n = r.Nodes[0]
	}
	nodes, cpus := r.counts()
	result := v45.SinfoRecord{
		Port:     v45.Int32(n.Port),
		Nodes:    v45.SinfoNodes{Allocated: v45.Int32(nodes.Alloc), Idle: v45.Int32(nodes.Idle), Other: v45.Int32(nodes.Other), Total: v45.Int32(nodes.Total), Nodes: orderedNames(r.names(""))},
		CPUs:     v45.SinfoCPUs{Allocated: v45.Int32(cpus.Alloc), Idle: v45.Int32(cpus.Idle), Other: v45.Int32(cpus.Other), Total: v45.Int32(cpus.Total)},
		Features: v45.SinfoFeatures{Total: n.Features, Active: n.ActiveFeatures},
		GRES:     v45.SinfoGRES{Total: n.Gres, Used: n.GresUsed},
		Cluster:  r.Cluster, Comment: n.Comment, Extra: n.Extra,
		Reason: v45.SinfoReason{Description: n.Reason}, Reservation: n.Reservation,
		Partition: structuredPartition(r.Partition),
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
	if len(r.Nodes) != 0 {
		result.Node.State = strings.Split(n.State.complete(), "+")
		result.Memory.Allocated = r.Nodes[len(r.Nodes)-1].AllocMemory
	}
	if !n.ReasonTime.IsZero() {
		result.Reason.Time = n.ReasonTime.Unix()
	}
	if n.ReasonUID != nil {
		name, ok := userName(*n.ReasonUID)
		if !ok {
			name = strconv.FormatUint(uint64(*n.ReasonUID), 10)
		}
		result.ReasonSetByUser = name
	}
	rangeOf := func(group string) (v45.Int32, v45.Int32) {
		lo, hi := r.minmax(group)
		return v45.Int32(lo), v45.Int32(hi)
	}
	result.CPUs.Minimum, result.CPUs.Maximum = rangeOf("cpus")
	result.CPUs.Load.Minimum, result.CPUs.Load.Maximum = rangeOf("load")
	result.CPUs.PerNode.Max = v45.Numeric[v45.Int32](r.Partition.MaxCPUs, false, r.Partition.MaxCPUs == 0xffffffff)
	result.Sockets.Minimum, result.Sockets.Maximum = rangeOf("sockets")
	result.Cores.Minimum, result.Cores.Maximum = rangeOf("cores")
	result.Threads.Minimum, result.Threads.Maximum = rangeOf("threads")
	result.Disk.Minimum, result.Disk.Maximum = rangeOf("disk")
	result.Weight.Minimum, result.Weight.Maximum = rangeOf("weight")
	result.Memory.Minimum, result.Memory.Maximum = r.minmax("memory")
	freeLo, freeHi := r.minmax("freemem")
	result.Memory.Free.Minimum = v45.Numeric[uint64](freeLo, freeLo == 0xfffffffffffffffe, freeLo == 0xffffffffffffffff)
	result.Memory.Free.Maximum = v45.Numeric[uint64](freeHi, freeHi == 0xfffffffffffffffe, freeHi == 0xffffffffffffffff)
	return result
}

func structuredPartition(p *partition) v45.PartitionInfo {
	allAsEmpty := func(v string) string {
		if v == "all" {
			return ""
		}
		return v
	}
	configured := p.ConfiguredNodes
	if configured == "" {
		configured = utils.CompressHosts(p.Nodes)
	}
	tres := p.TRES
	if tres == "" {
		tres = fmt.Sprintf("cpu=%d,mem=%s,node=%d,billing=%d", p.TotalCPUs, memoryTRES(p.TotalMemory), len(p.Nodes), p.TotalCPUs)
	}
	result := v45.PartitionInfo{
		Nodes:      v45.PartitionNodes{AllowedAllocation: allAsEmpty(p.AllocNodes), Configured: configured, Total: v45.Int32(len(p.Nodes))},
		Accounts:   v45.PartitionAccounts{Allowed: strings.Join(p.AllowedAccounts, ","), Deny: strings.Join(p.DeniedAccounts, ",")},
		Groups:     v45.PartitionGroups{Allowed: allAsEmpty(p.Groups)},
		QoS:        v45.PartitionQoS{Assigned: p.DefaultQoS},
		TRES:       v45.PartitionTRES{Configured: tres},
		SelectType: []string{"NONE"},
		CPUs:       v45.PartitionCPUs{Total: v45.Int32(p.TotalCPUs)},
		Defaults: v45.PartitionDefaults{
			PartitionMemoryPerCPU:  v45.Numeric[uint64](p.DefaultMemCPU, p.DefaultMemCPU == 0, false),
			PartitionMemoryPerNode: v45.Numeric[uint64](p.DefaultMemNode, p.DefaultMemNode == 0, false),
			Time:                   reportLimit(p.DefaultTime),
		},
		Partition: v45.PartitionState{Exclusive: "NO", Oversubscribe: p.OverSubscribe, State: []string{strings.ToUpper(p.Available)}},
		Maximums: v45.PartitionMaximums{
			CPUsPerNode:            v45.Numeric[v45.Int32](p.MaxCPUs, false, p.MaxCPUs == 0xffffffff),
			CPUsPerSocket:          v45.Numeric[v45.Int32](0, false, true),
			PartitionMemoryPerCPU:  v45.Numeric[uint64](p.MaxMemCPU, p.MaxMemCPU == 0, false),
			PartitionMemoryPerNode: v45.Numeric[uint64](p.MaxMemNode, p.MaxMemNode == 0, false),
			Nodes:                  v45.Numeric[v45.Int32](p.MaxNodes, false, p.MaxNodes == 0xffffffff),
			Time:                   reportLimit(p.TimeLimit), OverTimeLimit: v45.Numeric[v45.Int32](0, true, false),
		},
		Minimums: v45.PartitionMinimums{Nodes: v45.Int32(p.MinNodes)},
		Name:     p.Name, NodeSets: p.NodeSets, PreemptMode: []string{"CLUSTER_GLOBAL"},
		Priority:    v45.PartitionPriority{JobFactor: v45.Int32(p.PriorityFactor), Tier: v45.Int32(p.PriorityTier)},
		Timeouts:    v45.PartitionTimeouts{Resume: v45.Numeric[v45.Int32](0, true, false), Suspend: v45.Numeric[v45.Int32](0, true, false)},
		SuspendTime: v45.Numeric[v45.Int32](0, true, false),
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
	schema, err := selectSchema(o, diagnostics)
	if err != nil {
		return err
	}
	if schema.SpecOnly {
		return utils.WriteSchema[v45.SinfoResponse](w, o.Serialization, o.Serializer)
	}
	if err := validateStructuredProjection(rows, snap, o); err != nil {
		return err
	}
	entries := make([]v45.SinfoRecord, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, structuredRow(row, o, userName))
	}
	name, _ := userName(uint32(os.Getuid()))
	group := strconv.Itoa(os.Getgid())
	if g, err := user.LookupGroupId(group); err == nil {
		group = g.Name
	}
	report := v45.SinfoResponse{
		Sinfo: entries,
		Meta: v45.Meta{
			Plugin: v45.MetaPlugin{DataParser: schema.Plugin, AccountingStorage: snap.AccountingStorage},
			Client: v45.MetaClient{User: name, Group: group}, Command: argv,
			Slurm: v45.SlurmMeta{Version: v45.SlurmVersion{Major: "26", Micro: "4", Minor: "05"}, Release: compatibilityVersion, Cluster: snap.Cluster},
		},
	}
	return utils.WriteReport(w, report, o.Serialization, o.Serializer, schema.Complex)
}
