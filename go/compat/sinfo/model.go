// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"CraneFrontEnd/compat/utils"
	"cmp"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The adapter supplies scheduler facts, in controller declaration order.
// This model has no RPC or presentation types. In particular, administrative
// availability and time limits belong to a partition, not its live nodes or QoS.
type snapshot struct {
	Cluster           string
	AccountingStorage string // empty when no Slurm accounting-storage plugin is in use.
	Nodes             []node
	Partitions        []partition
	Reservations      []reservation
	// Product features absent from the source, distinct from a configured empty
	// value. Complete structured schemas must validate their implicit fields.
	UnsupportedFields map[string]string
}

type node struct {
	Name, Address, Hostname, Version                         string
	State                                                    nodeState
	CPUs, AllocCPUs, Memory, AllocMemory                     uint64
	FractionalAllocation, HasCPUAllocation                   bool
	Sockets, Cores, Threads, Disk, Weight, Port              uint64
	CPULoad, FreeMemory                                      *uint64 // hundredths of load; MiB, respectively
	TelemetryExpected                                        bool
	Features, ActiveFeatures, Gres, GresUsed, Comment, Extra string
	GresIssue                                                error
	Reason, Reservation                                      string
	ReasonTime                                               time.Time
	ReasonUID                                                *uint32
}

type partition struct {
	Name                                                 string
	Nodes                                                []string
	Default, Hidden, RootOnly                            bool
	Available                                            string
	TimeLimit, DefaultTime                               durationLimit
	MinNodes, MaxNodes, MaxCPUs                          uint64
	Groups, AllocNodes, OverSubscribe, PreemptMode       string
	PriorityFactor, PriorityTier                         uint64
	AllowedAccounts, DeniedAccounts                      []string
	DefaultMemCPU, MaxMemCPU, DefaultMemNode, MaxMemNode uint64 // MiB; zero is unconfigured.
	TotalCPUs, TotalMemory                               uint64
	ConfiguredNodes, NodeSets, TRES                      string
	DefaultQoS                                           string
}

type durationLimit struct {
	Seconds         uint64
	Infinite, Unset bool
}

type reservation struct {
	Name, Nodes string
	Start, End  time.Time
}

type row struct {
	Partition      *partition
	PartitionOrder int
	Nodes          []*node
	seen           map[string]bool
	Cluster        string
}

func (r *row) add(n *node) {
	if r.seen == nil {
		r.seen = make(map[string]bool)
	}
	if !r.seen[n.Name] {
		r.Nodes = append(r.Nodes, n)
		r.seen[n.Name] = true
	}
}

// Only requested grouping dimensions enter the key. Encoding each component
// independently prevents delimiters in a node reason from joining groups.
func groupingKey(p *partition, n *node, fields []formatItem, o *plan) string {
	var key strings.Builder
	for _, item := range fields {
		if item.Field == nil || item.Field.Group == "" {
			continue
		}
		group := item.Field.Group
		v, isPartition := p.groupValue(group)
		if isPartition {
			if !o.Reasons {
				fmt.Fprintf(&key, "%s=%q;", group, v)
			}
			continue
		}
		if n == nil {
			continue
		}
		if item.Field.Numeric && group != "allocmem" && !o.Exact {
			continue
		}
		if (group == "version" || group == "port") && !o.Exact {
			continue
		}
		fmt.Fprintf(&key, "%s=%q;", group, n.groupValue(group))
	}
	return key.String()
}

func buildRows(s *snapshot, o *plan) []*row {
	cluster := ""
	if o.Federation && !o.Local {
		cluster = s.Cluster
	}
	byName := make(map[string]*node, len(s.Nodes))
	for i := range s.Nodes {
		byName[s.Nodes[i].Name] = &s.Nodes[i]
	}
	partFilter := make(map[string]bool)
	if o.HasPartition {
		for _, p := range strings.Split(o.Partition, ",") {
			if p != "" {
				partFilter[p] = true
			}
		}
	}
	partField := false
	for _, f := range o.Fields {
		partField = partField || f.Field != nil && f.Field.Group == "partition"
	}
	var rows []*row
	groups := make(map[string]*row)
	empty := make(map[string][]*row)
	selected := func(p *partition) bool {
		return (!p.Hidden || o.All) && (!o.HasPartition || partFilter[p.Name])
	}
	if partField && !o.Node {
		for i := range s.Partitions {
			p := &s.Partitions[i]
			if !selected(p) {
				continue
			}
			r := &row{Partition: p, PartitionOrder: i, Cluster: cluster}
			rows = append(rows, r)
			k := groupingKey(p, nil, o.Fields, o)
			empty[k] = append(empty[k], r)
		}
	}
	for i := range s.Partitions {
		p := &s.Partitions[i]
		if !selected(p) {
			continue
		}
		for _, name := range p.Nodes {
			n := byName[name]
			if n == nil || o.NodeFilter != nil && !o.NodeFilter.Match(name) ||
				o.Dead && !n.State.NoRespond || o.Responding && n.State.NoRespond ||
				!o.StateFilter.match(n.State, n.allocatedSignal()) {
				continue
			}
			key := groupingKey(p, n, o.Fields, o)
			r := groups[key]
			if o.Node {
				r = nil
			}
			if r == nil {
				pk := groupingKey(p, nil, o.Fields, o)
				if candidates := empty[pk]; !o.Node && len(candidates) != 0 {
					r = candidates[0]
					empty[pk] = candidates[1:]
				} else {
					r = &row{Partition: p, PartitionOrder: i, Cluster: cluster}
					rows = append(rows, r)
				}
				if !o.Node {
					groups[key] = r
				}
			}
			r.add(n)
		}
	}
	return rows
}

func (p *partition) groupValue(group string) (string, bool) {
	if p == nil {
		return "", false
	}
	var value any
	switch group {
	case "partition":
		value = p.Name
	case "available":
		value = p.Available
	case "time":
		value = p.TimeLimit
	case "defaulttime":
		value = p.DefaultTime
	case "size":
		value = [2]uint64{p.MinNodes, p.MaxNodes}
	case "maxcpus":
		value = p.MaxCPUs
	case "groups":
		value = p.Groups
	case "root":
		value = p.RootOnly
	case "oversubscribe":
		value = p.OverSubscribe
	case "preempt":
		value = p.PreemptMode
	case "prioritytier":
		value = p.PriorityTier
	case "priorityfactor":
		value = p.PriorityFactor
	default:
		return "", false
	}
	return fmt.Sprint(value), true
}

func (n *node) allocatedSignal() uint64 {
	if n.HasCPUAllocation {
		return 1
	}
	return n.AllocCPUs
}

func (n *node) groupValue(group string) string {
	switch group {
	case "state":
		return n.State.long()
	case "statecomplete":
		return n.State.complete()
	case "address":
		return n.Address
	case "hostname":
		return n.Hostname
	case "version":
		return n.Version
	case "topology":
		return fmt.Sprint([3]uint64{n.Sockets, n.Cores, n.Threads})
	case "reason":
		return n.Reason
	case "reasontime":
		return strconv.FormatInt(n.ReasonTime.Unix(), 10)
	case "reasonuser":
		if n.ReasonUID == nil {
			return "unset"
		}
		return fmt.Sprint(*n.ReasonUID)
	case "reservation":
		return n.Reservation
	case "features":
		return n.Features
	case "activefeatures":
		return n.ActiveFeatures
	case "gres":
		return n.Gres
	case "gresused":
		return n.GresUsed
	case "comment":
		return n.Comment
	case "extra":
		return n.Extra
	default:
		return strconv.FormatUint(n.number(group), 10)
	}
}

func (n *node) number(group string) uint64 {
	switch group {
	case "cpus":
		return n.CPUs
	case "sockets":
		return n.Sockets
	case "cores":
		return n.Cores
	case "threads":
		return n.Threads
	case "disk":
		return n.Disk
	case "memory":
		return n.Memory
	case "allocmem":
		return n.AllocMemory
	case "weight":
		return n.Weight
	case "port":
		return n.Port
	case "load":
		if n.CPULoad != nil {
			return *n.CPULoad
		}
		return 0xfffffffe
	case "freemem":
		if n.FreeMemory != nil {
			return *n.FreeMemory
		}
		return 0xfffffffffffffffe
	}
	return 0
}

func (r *row) minmax(group string) (uint64, uint64) {
	if len(r.Nodes) == 0 {
		return 0, 0
	}
	lo, hi := r.Nodes[0].number(group), r.Nodes[0].number(group)
	for _, n := range r.Nodes[1:] {
		value := n.number(group)
		lo = min(lo, value)
		hi = max(hi, value)
	}
	return lo, hi
}

type counts struct{ Alloc, Idle, Other, Total uint64 }

func (c counts) String() string { return fmt.Sprintf("%d/%d/%d/%d", c.Alloc, c.Idle, c.Other, c.Total) }
func (r *row) counts() (nodes, cpus counts) {
	for _, n := range r.Nodes {
		nodes.Total++
		unavailable := n.State.Drain || n.State.Base == "down"
		if n.State.Base == "allocated" || n.State.Base == "mixed" || n.State.Completing {
			nodes.Alloc++
		} else if unavailable {
			nodes.Other++
		} else {
			nodes.Idle++
		}
		cpus.Total += n.CPUs
		cpus.Alloc += n.AllocCPUs
		if unavailable {
			cpus.Other += n.CPUs - n.AllocCPUs
		} else {
			cpus.Idle += n.CPUs - n.AllocCPUs
		}
	}
	return
}

func (r *row) names(kind string) []string {
	var names []string
	for _, n := range r.Nodes {
		switch kind {
		case "hostname":
			names = append(names, n.Hostname)
		case "address":
			names = append(names, n.Address)
		default:
			names = append(names, n.Name)
		}
	}
	return names
}

func compareName(a, b string) int {
	if a == b {
		return 0
	}
	if utils.HostLess(a, b) {
		return -1
	}
	return 1
}

func (r *row) compareNumber(s *row, key string) int {
	a, _ := r.minmax(key)
	b, _ := s.minmax(key)
	return cmp.Compare(a, b)
}

func sortRows(rows []*row, o *plan) {
	// Slurm only populates hostnames and addresses when those output fields
	// are requested. Sorting on an unpopulated column is a stable no-op.
	collected := make(map[byte]bool)
	for _, item := range o.Fields {
		if item.Field != nil {
			collected[item.Field.Short] = true
		}
	}
	keys := o.Sort
	if !o.HasSort {
		keys = "#P,-t"
		if o.Node {
			keys = "N"
		}
	}
	for i := len(keys) - 1; i >= 0; i-- {
		key := keys[i]
		if (key == 'n' || key == 'o') && !collected[key] {
			continue
		}
		if !strings.ContainsRune("aAbcdDEfFghHlmMnoNOepPrRsStTuUVwXYZz", rune(key)) {
			continue
		}
		reverse, partOrder := i > 0 && keys[i-1] == '-', i > 0 && keys[i-1] == '#'
		sort.SliceStable(rows, func(a, b int) bool {
			diff := compareRows(rows[a], rows[b], key, partOrder)
			if reverse {
				return diff > 0
			}
			return diff < 0
		})
	}
}

func compareRows(a, b *row, key byte, partOrder bool) int {
	pa, pb := a.Partition, b.Partition
	na, nb := &node{}, &node{}
	if len(a.Nodes) != 0 {
		na = a.Nodes[0]
	}
	if len(b.Nodes) != 0 {
		nb = b.Nodes[0]
	}
	if group, ok := map[byte]string{'c': "cpus", 'd': "disk", 'm': "memory", 'O': "load", 'e': "freemem", 'w': "weight", 'X': "sockets", 'Y': "cores", 'Z': "threads"}[key]; ok {
		return a.compareNumber(b, group)
	}
	switch key {
	case 'P', 'R':
		if partOrder {
			return cmp.Compare(a.PartitionOrder, b.PartitionOrder)
		}
		return strings.Compare(pa.Name, pb.Name)
	case 'a':
		states := map[string]int{"inactive": 0, "down": 1, "drain": 2, "up": 3}
		return cmp.Compare(states[pa.Available], states[pb.Available])
	case 'A', 'F':
		ca, _ := a.counts()
		cb, _ := b.counts()
		return cmp.Compare(ca.Alloc, cb.Alloc)
	case 'D':
		return cmp.Compare(len(a.Nodes), len(b.Nodes))
	case 'b':
		return strings.Compare(na.ActiveFeatures, nb.ActiveFeatures)
	case 'E':
		return strings.Compare(na.Reason, nb.Reason)
	case 'f':
		return strings.Compare(na.Features, nb.Features)
	case 'g':
		return strings.Compare(pa.Groups, pb.Groups)
	case 'h':
		return cmp.Compare(shareOrder(pa.OverSubscribe), shareOrder(pb.OverSubscribe))
	case 'H':
		return na.ReasonTime.Compare(nb.ReasonTime)
	case 'l':
		value := func(limit durationLimit) uint64 {
			if limit.Infinite {
				return ^uint64(0)
			}
			return limit.Seconds
		}
		return cmp.Compare(value(pa.TimeLimit), value(pb.TimeLimit))
	case 'M':
		order := map[string]uint16{"OFF": 0, "SUSPEND": 1, "REQUEUE": 2, "CANCEL": 8, "GANG": 0x8000}
		return cmp.Compare(order[pa.PreemptMode], order[pb.PreemptMode])
	case 'N', 'n', 'o':
		kind := ""
		if key == 'n' {
			kind = "hostname"
		}
		if key == 'o' {
			kind = "address"
		}
		first := func(r *row) string {
			names := r.names(kind)
			if len(names) == 0 {
				return ""
			}
			n := names[0]
			for _, name := range names[1:] {
				if utils.HostLess(name, n) {
					n = name
				}
			}
			return n
		}
		return compareName(first(a), first(b))
	case 'p':
		return cmp.Compare(pa.PriorityTier, pb.PriorityTier)
	case 'r':
		if pa.RootOnly == pb.RootOnly {
			return 0
		}
		if pa.RootOnly {
			return 1
		}
		return -1
	case 's':
		size := func(p *partition) uint32 {
			if p.MaxNodes == 0xffffffff {
				return 0xffffffff
			}
			return uint32(p.MaxNodes + p.MinNodes)
		}
		return cmp.Compare(size(pa), size(pb))
	case 'S':
		return cmp.Compare(pa.PriorityFactor, pb.PriorityFactor)
	case 't', 'T':
		return cmp.Compare(na.State.order(), nb.State.order())
	case 'u', 'U':
		value := func(uid *uint32) uint32 {
			if uid == nil {
				return 0xfffffffe
			}
			return *uid
		}
		return cmp.Compare(value(na.ReasonUID), value(nb.ReasonUID))
	case 'V':
		return strings.Compare(a.Cluster, b.Cluster)
	case 'z':
		for _, group := range []string{"sockets", "cores", "threads"} {
			if diff := a.compareNumber(b, group); diff != 0 {
				return diff
			}
		}
	}
	return 0
}

func shareOrder(value string) uint16 {
	switch value {
	case "EXCLUSIVE":
		return 0
	case "NO":
		return 1
	}
	name, count, _ := strings.Cut(value, ":")
	n := uint16(decimalPrefix(count))
	if name == "FORCE" {
		n |= 0x8000
	}
	return n
}
