// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"CraneFrontEnd/compat/utils"
	"CraneFrontEnd/generated/protos"
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// query contains only facts needed by the controller and snapshot adapter.
type query struct{ ReservationsOnly bool }

type snapshotSource interface {
	Load(context.Context, query) (*snapshot, error)
	Close() error
}

type craneSource struct{ session *utils.Session }

func openCraneSource(ctx context.Context, configPath string) (snapshotSource, error) {
	session, err := utils.OpenSession(ctx, configPath)
	if err != nil {
		return nil, err
	}
	return &craneSource{session: session}, nil
}

func (s *craneSource) Load(ctx context.Context, q query) (*snapshot, error) {
	var reply *protos.QueryClusterInfoReply
	err := s.session.Query(ctx, func(ctx context.Context, client protos.CraneCtldClient, uid uint32) error {
		var err error
		// Adapt complete memberships against full partition totals before applying
		// Slurm filters and grouping. RPC name filters would leave partial facts.
		reply, err = client.QueryClusterInfo(ctx, &protos.QueryClusterInfoRequest{
			Uid: &uid, IncludeNodes: !q.ReservationsOnly, IncludePartitions: !q.ReservationsOnly,
			IncludeReservations: true, IncludeAllocationCleanup: !q.ReservationsOnly,
		})
		return err
	})
	if status.Code(err) == codes.Unimplemented {
		return nil, fmt.Errorf("Crane controller does not support the cluster query API")
	}
	if err != nil {
		return nil, err
	}
	return adaptSnapshot(reply, q)
}

func (s *craneSource) Close() error { return s.session.Close() }

func adaptSnapshot(reply *protos.QueryClusterInfoReply, q query) (*snapshot, error) {
	if reply == nil || !reply.Ok {
		return nil, fmt.Errorf("controller returned no successful cluster query reply")
	}
	s := &snapshot{Cluster: reply.ClusterName, UnsupportedFields: map[string]string{}}
	for _, field := range fieldSpecs {
		if field.Capability != "" {
			s.UnsupportedFields[field.Name] = field.Capability
		}
	}
	if reply.ObservedAt == nil || reply.ObservedAt.CheckValid() != nil {
		return nil, fmt.Errorf("controller returned no valid observation time")
	}
	now := reply.ObservedAt.AsTime()
	reservations := slices.Clone(reply.Reservations)
	for _, raw := range reservations {
		if raw == nil || raw.StartTime == nil || raw.Duration == nil || raw.StartTime.CheckValid() != nil || raw.Duration.CheckValid() != nil {
			return nil, fmt.Errorf("reservation %q is missing its time interval", raw.GetReservationName())
		}
		if raw.Duration.Seconds < 0 || raw.Duration.Nanos < 0 {
			return nil, fmt.Errorf("reservation %q has a negative duration", raw.ReservationName)
		}
		if raw.CreationOrder == 0 {
			return nil, fmt.Errorf("reservation %q is missing its creation order", raw.ReservationName)
		}
	}
	slices.SortStableFunc(reservations, func(a, b *protos.ReservationInfo) int { return cmp.Compare(a.CreationOrder, b.CreationOrder) })
	activeReservations := make(map[string][]string)
	for _, raw := range reservations {
		start := raw.StartTime.AsTime()
		end := start.Add(raw.Duration.AsDuration())
		s.Reservations = append(s.Reservations, reservation{Name: raw.ReservationName, Nodes: utils.CompressHosts(raw.AllocatedNodes), Start: start, End: end})
		if !now.Before(start) && now.Before(end) {
			for _, name := range raw.AllocatedNodes {
				activeReservations[name] = append(activeReservations[name], raw.ReservationName)
			}
		}
	}
	if q.ReservationsOnly {
		return s, nil
	}
	partitionNodes := make(map[string][]int, len(reply.Partitions))
	for index, detail := range reply.Nodes {
		if detail == nil || detail.ResTotal == nil || detail.ResAlloc == nil {
			return nil, fmt.Errorf("controller returned an incomplete node record")
		}
		cpus, err := wholeCPUs(detail.ResTotal.CpuCount, detail.Hostname)
		if err != nil {
			return nil, err
		}
		allocation, err := activeAllocation(detail)
		if err != nil {
			return nil, err
		}
		allocated := allocation.CpuCount
		if math.IsNaN(allocated) || math.IsInf(allocated, 0) || allocated < 0 || allocated > float64(cpus) {
			return nil, fmt.Errorf("node %q has an invalid allocated CPU count", detail.Hostname)
		}
		n := node{
			Name: detail.Hostname, Hostname: detail.NodeHostname, Address: detail.NodeAddr, Version: detail.CranedVersion,
			CPUs: cpus, AllocCPUs: uint64(allocated), FractionalAllocation: allocated != math.Trunc(allocated), HasCPUAllocation: allocated > 0,
			Memory: detail.ResTotal.MemoryBytes / (1024 * 1024), AllocMemory: allocation.MemoryBytes / (1024 * 1024),
			Port: uint64(detail.Port), Reason: detail.StateReason, ReasonUID: detail.StateReasonUid,
			TelemetryExpected: detail.HasRegistered,
		}
		gresIssue := subtractCleanupGRES(allocation.Gres, detail.CleanupResources.Gres)
		n.Gres, n.GresUsed, n.GresIssue = projectGRES(detail.ResTotal.Gres, allocation.Gres, detail.GresOrder)
		if gresIssue != nil {
			n.GresIssue = gresIssue
		}
		if !detail.Responding {
			zero := uint64(0)
			n.CPULoad = &zero
		} else if detail.CpuLoad != nil {
			value := uint64(*detail.CpuLoad)
			n.CPULoad = &value
		}
		n.FreeMemory = detail.FreeMemoryMb
		if detail.StateReasonTime != nil {
			if err := detail.StateReasonTime.CheckValid(); err != nil {
				return nil, fmt.Errorf("node %q has an invalid state reason time: %w", n.Name, err)
			}
			n.ReasonTime = detail.StateReasonTime.AsTime()
		}
		if outage := detail.Outage; outage != nil {
			if outage.Time == nil || outage.Time.CheckValid() != nil {
				return nil, fmt.Errorf("node %q has an incomplete outage event", n.Name)
			}
			if n.Reason == "" {
				n.Reason = "Not responding"
			} else if !strings.Contains(n.Reason, " : Not responding") {
				n.Reason += " : Not responding"
			}
			uid := outage.Uid
			n.ReasonUID, n.ReasonTime = &uid, outage.Time.AsTime()
		}
		// Crane schedules logical CPUs without a separate SMT policy. Its
		// configured capacity is distinct from the daemon's physical topology.
		if sockets := uint64(detail.ConfiguredSockets); sockets != 0 && cpus != 0 && cpus%sockets == 0 {
			n.Sockets, n.Cores, n.Threads = sockets, cpus/sockets, 1
		}
		n.State = nodeState{Base: "idle", NoRespond: !detail.Responding, Drain: detail.ControlState == protos.CranedControlState_CRANE_DRAIN}
		switch {
		case !detail.Responding && !detail.HasRegistered:
			n.State.Base = "unknown"
		case !detail.Responding:
			n.State.Base = "down"
		case allocated >= float64(cpus) && cpus != 0:
			n.State.Base = "allocated"
		case allocated > 0:
			n.State.Base = "mixed"
		}
		switch detail.PowerState {
		case protos.CranedPowerState_CRANE_POWER_SLEEPING, protos.CranedPowerState_CRANE_POWER_POWEREDOFF:
			n.State.Power = "powered_down"
		case protos.CranedPowerState_CRANE_POWER_TO_SLEEPING, protos.CranedPowerState_CRANE_POWER_POWERING_OFF:
			n.State.Power = "powering_down"
		case protos.CranedPowerState_CRANE_POWER_WAKING_UP, protos.CranedPowerState_CRANE_POWER_POWERING_ON:
			n.State.Power = "powering_up"
		}
		if n.State.Power != "" {
			// A successful suspend/resume transition is expected to stop
			// heartbeats. Crane has no separate administrative DOWN state.
			n.State.NoRespond = n.State.Power == "powering_up" && !detail.Responding
			n.State.Base = "idle"
			if allocated >= float64(cpus) && cpus != 0 {
				n.State.Base = "allocated"
			} else if allocated > 0 {
				n.State.Base = "mixed"
			}
		}
		n.State.Completing = detail.CleanupPending && detail.Responding && n.State.Power != "powered_down" && n.State.Power != "powering_up"
		if names := activeReservations[n.Name]; len(names) != 0 {
			// Crane excludes overlapping whole-node reservations. The Slurm
			// node record exposes one name, not a fabricated comma-separated list.
			if len(names) != 1 {
				return nil, fmt.Errorf("node %q has overlapping reservations without an ordered reservation projection", n.Name)
			}
			n.Reservation = names[0]
			n.State.Reserved = true
		}
		s.Nodes = append(s.Nodes, n)
		for _, name := range detail.PartitionNames {
			partitionNodes[name] = append(partitionNodes[name], index)
		}
	}
	for _, detail := range reply.Partitions {
		if detail == nil {
			return nil, fmt.Errorf("controller returned an incomplete partition record")
		}
		p := partition{
			Name: detail.Name, Default: detail.IsDefault, Available: "up", TimeLimit: durationLimit{Infinite: true},
			DefaultTime: durationLimit{Unset: true}, PriorityFactor: uint64(detail.Priority),
			Groups: "all", AllocNodes: "all", OverSubscribe: "NO", MaxNodes: 0xffffffff, MaxCPUs: 0xffffffff, MinNodes: 1,
			AllowedAccounts: slices.Clone(detail.AllowedAccounts), DeniedAccounts: slices.Clone(detail.DeniedAccounts),
			DefaultMemCPU: detail.DefaultMemPerCpu / (1024 * 1024), MaxMemCPU: detail.MaxMemPerCpu / (1024 * 1024),
			DefaultMemNode: detail.DefaultMemPerNode / (1024 * 1024), MaxMemNode: detail.MaxMemPerNode / (1024 * 1024),
			ConfiguredNodes: detail.Hostlist,
		}
		slices.Sort(p.AllowedAccounts)
		slices.Sort(p.DeniedAccounts)
		// Crane currently has no partition administrative-down or partition
		// time-limit setting. Node liveness and job/QoS limits do not substitute.
		mode, ok := map[protos.PreemptMode]string{
			protos.PreemptMode_PREEMPT_MODE_OFF: "OFF", protos.PreemptMode_PREEMPT_MODE_CANCEL: "CANCEL",
			protos.PreemptMode_PREEMPT_MODE_REQUEUE: "REQUEUE", protos.PreemptMode_PREEMPT_MODE_SUSPEND: "SUSPEND",
		}[detail.PreemptMode]
		if !ok {
			return nil, fmt.Errorf("partition %q has an unknown preemption mode", p.Name)
		}
		p.PreemptMode = mode
		for _, index := range partitionNodes[p.Name] {
			n := s.Nodes[index]
			p.Nodes = append(p.Nodes, n.Name)
			p.TotalCPUs += n.CPUs
			p.TotalMemory += n.Memory
		}
		if len(p.Nodes) != int(detail.TotalNodes) {
			return nil, fmt.Errorf("partition %q references nodes missing from the controller snapshot", p.Name)
		}
		s.Partitions = append(s.Partitions, p)
	}
	return s, nil
}

// The controller reads held resources and ending allocations under the same
// locks. Only the client excludes cleanup holdings from Slurm's active totals.
func activeAllocation(node *protos.CranedInfo) (*protos.ResourceInNodeV3, error) {
	cleanup := node.CleanupResources
	if cleanup == nil {
		return nil, fmt.Errorf("node %q is missing allocation cleanup data", node.Hostname)
	}
	held := node.ResAlloc
	if math.IsNaN(held.CpuCount) || math.IsInf(held.CpuCount, 0) || math.IsNaN(cleanup.CpuCount) || math.IsInf(cleanup.CpuCount, 0) || cleanup.CpuCount < 0 || cleanup.CpuCount > held.CpuCount || cleanup.MemoryBytes > held.MemoryBytes || cleanup.MemorySwBytes > held.MemorySwBytes {
		return nil, fmt.Errorf("node %q has inconsistent allocation cleanup resources", node.Hostname)
	}
	active := proto.Clone(held).(*protos.ResourceInNodeV3)
	// State selection and fractional-capability checks need scheduler precision.
	active.CpuCount = held.CpuCount - cleanup.CpuCount
	active.MemoryBytes -= cleanup.MemoryBytes
	active.MemorySwBytes -= cleanup.MemorySwBytes
	return active, nil
}

// Mutates only the client's cloned holdings. GRES errors remain field-specific,
// so a malformed device inventory does not prevent queries for node names.
func subtractCleanupGRES(held, cleanup *protos.DedicatedResourceInNode) error {
	for name, types := range cleanup.GetNameTypeMap() {
		for kind, slots := range types.GetTypeSlotsMap() {
			allocated := held.GetNameTypeMap()[name].GetTypeSlotsMap()[kind]
			remove := make(map[string]bool, len(slots.GetSlots()))
			for _, slot := range slots.GetSlots() {
				if remove[slot] {
					return fmt.Errorf("cleanup GRES %s:%s contains duplicate device %q", name, kind, slot)
				}
				remove[slot] = true
			}
			var remaining []string
			for _, slot := range allocated.GetSlots() {
				if remove[slot] {
					delete(remove, slot)
				} else {
					remaining = append(remaining, slot)
				}
			}
			if len(remove) != 0 {
				return fmt.Errorf("cleanup GRES %s:%s contains unallocated devices", name, kind)
			}
			if allocated != nil {
				allocated.Slots = remaining
			}
		}
	}
	return nil
}

func wholeCPUs(value float64, name string) (uint64, error) {
	if value < 0 || value > math.MaxUint32 || math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value {
		return 0, fmt.Errorf("node %q CPU count cannot be represented by Slurm sinfo: %g", name, value)
	}
	return uint64(value), nil
}

type gresDevice struct{ kind, path string }

// The resource order comes from the controller's configuration, independently
// of protobuf map iteration. GPU devices use natural file order, as in Slurm's
// file-backed GPU configuration without Links/AutoDetect. Other file-backed
// resources retain declaration order. Crane has no GPU topology/Links policy.
func projectGRES(total, allocated *protos.DedicatedResourceInNode, order []*protos.GresResource) (string, string, error) {
	names := []string{}
	devices := map[string][]gresDevice{}
	declared := map[string]map[string]string{}
	for _, resource := range order {
		if resource == nil || resource.Name == "" {
			return "", "", fmt.Errorf("controller returned an invalid GRES declaration")
		}
		name := resource.Name
		if declared[name] == nil {
			names = append(names, name)
			declared[name] = map[string]string{}
		}
		for _, path := range resource.Devices {
			if kind, found := declared[name][path]; found {
				if kind != resource.Type {
					return "", "", fmt.Errorf("GRES %s device %q has conflicting types", name, path)
				}
				continue
			}
			declared[name][path] = resource.Type
			devices[name] = append(devices[name], gresDevice{resource.Type, path})
		}
	}
	inventory := map[string]map[string]string{}
	for name, types := range total.GetNameTypeMap() {
		inventory[name] = map[string]string{}
		for kind, slots := range types.GetTypeSlotsMap() {
			for _, slot := range slots.GetSlots() {
				if _, exists := inventory[name][slot]; exists {
					return "", "", fmt.Errorf("GRES %s has duplicate device %q", name, slot)
				}
				if declaredKind, found := declared[name][slot]; !found || declaredKind != kind {
					return "", "", fmt.Errorf("GRES %s:%s device %q is missing its configuration order", name, kind, slot)
				}
				inventory[name][slot] = kind
			}
		}
	}
	used := map[string]map[string]bool{}
	for name, types := range allocated.GetNameTypeMap() {
		used[name] = map[string]bool{}
		for kind, slots := range types.GetTypeSlotsMap() {
			for _, slot := range slots.GetSlots() {
				actualKind, found := inventory[name][slot]
				if !found || actualKind != kind || used[name][slot] {
					return "", "", fmt.Errorf("allocated GRES %s:%s contains an unknown or duplicate device %q", name, kind, slot)
				}
				used[name][slot] = true
			}
		}
	}
	var configured, consumption []string
	for _, name := range names {
		if len(inventory[name]) == 0 {
			continue
		}
		if name == "mps" || name == "shard" {
			return "", "", fmt.Errorf("current Crane version does not support Slurm shared GRES %q accounting", name)
		}
		ordered := devices[name]
		if name == "gpu" {
			slices.SortStableFunc(ordered, func(a, b gresDevice) int {
				if utils.NaturalLess(a.path, b.path) {
					return -1
				}
				if utils.NaturalLess(b.path, a.path) {
					return 1
				}
				return 0
			})
		}
		kinds := []string{}
		counts := map[string]uint64{}
		indices := map[string][]int{}
		index := 0
		for _, device := range ordered {
			if _, found := inventory[name][device.path]; !found {
				continue
			}
			if counts[device.kind] == 0 {
				kinds = append(kinds, device.kind)
			}
			counts[device.kind]++
			if used[name][device.path] {
				indices[device.kind] = append(indices[device.kind], index)
			}
			index++
		}
		for _, kind := range kinds {
			prefix := name
			if kind != "" {
				prefix += ":" + kind
			}
			count, unit := counts[kind], ""
			for _, suffix := range "KMGTP" {
				if count == 0 || count%1024 != 0 {
					break
				}
				count /= 1024
				unit = string(suffix)
			}
			configured = append(configured, fmt.Sprintf("%s:%d%s", prefix, count, unit))
			consumption = append(consumption, fmt.Sprintf("%s:%d(IDX:%s)", prefix, len(indices[kind]), gresIndices(indices[kind])))
		}
	}
	return strings.Join(configured, ","), strings.Join(consumption, ","), nil
}

func gresIndices(indices []int) string {
	if len(indices) == 0 {
		return "N/A"
	}
	var ranges []string
	for start := 0; start < len(indices); {
		end := start + 1
		for end < len(indices) && indices[end] == indices[end-1]+1 {
			end++
		}
		value := fmt.Sprint(indices[start])
		if end > start+1 {
			value += fmt.Sprint("-", indices[end-1])
		}
		ranges = append(ranges, value)
		start = end
	}
	value := strings.Join(ranges, ",")
	// Slurm exposes a 64-byte bit-format buffer, including the terminator.
	if len(value) > 63 {
		value = value[:63]
	}
	return value
}
