/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ccontrol

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"encoding/json"
	"fmt"
)

type slurmNodesOutput struct {
	Nodes []slurmNodeInfo `json:"nodes"`
}

type slurmNodeInfo struct {
	Hostname        string          `json:"hostname"`
	NodeHostname    string          `json:"node_hostname"`
	NodeAddr        string          `json:"node_addr"`
	State           string          `json:"state"`
	ResTotal        json.RawMessage `json:"res_total,omitempty"`
	ResAvail        json.RawMessage `json:"res_avail,omitempty"`
	ResAlloc        json.RawMessage `json:"res_alloc,omitempty"`
	PartitionNames  []string        `json:"partition_names"`
	RunningJobNum   uint32          `json:"running_job_num"`
	SlurmdVersion   string          `json:"slurmd_version"`
	SystemDesc      string          `json:"system_desc"`
	SlurmdStartTime json.RawMessage `json:"slurmd_start_time,omitempty"`
	SystemBootTime  json.RawMessage `json:"system_boot_time,omitempty"`
	LastBusyTime    json.RawMessage `json:"last_busy_time,omitempty"`
	NodeTopoInfo    json.RawMessage `json:"node_topo_info,omitempty"`
}

type slurmReservationsOutput struct {
	OK                  bool                   `json:"ok"`
	Reason              string                 `json:"reason"`
	ReservationInfoList []slurmReservationInfo `json:"reservation_info_list"`
}

type slurmReservationInfo struct {
	ReservationName string          `json:"reservation_name"`
	StartTime       json.RawMessage `json:"start_time,omitempty"`
	Duration        json.RawMessage `json:"duration,omitempty"`
	Partition       string          `json:"partition"`
	Nodes           string          `json:"nodes"`
	ResTotal        json.RawMessage `json:"res_total,omitempty"`
	ResAvail        json.RawMessage `json:"res_avail,omitempty"`
	ResAlloc        json.RawMessage `json:"res_alloc,omitempty"`
	AllowedAccounts []string        `json:"allowed_accounts"`
	DeniedAccounts  []string        `json:"denied_accounts"`
	AllowedUsers    []string        `json:"allowed_users"`
	DeniedUsers     []string        `json:"denied_users"`
}

type slurmTraceConfigOutput struct {
	OK          bool            `json:"ok"`
	Reason      string          `json:"reason"`
	Config      json.RawMessage `json:"config,omitempty"`
	FailedNodes []string        `json:"failed_nodes"`
}

func formatSlurmNodesJSON(reply *protos.QueryCranedInfoReply) (string, error) {
	output := slurmNodesOutput{Nodes: make([]slurmNodeInfo, 0, len(reply.CranedInfoList))}
	for _, node := range reply.CranedInfoList {
		if node == nil {
			return "", fmt.Errorf("node list contains a nil node")
		}
		item, err := newSlurmNodeInfo(node)
		if err != nil {
			return "", err
		}
		output.Nodes = append(output.Nodes, item)
	}
	return marshalSlurmControlOutput(output)
}

func newSlurmNodeInfo(node *protos.CranedInfo) (slurmNodeInfo, error) {
	resTotal, err := util.MarshalSlurmProtoField("node.res_total", node.ResTotal)
	if err != nil {
		return slurmNodeInfo{}, err
	}
	resAvail, err := util.MarshalSlurmProtoField("node.res_avail", node.ResAvail)
	if err != nil {
		return slurmNodeInfo{}, err
	}
	resAlloc, err := util.MarshalSlurmProtoField("node.res_alloc", node.ResAlloc)
	if err != nil {
		return slurmNodeInfo{}, err
	}
	slurmdStartTime, err := util.MarshalSlurmProtoField("node.slurmd_start_time", node.CranedStartTime)
	if err != nil {
		return slurmNodeInfo{}, err
	}
	systemBootTime, err := util.MarshalSlurmProtoField("node.system_boot_time", node.SystemBootTime)
	if err != nil {
		return slurmNodeInfo{}, err
	}
	lastBusyTime, err := util.MarshalSlurmProtoField("node.last_busy_time", node.LastBusyTime)
	if err != nil {
		return slurmNodeInfo{}, err
	}
	nodeTopoInfo, err := util.MarshalSlurmProtoField("node.node_topo_info", node.NodeTopoInfo)
	if err != nil {
		return slurmNodeInfo{}, err
	}

	return slurmNodeInfo{
		Hostname:        node.Hostname,
		NodeHostname:    node.NodeHostname,
		NodeAddr:        node.NodeAddr,
		State:           util.FormatSlurmNodeState(node.ResourceState, node.ControlState, node.PowerState),
		ResTotal:        resTotal,
		ResAvail:        resAvail,
		ResAlloc:        resAlloc,
		PartitionNames:  append([]string{}, node.PartitionNames...),
		RunningJobNum:   node.RunningJobNum,
		SlurmdVersion:   node.CranedVersion,
		SystemDesc:      node.SystemDesc,
		SlurmdStartTime: slurmdStartTime,
		SystemBootTime:  systemBootTime,
		LastBusyTime:    lastBusyTime,
		NodeTopoInfo:    nodeTopoInfo,
	}, nil
}

func formatSlurmReservationsJSON(reply *protos.QueryReservationInfoReply) (string, error) {
	output := slurmReservationsOutput{
		OK:                  reply.Ok,
		Reason:              reply.Reason,
		ReservationInfoList: make([]slurmReservationInfo, 0, len(reply.ReservationInfoList)),
	}
	for _, reservation := range reply.ReservationInfoList {
		if reservation == nil {
			return "", fmt.Errorf("reservation_info_list contains a nil reservation")
		}
		item, err := newSlurmReservationInfo(reservation)
		if err != nil {
			return "", err
		}
		output.ReservationInfoList = append(output.ReservationInfoList, item)
	}
	return marshalSlurmControlOutput(output)
}

func newSlurmReservationInfo(reservation *protos.ReservationInfo) (slurmReservationInfo, error) {
	startTime, err := util.MarshalSlurmProtoField("reservation.start_time", reservation.StartTime)
	if err != nil {
		return slurmReservationInfo{}, err
	}
	duration, err := util.MarshalSlurmProtoField("reservation.duration", reservation.Duration)
	if err != nil {
		return slurmReservationInfo{}, err
	}
	resTotal, err := util.MarshalSlurmProtoField("reservation.res_total", reservation.ResTotal)
	if err != nil {
		return slurmReservationInfo{}, err
	}
	resAvail, err := util.MarshalSlurmProtoField("reservation.res_avail", reservation.ResAvail)
	if err != nil {
		return slurmReservationInfo{}, err
	}
	resAlloc, err := util.MarshalSlurmProtoField("reservation.res_alloc", reservation.ResAlloc)
	if err != nil {
		return slurmReservationInfo{}, err
	}

	return slurmReservationInfo{
		ReservationName: reservation.ReservationName,
		StartTime:       startTime,
		Duration:        duration,
		Partition:       reservation.Partition,
		Nodes:           reservation.CranedRegex,
		ResTotal:        resTotal,
		ResAvail:        resAvail,
		ResAlloc:        resAlloc,
		AllowedAccounts: append([]string{}, reservation.AllowedAccounts...),
		DeniedAccounts:  append([]string{}, reservation.DeniedAccounts...),
		AllowedUsers:    append([]string{}, reservation.AllowedUsers...),
		DeniedUsers:     append([]string{}, reservation.DeniedUsers...),
	}, nil
}

func formatSlurmTraceConfigJSON(reply *protos.SetTraceConfigReply) (string, error) {
	config, err := util.MarshalSlurmProtoField("trace.config", reply.Config)
	if err != nil {
		return "", err
	}
	return marshalSlurmControlOutput(slurmTraceConfigOutput{
		OK:          reply.Ok,
		Reason:      reply.Reason,
		Config:      config,
		FailedNodes: append([]string{}, reply.FailedCranedIds...),
	})
}

func marshalSlurmControlOutput(output any) (string, error) {
	data, err := json.Marshal(output)
	if err != nil {
		return "", fmt.Errorf("failed to marshal Slurm control output: %w", err)
	}
	return string(data), nil
}
