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

package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"encoding/json"
	"fmt"
	"strings"
)

type slurmClusterOutput struct {
	OK         bool                 `json:"ok"`
	Partitions []slurmPartitionInfo `json:"partitions"`
}

type slurmPartitionInfo struct {
	Name      string          `json:"name"`
	State     string          `json:"state"`
	NodeLists []slurmNodeList `json:"node_lists"`
}

type slurmNodeList struct {
	State    string `json:"state"`
	Count    uint32 `json:"count"`
	NodeList string `json:"node_list"`
	Reason   string `json:"reason,omitempty"`
}

func formatSlurmClusterJSON(reply *protos.QueryClusterInfoReply) (string, error) {
	output := slurmClusterOutput{
		OK:         reply.Ok,
		Partitions: make([]slurmPartitionInfo, 0, len(reply.Partitions)),
	}
	for _, part := range reply.Partitions {
		if part == nil {
			return "", fmt.Errorf("partition list contains a nil partition")
		}
	}
	groups := groupClusterNodes(reply)
	for _, partition := range reply.Partitions {
		item := slurmPartitionInfo{
			Name:      partition.Name,
			State:     strings.ToLower(strings.TrimPrefix(partition.State.String(), "PARTITION_")),
			NodeLists: make([]slurmNodeList, 0, len(groups[partition.Name])),
		}
		for _, nodes := range groups[partition.Name] {
			item.Name = nodes.PartitionName
			if !nodes.HasNodes {
				continue
			}
			item.NodeLists = append(item.NodeLists, slurmNodeList{
				State: util.FormatSlurmNodeState(
					nodes.ResourceState, nodes.ControlState, nodes.PowerState),
				Count:    uint32(nodes.CranedListCount),
				NodeList: nodes.CranedListRegex,
				Reason:   nodes.Reason,
			})
		}
		output.Partitions = append(output.Partitions, item)
	}

	data, err := json.Marshal(output)
	if err != nil {
		return "", fmt.Errorf("failed to marshal Slurm cluster output: %w", err)
	}
	return string(data), nil
}
