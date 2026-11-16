/**
 * Copyright (c) 2024 Peking University and Peking University
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
	"context"
	"fmt"
	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/util"
	"math"
	"strconv"
	"strings"
)

func nodeCommandParams(command *CControlCommand, allowed ...string) (map[string]string, error) {
	var params []*KeyValueParam
	switch cmd := command.Command.(type) {
	case CreateCommand:
		params = cmd.KeyValueParam
	case DeleteCommand:
		params = cmd.KeyValueParam
	}
	if command.GetID() != "" {
		return nil, util.NewCraneErr(util.ErrorCmdArg, "Specify nodes with NodeName=<hostlist>.")
	}
	values := make(map[string]string)
	for _, param := range params {
		key := strings.ToLower(param.Key)
		known := false
		for _, name := range allowed {
			if key == name {
				known = true
				break
			}
		}
		if !known {
			return nil, util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Unknown node attribute: %s", param.Key))
		}
		if _, exists := values[key]; exists {
			return nil, util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Duplicate node attribute: %s", param.Key))
		}
		value := unquoteIfQuoted(param.Value)
		if value == "" {
			return nil, util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Node attribute %s must not be empty.", param.Key))
		}
		values[key] = value
	}
	return values, nil
}

func parseNodeListAttribute(value, name string) ([]string, error) {
	values := strings.Split(value, ",")
	seen := make(map[string]bool)
	for _, item := range values {
		if item == "" || strings.ContainsAny(item, " \t\r\n") || seen[item] {
			return nil, util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid %s list: %s", name, value))
		}
		seen[item] = true
	}
	return values, nil
}

func executeCreateNodeCommand(command *CControlCommand) error {
	params, err := nodeCommandParams(command, "nodename", "state", "cpus", "realmemory", "partition", "sockets", "features")
	if err != nil {
		return err
	}
	for _, key := range []string{"nodename", "state", "cpus", "realmemory", "partition"} {
		if params[key] == "" {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Missing node attribute: %s", key))
		}
	}
	if !strings.EqualFold(params["state"], "FUTURE") {
		return util.NewCraneErr(util.ErrorCmdArg, "Only State=FUTURE is supported.")
	}
	names, err := expandHostlist(params["nodename"])
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}
	cpu, err := strconv.ParseUint(params["cpus"], 10, 32)
	if err != nil || cpu == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "CPUs must be a positive 32-bit integer.")
	}
	memory, err := util.ParseMemStringAsByte(params["realmemory"])
	if err != nil || memory == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "RealMemory must be a positive memory size (default unit: MiB).")
	}
	memoryValue := params["realmemory"]
	multiplier := float64(1024 * 1024)
	switch memoryValue[len(memoryValue)-1] {
	case 'B':
		multiplier = 1
	case 'K', 'k':
		multiplier = 1024
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
	}
	magnitude, err := strconv.ParseFloat(strings.TrimRight(memoryValue, "BKkMmGg"), 64)
	if err != nil || magnitude*multiplier >= math.Exp2(64) {
		return util.NewCraneErr(util.ErrorCmdArg, "RealMemory exceeds the maximum supported size.")
	}
	sockets := uint64(1)
	if value, ok := params["sockets"]; ok {
		sockets, err = strconv.ParseUint(value, 10, 32)
		if err != nil || sockets == 0 {
			return util.NewCraneErr(util.ErrorCmdArg, "Sockets must be a positive 32-bit integer.")
		}
	}
	if sockets > cpu || cpu%sockets != 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "CPUs must be divisible by Sockets.")
	}
	partitions, err := parseNodeListAttribute(params["partition"], "Partition")
	if err != nil {
		return err
	}
	var features []string
	if value, ok := params["features"]; ok {
		features, err = parseNodeListAttribute(value, "Features")
		if err != nil {
			return err
		}
	}
	req := &protos.CreateNodesRequest{Uid: userUid}
	for _, name := range names {
		req.Nodes = append(req.Nodes, &protos.DynamicNodeDefinition{
			Name: name, Cpu: uint32(cpu), MemoryBytes: memory,
			Sockets: uint32(sockets), Features: features, Partitions: partitions,
		})
	}
	reply, err := stub.CreateNodes(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to create nodes")
	}
	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotCreatedNodes) > 0 {
			return &util.CraneError{Code: util.ErrorBackend}
		}
		return nil
	}
	return summarizeNodeChanges("created", reply.CreatedNodes, reply.NotCreatedNodes, reply.NotCreatedReasons)
}

func executeDeleteNodeCommand(command *CControlCommand) error {
	params, err := nodeCommandParams(command, "nodename")
	if err != nil {
		return err
	}
	names, err := expandHostlist(params["nodename"])
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}
	reply, err := stub.DeleteNodes(context.Background(), &protos.DeleteNodesRequest{Uid: userUid, NodeNames: names})
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to delete nodes")
	}
	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotDeletedNodes) > 0 {
			return &util.CraneError{Code: util.ErrorBackend}
		}
		return nil
	}
	return summarizeNodeChanges("deleted", reply.DeletedNodes, reply.NotDeletedNodes, reply.NotDeletedReasons)
}

func summarizeNodeChanges(action string, changed, failed, reasons []string) error {
	for _, name := range changed {
		fmt.Printf("Node %s %s successfully.\n", name, action)
	}
	if len(failed) == 0 {
		return nil
	}
	var messages []string
	for i, name := range failed {
		messages = append(messages, fmt.Sprintf("Node %s was not %s: %s", name, action, reasons[i]))
	}
	return util.NewCraneErr(util.ErrorBackend, strings.Join(messages, "\n"))
}
