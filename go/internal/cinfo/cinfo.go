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

package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	resourceStateMap = map[string]protos.CranedResourceState{
		"idle":  protos.CranedResourceState_CRANE_IDLE,
		"mix":   protos.CranedResourceState_CRANE_MIX,
		"alloc": protos.CranedResourceState_CRANE_ALLOC,
		"down":  protos.CranedResourceState_CRANE_DOWN,
	}

	controlStateMap = map[string]protos.CranedControlState{
		"none":  protos.CranedControlState_CRANE_NONE,
		"drain": protos.CranedControlState_CRANE_DRAIN,
	}

	powerStateMap = map[string]protos.CranedPowerState{
		"active":     protos.CranedPowerState_CRANE_POWER_ACTIVE,
		"power-idle": protos.CranedPowerState_CRANE_POWER_IDLE,
		"sleeping":   protos.CranedPowerState_CRANE_POWER_SLEEPING,
		"poweredoff": protos.CranedPowerState_CRANE_POWER_POWEREDOFF,
		"off":        protos.CranedPowerState_CRANE_POWER_POWEREDOFF,
		"to-sleep":   protos.CranedPowerState_CRANE_POWER_TO_SLEEPING,
		"waking":     protos.CranedPowerState_CRANE_POWER_WAKING_UP,
		"oning":      protos.CranedPowerState_CRANE_POWER_POWERING_ON,
		"offing":     protos.CranedPowerState_CRANE_POWER_POWERING_OFF,
	}
)

func ConvertStates[T comparable](stateMap map[string]T, stateType string) ([]T, error) {
	var states []T
	for _, stateStr := range FlagFilterCranedStates {
		lowered := strings.ToLower(stateStr)
		if state, exists := stateMap[lowered]; exists {
			states = append(states, state)
			continue
		}
		if _, ok := resourceStateMap[lowered]; ok && stateType != "resource" {
			continue
		}
		if _, ok := controlStateMap[lowered]; ok && stateType != "control" {
			continue
		}
		if _, ok := powerStateMap[lowered]; ok && stateType != "power" {
			continue
		}
		return nil, util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid %s state given: %s", stateType, stateStr))
	}

	return states, nil
}

func ConvertToResourceStates() ([]protos.CranedResourceState, error) {
	return ConvertStates(resourceStateMap, "resource")
}
func ConvertToControlStates() ([]protos.CranedControlState, error) {
	return ConvertStates(controlStateMap, "control")
}
func ConvertToPowerStates() ([]protos.CranedPowerState, error) {
	return ConvertStates(powerStateMap, "power")
}

func ApplyResourceStateDefaults(states []protos.CranedResourceState) []protos.CranedResourceState {
	if len(states) > 0 {
		return states
	}
	if FlagFilterRespondingOnly {
		return []protos.CranedResourceState{
			protos.CranedResourceState_CRANE_IDLE,
			protos.CranedResourceState_CRANE_MIX,
			protos.CranedResourceState_CRANE_ALLOC,
		}
	}
	if FlagFilterDownOnly {
		return []protos.CranedResourceState{protos.CranedResourceState_CRANE_DOWN}
	}
	return []protos.CranedResourceState{
		protos.CranedResourceState_CRANE_IDLE,
		protos.CranedResourceState_CRANE_MIX,
		protos.CranedResourceState_CRANE_ALLOC,
		protos.CranedResourceState_CRANE_DOWN,
	}
}
func ApplyControlStateDefaults(states []protos.CranedControlState) []protos.CranedControlState {
	if len(states) > 0 {
		return states
	}
	return []protos.CranedControlState{
		protos.CranedControlState_CRANE_NONE,
		protos.CranedControlState_CRANE_DRAIN,
	}
}
func ApplyPowerStateDefaults(states []protos.CranedPowerState) []protos.CranedPowerState {
	if len(states) > 0 {
		return states
	}
	return []protos.CranedPowerState{
		protos.CranedPowerState_CRANE_POWER_ACTIVE,
		protos.CranedPowerState_CRANE_POWER_IDLE,
		protos.CranedPowerState_CRANE_POWER_SLEEPING,
		protos.CranedPowerState_CRANE_POWER_POWEREDOFF,
		protos.CranedPowerState_CRANE_POWER_TO_SLEEPING,
		protos.CranedPowerState_CRANE_POWER_WAKING_UP,
		protos.CranedPowerState_CRANE_POWER_POWERING_ON,
		protos.CranedPowerState_CRANE_POWER_POWERING_OFF,
	}
}

func BuildFilteredStringList(items []string, warnMsg string) []string {
	var list []string
	for _, item := range items {
		if item != "" {
			list = append(list, item)
		} else {
			log.Warn(warnMsg)
		}
	}
	return list
}

func parseStateFilter() (func(*protos.CranedInfo) bool, error) {
	resourceStateList, err := ConvertToResourceStates()
	if err != nil {
		return nil, err
	}
	controlStateList, err := ConvertToControlStates()
	if err != nil {
		return nil, err
	}
	powerStateList, err := ConvertToPowerStates()
	if err != nil {
		return nil, err
	}

	resourceStateList = ApplyResourceStateDefaults(resourceStateList)
	controlStateList = ApplyControlStateDefaults(controlStateList)
	powerStateList = ApplyPowerStateDefaults(powerStateList)

	return func(node *protos.CranedInfo) bool {
		return slices.Contains(resourceStateList, node.ResourceState) &&
			slices.Contains(controlStateList, node.ControlState) &&
			slices.Contains(powerStateList, node.PowerState)
	}, nil
}

func queryClusterInfo(config *util.Config) (*protos.QueryClusterInfoReply, error) {
	matches, err := parseStateFilter()
	if err != nil {
		return nil, err
	}
	req := &protos.QueryClusterInfoRequest{
		IncludeNodes: true, IncludePartitions: true,
		FilterNodes:      BuildFilteredStringList(FlagFilterNodes, "Empty node name is ignored."),
		FilterPartitions: BuildFilteredStringList(FlagFilterPartitions, "Empty partition name is ignored."),
	}
	stub := util.GetStubToCtldByConfig(config)
	reply, err := stub.QueryClusterInfo(context.Background(), req)
	if err != nil {
		return nil, util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query cluster information")
	}
	reply.Nodes = slices.DeleteFunc(reply.Nodes, func(node *protos.CranedInfo) bool { return !matches(node) })
	return reply, nil
}

func QueryClusterInfo() (*protos.QueryClusterInfoReply, error) {
	return queryClusterInfo(util.ParseConfig(FlagConfigFilePath))
}

func Query() error {
	config := util.ParseConfig(FlagConfigFilePath)
	reply, err := queryClusterInfo(config)
	if err != nil {
		return err
	}
	if FlagJson {
		return JsonOutput(reply)
	}
	aliases := map[string]string{}
	if len(FlagFilterNodes) != 0 {
		aliases, err = util.BuildNodeAliasMap(config.CranedNodeList)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "invalid node aliases in config: %v", err)
		}
	}
	return QueryTableOutput(reply, aliases)
}

func loopedQuery(iterate uint64) error {
	interval, err := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}

	return loopedSubQuery(interval)
}

func loopedSubQuery(interval time.Duration) error {
	for {
		fmt.Println(time.Now().String()[0:19])
		err := Query()
		if err != nil {
			return err
		}
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
