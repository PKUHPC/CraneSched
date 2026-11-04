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

package util

import (
	"CraneFrontEnd/generated/protos"
	"strings"
)

type OutputMode uint8

const (
	OutputModeNative OutputMode = iota
	OutputModeSlurm
)

var outputMode = OutputModeNative

func SetOutputMode(mode OutputMode) {
	outputMode = mode
}

func IsSlurmOutputMode() bool {
	return outputMode == OutputModeSlurm
}

func FormatSlurmNodeState(resourceState protos.CranedResourceState,
	controlState protos.CranedControlState, powerState protos.CranedPowerState) string {
	states := []string{strings.ToLower(strings.TrimPrefix(resourceState.String(), "CRANE_"))}

	if controlState == protos.CranedControlState_CRANE_DRAIN {
		states = append(states, "drain")
	}

	switch powerState {
	case protos.CranedPowerState_CRANE_POWER_SLEEPING,
		protos.CranedPowerState_CRANE_POWER_POWEREDOFF:
		states = append(states, "powered_down")
	case protos.CranedPowerState_CRANE_POWER_TO_SLEEPING,
		protos.CranedPowerState_CRANE_POWER_POWERING_OFF:
		states = append(states, "powering_down")
	case protos.CranedPowerState_CRANE_POWER_WAKING_UP,
		protos.CranedPowerState_CRANE_POWER_POWERING_ON:
		states = append(states, "powering_up")
	}

	return strings.Join(states, "+")
}
