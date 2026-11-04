/**
 * Copyright (c) 2025 Peking University and Peking University
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

package crun

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func setInheritedStepFieldsFromEnv(step *protos.StepToCtld) error {
	if value, exists := os.LookupEnv("SLURM_EXTERNAL_LAUNCHER"); exists {
		value = strings.TrimSpace(strings.ToLower(value))
		step.ExternalLauncher = step.ExternalLauncher ||
			(value != "" && value != "0" && value != "false" && value != "no")
	}
	if ntasksString, exists := os.LookupEnv("CRANE_NTASKS"); exists {
		ntasks, err := strconv.ParseUint(ntasksString, 10, 32)
		if err != nil {
			return util.NewCraneErr(util.ErrorInvalidFormat, "invalid CRANE_NTASKS from env")
		}
		step.Ntasks = uint32(ntasks)
	}
	if nodeCountString, exists := os.LookupEnv("CRANE_JOB_NUM_NODES"); exists {
		nodeCount, err := strconv.ParseUint(nodeCountString, 10, 32)
		if err != nil {
			return util.NewCraneErr(util.ErrorInvalidFormat, "invalid CRANE_JOB_NUM_NODES from env")
		}
		step.NodeNum = uint32(nodeCount)
	}
	if ntasksPerNodeString, exists := os.LookupEnv("CRANE_NTASKS_PER_NODE"); exists {
		ntasksPerNode, err := strconv.ParseUint(ntasksPerNodeString, 10, 32)
		if err != nil {
			return util.NewCraneErr(util.ErrorInvalidFormat, "invalid CRANE_NTASKS_PER_NODE from env")
		}
		step.NtasksPerNode = uint32(ntasksPerNode)
	}
	if memoryPerNodeString, exists := os.LookupEnv("CRANE_MEM_PER_NODE"); exists {
		memoryPerNode, err := util.ParseMemStringAsByte(memoryPerNodeString)
		if err != nil {
			return util.NewCraneErr(util.ErrorInvalidFormat,
				fmt.Sprintf("invalid CRANE_MEM_PER_NODE from env: %s", memoryPerNodeString))
		}
		step.MemPerNode = &memoryPerNode
	}
	return nil
}
