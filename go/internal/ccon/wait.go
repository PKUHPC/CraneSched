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

package ccon

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

const (
	defaultWaitIntervalSeconds = 30
	minWaitIntervalSeconds     = 10
)

type waitStepStatus struct {
	StepId uint32           `json:"step_id"`
	Status protos.JobStatus `json:"status"`
}

type waitResult struct {
	JobId uint32           `json:"job_id"`
	Steps []waitStepStatus `json:"steps"`
}

func waitExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "wait command does not accept any arguments")
	}

	jobId, stepMode, err := util.ParseJobNestedEnv()
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "invalid CRANE_JOB_ID: %v", err)
	}
	if !stepMode {
		return util.NewCraneErr(util.ErrorCmdArg, "wait can only be used inside a job step (CRANE_JOB_ID is not set)")
	}

	f := GetFlags()
	if f.Wait.Interval < minWaitIntervalSeconds {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("poll interval must be at least %d seconds", minWaitIntervalSeconds))
	}

	interval := time.Duration(f.Wait.Interval) * time.Second
	done, result, err := checkContainerStepsDone(jobId)
	if err != nil {
		return err
	}
	if done {
		return finishWait(f, result)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		done, result, err = checkContainerStepsDone(jobId)
		if err != nil {
			return err
		}
		if done {
			return finishWait(f, result)
		}
	}

	return nil
}

func finishWait(f *Flags, result waitResult) error {
	if f.Global.Json {
		outputJson("wait", "", f.Wait, result)
		return nil
	}

	fmt.Printf("All container steps in job %d have finished.\n", result.JobId)
	return nil
}

func checkContainerStepsDone(jobId uint32) (bool, waitResult, error) {
	job, err := GetContainerJob(jobId, false)
	if err != nil {
		return false, waitResult{}, err
	}

	result := waitResult{
		JobId: jobId,
		Steps: make([]waitStepStatus, 0, len(job.StepInfoList)),
	}

	var containerSteps []*protos.StepInfo
	for _, step := range job.StepInfoList {
		// Skip daemon and non-container steps
		if step.StepId == 0 || step.ContainerMeta == nil {
			continue
		}
		containerSteps = append(containerSteps, step)
	}

	if len(containerSteps) == 0 {
		return true, result, nil
	}

	allDone := true
	for _, step := range containerSteps {
		result.Steps = append(result.Steps, waitStepStatus{
			StepId: step.StepId,
			Status: step.Status,
		})
		if !isTerminalStatus(step.Status) {
			allDone = false
		}
	}

	return allDone, result, nil
}

func isTerminalStatus(status protos.JobStatus) bool {
	switch status {
	case protos.JobStatus_Pending,
		protos.JobStatus_Running,
		protos.JobStatus_Configuring,
		protos.JobStatus_Starting,
		protos.JobStatus_Completing:
		return false
	default:
		return true
	}
}
