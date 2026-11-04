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

package ccancel

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"encoding/json"
	"fmt"
	"os"
)

var (
	stub protos.CraneCtldClient
)

type cancelledStepsJson struct {
	Steps []uint32 `json:"steps"`
}

type notCancelledJobStepsJson struct {
	Reason      string   `json:"reason"`
	StepIds     []uint32 `json:"step_ids"`
	StepReasons []string `json:"step_reasons"`
}

type cancelJobReplyJson struct {
	CancelledSteps       map[string]cancelledStepsJson       `json:"cancelled_steps"`
	NotCancelledJobSteps map[string]notCancelledJobStepsJson `json:"not_cancelled_job_steps"`
}

func formatCancelJobReplyJson(reply *protos.CancelJobReply) (string, error) {
	output := cancelJobReplyJson{
		CancelledSteps:       make(map[string]cancelledStepsJson),
		NotCancelledJobSteps: make(map[string]notCancelledJobStepsJson),
	}

	for _, item := range reply.Cancelled {
		key := util.FormatJobIdFromArrayTaskId(item.JobId, item.ArrayTaskId)
		output.CancelledSteps[key] = cancelledStepsJson{
			Steps: append([]uint32{}, item.Steps...),
		}
	}

	for _, item := range reply.NotCancelled {
		key := util.FormatJobIdFromArrayTaskId(item.JobId, item.ArrayTaskId)
		entry := output.NotCancelledJobSteps[key]
		if entry.StepIds == nil {
			entry.StepIds = []uint32{}
		}
		if entry.StepReasons == nil {
			entry.StepReasons = []string{}
		}
		if item.StepId == nil {
			entry.Reason = item.Reason
		} else {
			entry.StepIds = append(entry.StepIds, *item.StepId)
			entry.StepReasons = append(entry.StepReasons, item.Reason)
		}
		output.NotCancelledJobSteps[key] = entry
	}

	data, err := json.Marshal(output)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func CancelJob(args []string) error {
	req := &protos.CancelJobRequest{
		OperatorUid: uint32(os.Getuid()),

		FilterPartition: FlagPartition,
		FilterAccount:   FlagAccount,
		FilterState:     protos.JobStatus_Invalid,
		FilterUsername:  FlagUserName,
	}

	err := util.CheckJobNameLength(FlagJobName)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid job name: %v.", err))
	}
	req.FilterJobName = FlagJobName

	if len(args) > 0 {
		selectors, err := util.ParseJobIdSelectorList(args[0], ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid job list specified: %v.\n", err))
		}
		req.FilterJobIds = selectors
	}

	if FlagState != "" {
		stateList, err := util.ParseInRamJobStatusList(FlagState)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, err.Error())
		}
		if len(stateList) == 1 {
			req.FilterState = stateList[0]
		}
	}

	req.FilterNodes = FlagNodes

	reply, err := stub.CancelJob(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to cancel jobs")
	}

	if FlagJson {
		output, err := formatCancelJobReplyJson(reply)
		if err != nil {
			return util.WrapCraneErr(util.ErrorInvalidFormat, "Failed to marshal cancel reply: %v", err)
		}
		fmt.Println(output)
		if len(reply.NotCancelled) > 0 {
			return &util.CraneError{Code: util.ErrorBackend}
		} else {
			return nil
		}
	}

	if len(reply.Cancelled) > 0 {
		jobIdStrList := make([]string, 0)
		stepIdStrList := make([]string, 0)
		for _, item := range reply.Cancelled {
			if len(item.Steps) == 0 {
				jobIdStrList = append(jobIdStrList, util.FormatJobIdFromArrayTaskId(item.JobId, item.ArrayTaskId))
				continue
			}
			for _, stepId := range item.Steps {
				stepIdStrList = append(stepIdStrList, util.FormatStepIdFromArrayTaskId(item.JobId, item.ArrayTaskId, stepId))
			}
		}
		cancelledJobsString := ""
		if len(jobIdStrList) != 0 {
			cancelledJobsString = fmt.Sprintf("Job %s",
				util.ConvertSliceToString(jobIdStrList, ","))
		}
		if len(stepIdStrList) > 0 {
			stepStr := fmt.Sprintf("Step %s",
				util.ConvertSliceToString(stepIdStrList, ","))
			if len(cancelledJobsString) == 0 {
				cancelledJobsString = stepStr
			} else {
				cancelledJobsString += " " + stepStr
			}
		}
		fmt.Printf("%s cancelled successfully.\n", cancelledJobsString)
	}

	if len(reply.NotCancelled) > 0 {
		for _, entry := range reply.NotCancelled {
			id := util.FormatJobIdFromArrayTaskId(entry.JobId, entry.ArrayTaskId)
			if entry.StepId == nil {
				fmt.Printf("Failed to cancel job: %s. Reason: %s.\n", id, entry.Reason)
			} else {
				fmt.Printf("Failed to cancel step: %s-%d. Reason: %s.\n", id, *entry.StepId, entry.Reason)
			}
		}
		return &util.CraneError{Code: util.ErrorBackend}
	}
	return nil
}
