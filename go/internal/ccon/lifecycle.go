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
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// stopExecute handles the stop command execution
func stopExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return util.NewCraneErr(util.ErrorCmdArg, "stop requires exactly one argument: CONTAINER (JOBID.STEPID)")
	}

	f := GetFlags()

	jobId, stepId, err := util.ParseJobIdStepIdStrict(args[0])
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "invalid step ID: %v", err)
	}

	if stepId == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "step 0 is reserved for pods, please specify a container step ID")
	}

	// First, query the job to verify it's a container job
	queryReq := &protos.QueryJobsInfoRequest{
		FilterJobIds:   []*protos.JobIdSelector{{JobId: jobId, Steps: []uint32{stepId}}},
		FilterJobTypes: []protos.JobType{protos.JobType_Container},
	}

	queryReply, err := util.QueryJobsInfo(context.Background(), stub, queryReq)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query job information")
	}

	if !queryReply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, "Failed to query job information")
	}

	// Check if the job exists and is a container job
	if len(queryReply.JobInfoList) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Step %d.%d not found or is not a container", jobId, stepId))
	}

	// Create cancel request
	req := &protos.CancelJobRequest{
		OperatorUid:     uint32(os.Getuid()),
		FilterJobIds:    []*protos.JobIdSelector{{JobId: jobId, Steps: []uint32{stepId}}},
		FilterPartition: "",
		FilterAccount:   "",
		FilterState:     protos.JobStatus_Invalid,
		FilterUsername:  "",
		FilterNodes:     nil,
		FilterJobName:   "",
	}

	// Send cancel request
	reply, err := stub.CancelJob(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to stop container job")
	}

	if f.Global.Json {
		outputJson("stop", "", f.Stop, reply)
		if len(reply.NotCancelled) > 0 {
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, "")
		}
	}

	// Handle non-JSON output
	if len(reply.Cancelled) > 0 {
		fmt.Printf("Container %d.%d stopped successfully\n", jobId, stepId)
		return nil
	}

	if len(reply.NotCancelled) > 0 {
		reason := reply.NotCancelled[0].GetReason()
		return util.NewCraneErr(util.ErrorBackend,
			fmt.Sprintf("Failed to stop container %d.%d: %s", jobId, stepId, reason))
	}

	return nil
}

func rmExecute(cmd *cobra.Command, args []string) error {
	fmt.Printf("Error: 'rm' command is not supported in CraneSched.\n")
	fmt.Printf("Container jobs are automatically cleaned up when they complete.\n")
	fmt.Printf("Use 'ccon stop <job_id>' to cancel a running job.\n")

	return util.NewCraneErr(util.ErrorGeneric, "")
}

func createExecute(cmd *cobra.Command, args []string) error {
	fmt.Printf("Error: 'create' command is not supported in CraneSched.\n")
	fmt.Printf("Use 'ccon run <image>' to submit a container job.\n")

	return util.NewCraneErr(util.ErrorGeneric, "")
}

func startExecute(cmd *cobra.Command, args []string) error {
	fmt.Printf("Error: 'start' command is not supported in CraneSched.\n")
	fmt.Printf("Use 'ccon run <image>' to submit a new container job.\n")

	return util.NewCraneErr(util.ErrorGeneric, "")
}

func restartExecute(cmd *cobra.Command, args []string) error {
	fmt.Printf("Error: 'restart' command is not supported in CraneSched.\n")
	fmt.Printf("Use 'ccon run <image>' to submit a new container job.\n")

	return util.NewCraneErr(util.ErrorGeneric, "")
}
