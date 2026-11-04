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

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// execExecute handles the exec command execution
func execExecute(cmd *cobra.Command, args []string) error {
	if len(args) < 2 {
		return util.NewCraneErr(util.ErrorCmdArg, "exec requires at least two arguments: CONTAINER (JOBID.STEPID) COMMAND [ARG...]")
	}

	jobID, stepID, err := util.ParseJobIdStepIdStrict(args[0])
	if err != nil {
		return err
	}
	if stepID == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "step 0 is reserved for pods, please specify a container step ID")
	}

	command := args[1:]

	// Get flags
	f := GetFlags()

	_, step, err := GetContainerStep(jobID, stepID, false)
	if err != nil {
		return err
	}

	// Check if the step is in a state that allows exec
	if step.Status != protos.JobStatus_Running {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Cannot exec into container %d.%d in state: %s", jobID, stepID, step.Status.String()))
	}

	nodeName, err := resolveTargetNode(step, f.Exec.TargetNode)
	if err != nil {
		return err
	}

	// Determine stdin, stdout, stderr based on flags
	stdin := f.Exec.Interactive
	tty := f.Exec.Tty
	stdout := true // Always enable stdout for exec
	stderr := !tty // Enable stderr only in non-TTY mode

	// Call ExecInContainerStep RPC
	execReq := &protos.ExecInContainerStepRequest{
		Uid:      uint32(os.Getuid()),
		JobId:    jobID,
		StepId:   stepID,
		NodeName: nodeName,
		Command:  command,
		Stdin:    stdin,
		Tty:      tty,
		Stdout:   stdout,
		Stderr:   stderr,
	}

	log.Debugf("Calling ExecInContainerStep RPC for container %d.%d on node %q with command %v, flags: stdin=%t, stdout=%t, stderr=%t, tty=%t",
		jobID, stepID, nodeName, command, execReq.Stdin, execReq.Stdout, execReq.Stderr, execReq.Tty)

	reply, err := stub.ExecInContainerStep(context.Background(), execReq)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to exec into container job")
	}

	if !reply.Ok {
		err = util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Exec failed: %s", reply.GetStatus().GetDescription()))
	}

	if f.Global.Json {
		outputJson("exec", "", f.Exec, reply)
		return err
	}

	// Handle RPC response
	if reply.Ok {
		if !f.Global.Json {
			log.Debugf("Exec request successful for container %d.%d\n", jobID, stepID)
			if tty {
				log.Debugf("Executing command in container %d.%d (TTY enabled)...\n", jobID, stepID)
			} else {
				log.Debugf("Executing command in container %d.%d...\n", jobID, stepID)
			}
		}

		// Create stream options based on flags
		streamOpts := StreamOptions{
			Stdin:     stdin,
			Stdout:    stdout,
			Stderr:    stderr,
			Tty:       tty,
			Transport: f.Exec.Transport,
		}

		// Start streaming
		ctx := context.Background()
		if err := streamWithURL(ctx, reply.Url, streamOpts, jobID, stepID); err != nil {
			return util.WrapCraneErr(util.ErrorBackend, "Stream connection lost. Please check container status with 'ccon ps' or reattach with 'ccon attach': %v", err)
		}
	}

	return err
}
