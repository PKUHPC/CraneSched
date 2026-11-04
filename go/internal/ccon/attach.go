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
	"slices"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// attachExecute handles the attach command execution
func attachExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return util.NewCraneErr(util.ErrorCmdArg, "attach requires exactly one argument: CONTAINER (JOBID.STEPID)")
	}

	jobID, stepID, err := util.ParseJobIdStepIdStrict(args[0])
	if err != nil {
		return err
	}
	if stepID == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "step 0 is reserved for pods, please specify a container step ID")
	}

	// Get flags and apply tty/stderr mutual exclusion logic
	f := GetFlags()

	// When --tty is set, --stderr should be disabled (TTY combines stdout and stderr)
	if f.Attach.Tty {
		if cmd.Flags().Changed("stderr") && f.Attach.Stderr {
			return util.NewCraneErr(util.ErrorCmdArg, "Cannot use --stderr with --tty; stderr is combined into stdout in TTY mode")
		}
		f.Attach.Stderr = false
	}

	_, step, err := GetContainerStep(jobID, stepID, false)
	if err != nil {
		return err
	}

	// Check if the step is in a state that allows attaching
	if step.Status != protos.JobStatus_Running {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Cannot attach to container %d.%d in state: %s", jobID, stepID, step.Status.String()))
	}

	nodeName, err := resolveTargetNode(step, f.Attach.TargetNode)
	if err != nil {
		return err
	}

	// Call AttachContainerStep RPC
	attachReq := &protos.AttachContainerStepRequest{
		Uid:      uint32(os.Getuid()),
		JobId:    jobID,
		StepId:   stepID,
		NodeName: nodeName,
		Stdin:    f.Attach.Stdin,
		Tty:      f.Attach.Tty,
		Stdout:   f.Attach.Stdout,
		Stderr:   f.Attach.Stderr,
	}

	log.Debugf("Calling AttachContainerStep RPC for container %d.%d on node %q with flags: stdin=%t, stdout=%t, stderr=%t, tty=%t",
		jobID, stepID, nodeName, attachReq.Stdin, attachReq.Stdout, attachReq.Stderr, attachReq.Tty)

	reply, err := stub.AttachContainerStep(context.Background(), attachReq)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to attach to container job")
	}

	if !reply.Ok {
		err = util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Attach failed: %s", reply.GetStatus().GetDescription()))
	}

	if f.Global.Json {
		outputJson("attach", "", f.Attach, reply)
		return err
	}

	// Handle RPC response
	if reply.Ok {
		if !f.Global.Json {
			log.Debugf("Attach request successful for container %d.%d\n", jobID, stepID)
			if f.Attach.Tty {
				log.Debugf("Attaching to container %d.%d (TTY enabled)...\n", jobID, stepID)
			} else {
				log.Debugf("Attaching to container %d.%d...\n", jobID, stepID)
			}
		}

		// Create stream options based on flags
		streamOpts := StreamOptions{
			Stdin:     f.Attach.Stdin,
			Stdout:    f.Attach.Stdout,
			Stderr:    f.Attach.Stderr,
			Tty:       f.Attach.Tty,
			Transport: f.Attach.Transport,
		}

		// Start streaming
		ctx := context.Background()
		if err := streamWithURL(ctx, reply.Url, streamOpts, jobID, stepID); err != nil {
			return util.WrapCraneErr(util.ErrorBackend, "Stream connection lost. Please check container status with 'ccon ps' or reattach with 'ccon attach': %v", err)
		}
	}

	return err
}

func resolveTargetNode(step *protos.StepInfo, targetNode string) (string, error) {
	executionNodes := step.GetExecutionNode()
	if targetNode == "" {
		switch len(executionNodes) {
		case 0:
			return "", util.NewCraneErr(util.ErrorBackend, "execution node list of this step is empty")
		case 1:
			return executionNodes[0], nil
		default:
			return "", util.NewCraneErr(util.ErrorCmdArg,
				fmt.Sprintf("container is running on multiple nodes: %s; please specify --target-node to select one", step.GetCranedList()))
		}
	}

	canonicalTarget, err := resolveNodeAlias(targetNode)
	if err != nil {
		return "", err
	}

	if len(executionNodes) > 0 {
		if !slices.Contains(executionNodes, canonicalTarget) {
			return "", util.NewCraneErr(util.ErrorCmdArg,
				fmt.Sprintf("container is not running on the target node %q: %s", targetNode, step.GetCranedList()))
		}
	}

	return canonicalTarget, nil
}

func resolveNodeAlias(node string) (string, error) {
	if config == nil || len(config.CranedNodeList) == 0 {
		return node, nil
	}

	aliases, err := util.BuildNodeAliasMap(config.CranedNodeList)
	if err != nil {
		return "", util.WrapCraneErr(util.ErrorCmdArg,
			"invalid node aliases in config: %v", err)
	}

	if canonical, ok := aliases[node]; ok {
		return canonical, nil
	}
	// Unknown nodes are kept for the backend to validate.
	return node, nil
}
