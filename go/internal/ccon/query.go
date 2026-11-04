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
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

func psExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "ps command does not accept any arguments")
	}

	filterUsers, err := currentUserFilter()
	if err != nil {
		return err
	}

	f := GetFlags()
	request := protos.QueryJobsInfoRequest{
		FilterJobTypes:             []protos.JobType{protos.JobType_Container},
		OptionIncludeCompletedJobs: f.Ps.All,
		FilterUsers:                filterUsers,
	}

	reply, err := util.QueryJobsInfo(context.Background(), stub, &request)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query container jobs")
	}

	if !reply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, "")
	}

	// Flatten steps for container view
	type stepRow struct {
		JobId           uint32                 `json:"job_id"`
		StepId          uint32                 `json:"step_id"`
		Image           string                 `json:"image"`
		Command         string                 `json:"command"`
		CreatedAt       time.Time              `json:"created_at"`
		Status          protos.JobStatus       `json:"status"`
		Name            string                 `json:"name"`
		NodeList        string                 `json:"node_list"`
		QueryVisibility protos.QueryVisibility `json:"query_visibility"`
	}

	var rows []stepRow
	for _, job := range reply.JobInfoList {
		for _, step := range job.StepInfoList {
			if step.StepId == 0 || step.ContainerMeta == nil {
				// Skip pod step or non-container step
				continue
			}

			row := stepRow{
				JobId:           job.JobId,
				StepId:          step.StepId,
				Name:            step.Name,
				Status:          step.Status,
				QueryVisibility: step.QueryVisibility,
			}

			if step.GetContainerMeta().GetImage() != nil {
				row.Image = step.ContainerMeta.Image.Image
			}
			if step.ContainerMeta != nil {
				var cmdParts []string
				if step.ContainerMeta.Command != "" {
					cmdParts = append(cmdParts, step.ContainerMeta.Command)
				}
				if len(step.ContainerMeta.Args) > 0 {
					cmdParts = append(cmdParts, step.ContainerMeta.Args...)
				}
				if len(cmdParts) > 0 {
					row.Command = strings.Join(cmdParts, " ")
				}
			}
			if step.SubmitTime != nil {
				row.CreatedAt = step.SubmitTime.AsTime()
			} else if job.SubmitTime != nil {
				row.CreatedAt = job.SubmitTime.AsTime()
			}

			row.NodeList = step.GetCranedList()
			if row.NodeList == "" {
				row.NodeList = job.GetCranedList()
			}

			rows = append(rows, row)
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].JobId == rows[j].JobId {
			return rows[i].StepId > rows[j].StepId
		}
		return rows[i].JobId > rows[j].JobId
	})

	if f.Global.Json {
		outputJson("ps", "", f.Ps, rows)
		return nil
	}

	if f.Ps.Quiet {
		for _, row := range rows {
			fmt.Printf("%d.%d\n", row.JobId, row.StepId)
		}
		return nil
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	table.SetHeader([]string{"CONTAINER", "IMAGE", "COMMAND", "CREATED", "STATUS", "NODELIST"})

	for _, row := range rows {
		createdStr := "-"
		if !row.CreatedAt.IsZero() {
			createdStr = formatDuration(time.Since(row.CreatedAt)) + " ago"
		}
		table.Append([]string{
			fmt.Sprintf("%d.%d", row.JobId, row.StepId),
			util.PrivateJobField(row.Image, row.QueryVisibility),
			util.PrivateJobField(truncateCommand(row.Command, 20), row.QueryVisibility),
			createdStr,
			strings.ToUpper(row.Status.String()),
			row.NodeList,
		})
	}
	table.Render()
	return nil
}

func podExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "pod command does not accept any arguments")
	}

	filterUsers, err := currentUserFilter()
	if err != nil {
		return err
	}

	f := GetFlags()
	request := protos.QueryJobsInfoRequest{
		FilterJobTypes:             []protos.JobType{protos.JobType_Container},
		OptionIncludeCompletedJobs: f.Pod.All,
		FilterUsers:                filterUsers,
	}

	reply, err := util.QueryJobsInfo(context.Background(), stub, &request)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query container pods")
	}

	if !reply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, "")
	}

	sort.Slice(reply.JobInfoList, func(i, j int) bool {
		return reply.JobInfoList[i].JobId > reply.JobInfoList[j].JobId
	})

	if f.Global.Json {
		outputJson("pods", "", f.Pod, reply.JobInfoList)
		return nil
	}

	if f.Pod.Quiet {
		for _, job := range reply.JobInfoList {
			fmt.Println(job.JobId)
		}
		return nil
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	table.SetHeader([]string{"JOBID", "POD", "PARTITION", "CREATED", "STATUS", "PORTS"})

	for _, job := range reply.JobInfoList {
		createdStr := "-"
		if job.SubmitTime != nil {
			createdStr = formatDuration(time.Since(job.SubmitTime.AsTime())) + " ago"
		}

		podName := "-"
		if job.PodMeta != nil && job.PodMeta.Name != "" {
			podName = job.PodMeta.Name
		}

		var ports string
		if job.PodMeta != nil && len(job.PodMeta.Ports) > 0 {
			var portStrs []string
			for _, port := range job.PodMeta.Ports {
				portStrs = append(portStrs, fmt.Sprintf("%d:%d", port.HostPort, port.ContainerPort))
			}
			ports = strings.Join(portStrs, ", ")
		} else {
			ports = "-"
		}
		if job.QueryVisibility == protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC {
			podName = util.RestrictedJobField
			ports = util.RestrictedJobField
		}

		table.Append([]string{
			strconv.FormatUint(uint64(job.JobId), 10),
			podName,
			job.Partition,
			createdStr,
			strings.ToUpper(job.Status.String()),
			ports,
		})
	}

	table.Render()
	return nil
}

func truncateCommand(command string, maxLen int) string {
	if command == "" {
		return "-"
	}

	// Remove extra whitespace and newlines
	command = strings.ReplaceAll(command, "\n", " ")
	command = strings.ReplaceAll(command, "\t", " ")

	// Replace multiple spaces with single space
	for strings.Contains(command, "  ") {
		command = strings.ReplaceAll(command, "  ", " ")
	}
	command = strings.TrimSpace(command)

	if len(command) <= maxLen {
		return command
	}

	return command[:maxLen-3] + "..."
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%d seconds", int(d.Seconds()))
	} else if d < time.Hour {
		return fmt.Sprintf("%d minutes", int(d.Minutes()))
	} else if d < 24*time.Hour {
		return fmt.Sprintf("%d hours", int(d.Hours()))
	} else {
		return fmt.Sprintf("%d days", int(d.Hours()/24))
	}
}

func currentUserFilter() ([]string, error) {
	if os.Getuid() == 0 {
		return nil, nil
	}

	cu, err := user.Current()
	if err != nil {
		return nil, util.WrapCraneErr(util.ErrorGeneric, "failed to get username: %v", err)
	}

	return []string{cu.Username}, nil
}

func inspectPodExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return util.NewCraneErr(util.ErrorCmdArg, "inspectp requires exactly one argument: POD")
	}

	filterUsers, err := currentUserFilter()
	if err != nil {
		return err
	}

	jobIDStr := args[0]
	jobID, err := strconv.ParseUint(jobIDStr, 10, 32)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid job ID: %s", jobIDStr))
	}

	request := protos.QueryJobsInfoRequest{
		FilterJobIds:               []*protos.JobIdSelector{{JobId: uint32(jobID)}},
		FilterJobTypes:             []protos.JobType{protos.JobType_Container},
		OptionIncludeCompletedJobs: true,
		FilterUsers:                filterUsers,
	}

	reply, err := util.QueryJobsInfo(context.Background(), stub, &request)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query container pod")
	}

	if !reply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, "")
	}

	if len(reply.JobInfoList) == 0 {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container pod %s not found", jobIDStr))
	}

	job := reply.JobInfoList[0]
	if err := util.RequireJobDetails(job.QueryVisibility); err != nil {
		return err
	}
	jsonData, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return util.WrapCraneErr(util.ErrorBackend, "failed to format data from backend: %v", err)
	}

	fmt.Println(string(jsonData))
	return nil
}

func inspectStepExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return util.NewCraneErr(util.ErrorCmdArg, "inspect requires exactly one argument: CONTAINER (format: JOBID.STEPID)")
	}

	filterUsers, err := currentUserFilter()
	if err != nil {
		return err
	}

	jobID, stepID, err := util.ParseJobIdStepIdStrict(args[0])
	if err != nil {
		return err
	}

	if stepID == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "step 0 is reserved for pods. Please use inspectp to query it.")
	}

	request := protos.QueryJobsInfoRequest{
		FilterJobIds:               []*protos.JobIdSelector{{JobId: jobID, Steps: []uint32{stepID}}},
		FilterJobTypes:             []protos.JobType{protos.JobType_Container},
		OptionIncludeCompletedJobs: true,
		FilterUsers:                filterUsers,
	}

	reply, err := util.QueryJobsInfo(context.Background(), stub, &request)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query container step")
	}

	if !reply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, "")
	}

	if len(reply.JobInfoList) == 0 {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container step %d.%d not found", jobID, stepID))
	}

	var targetStep *protos.StepInfo
	for _, step := range reply.JobInfoList[0].StepInfoList {
		if step.StepId == stepID {
			targetStep = step
			break
		}
	}

	if targetStep == nil {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container step %d.%d not found", jobID, stepID))
	}

	if err := util.RequireJobDetails(targetStep.QueryVisibility); err != nil {
		return err
	}
	jsonData, err := json.MarshalIndent(targetStep, "", "  ")
	if err != nil {
		return util.WrapCraneErr(util.ErrorBackend, "failed to format data from backend: %v", err)
	}

	fmt.Println(string(jsonData))
	return nil
}

// GetContainerJob queries exact 1 specific container job and returns the job info.
func GetContainerJob(jobId uint32, includeCompleted bool) (*protos.JobInfo, error) {
	request := protos.QueryJobsInfoRequest{
		FilterJobIds:               []*protos.JobIdSelector{{JobId: jobId}},
		FilterJobTypes:             []protos.JobType{protos.JobType_Container},
		OptionIncludeCompletedJobs: includeCompleted,
	}

	reply, err := util.QueryJobsInfo(context.Background(), stub, &request)
	if err != nil {
		return nil, util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query container job")
	}
	if !reply.GetOk() {
		return nil, util.NewCraneErr(util.ErrorBackend, "")
	}
	if len(reply.JobInfoList) == 0 {
		return nil, util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container job %d not found", jobId))
	}

	return reply.JobInfoList[0], nil
}

// GetContainerStep queries exact 1 specific container step and returns the job and step info.
func GetContainerStep(jobID, stepID uint32, includeCompleted bool) (*protos.JobInfo, *protos.StepInfo, error) {
	req := protos.QueryJobsInfoRequest{
		FilterJobIds:               []*protos.JobIdSelector{{JobId: jobID, Steps: []uint32{stepID}}},
		FilterJobTypes:             []protos.JobType{protos.JobType_Container},
		OptionIncludeCompletedJobs: includeCompleted,
	}

	reply, err := util.QueryJobsInfo(context.Background(), stub, &req)
	if err != nil {
		return nil, nil, util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query container step")
	}
	if !reply.GetOk() {
		return nil, nil, util.NewCraneErr(util.ErrorBackend, "")
	}

	if len(reply.JobInfoList) == 0 {
		return nil, nil, util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container %d.%d not found", jobID, stepID))
	}

	job := reply.JobInfoList[0]
	var targetStep *protos.StepInfo
	for _, step := range job.StepInfoList {
		if step.StepId == stepID {
			targetStep = step
			break
		}
	}

	if targetStep == nil {
		return nil, nil, util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container %d.%d not found", jobID, stepID))
	}

	return job, targetStep, nil
}
