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
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/sjson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	userUid uint32
	stub    protos.CraneCtldClient
)

type UpdateJobParamFlags int

const (
	CommentTypeFlag UpdateJobParamFlags = 1 << iota
	MailUserTypeFlag
	MailTypeTypeFlag
	PriorityTypeFlag
	TimelimitTypeFlag
	DeadlineTypeFlag
)

func SummarizeReply(proto interface{}) error {
	switch reply := proto.(type) {
	case *protos.ModifyJobReply:
		if len(reply.ModifiedJobs) > 0 {
			modifiedJobsString := util.ConvertSliceToString(reply.ModifiedJobs, ", ")
			fmt.Printf("Jobs %s modified successfully.\n", modifiedJobsString)
		}
		if len(reply.NotModifiedJobs) > 0 {
			msg := ""
			for i := 0; i < len(reply.NotModifiedJobs); i++ {
				msg += fmt.Sprintf("Failed to modify job: %d. Reason: %s.\n", reply.NotModifiedJobs[i], reply.NotModifiedReasons[i])
			}
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
		return nil
	case *protos.ModifyCranedStateReply:
		if len(reply.ModifiedNodes) > 0 {
			nodeListString := util.ConvertSliceToString(reply.ModifiedNodes, ", ")
			fmt.Printf("Nodes %s modified successfully, please wait for a few minutes for the node state to fully update.\n", nodeListString)
		}
		if len(reply.NotModifiedNodes) > 0 {
			msg := ""
			for i := 0; i < len(reply.NotModifiedNodes); i++ {
				msg += fmt.Sprintf("Failed to modify node: %s. Reason: %s.\n", reply.NotModifiedNodes[i], reply.NotModifiedReasons[i])
			}
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
		return nil
	case *protos.ModifyJobsExtraAttrsReply:
		if len(reply.ModifiedJobs) > 0 {
			modifiedJobsString := util.ConvertSliceToString(reply.ModifiedJobs, ", ")
			fmt.Printf("Jobs %s modified successfully.\n", modifiedJobsString)
		}
		if len(reply.NotModifiedJobs) > 0 {
			msg := ""
			for i := 0; i < len(reply.NotModifiedJobs); i++ {
				msg += fmt.Sprintf("Failed to modify job: %d. Reason: %s.\n", reply.NotModifiedJobs[i], reply.NotModifiedReasons[i])
			}
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
		return nil
	default:
		return util.NewCraneErr(util.ErrorGeneric, "Unknown reply.")
	}
}

func ChangeJobTimeLimit(jobStr string, timeLimit string) error {
	seconds, err := util.ParseDurationStrToSeconds(timeLimit)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}

	jobIds, err := util.ParseJobIdSelectorList(jobStr, ",")
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Invalid job list specified: %s.\n", err)
	}

	req := &protos.ModifyJobRequest{
		Uid:       uint32(os.Getuid()),
		JobIds:    jobIds,
		Attribute: protos.ModifyJobRequest_TimeLimit,
		Value: &protos.ModifyJobRequest_TimeLimitSeconds{
			TimeLimitSeconds: seconds,
		},
	}
	reply, err := stub.ModifyJob(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to change job time limit")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedJobs) == 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return SummarizeReply(reply)
}

func ChangeDeadlineTime(jobStr string, deadline string) error {
	time, err := util.ParseTime(deadline)

	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: err.Error(),
		}
	}
	jobIds, err := util.ParseJobIdSelectorList(jobStr, ",")
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Invalid job list specified: %s.\n", err)
	}

	req := &protos.ModifyJobRequest{
		Uid:       uint32(os.Getuid()),
		JobIds:    jobIds,
		Attribute: protos.ModifyJobRequest_Deadline,
		Value: &protos.ModifyJobRequest_DeadlineTime{
			DeadlineTime: timestamppb.New(time),
		},
	}

	reply, err := stub.ModifyJob(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to change task deadline")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedJobs) == 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return SummarizeReply(reply)
}

func RequeueJobs(jobs string) error {
	jobList, displayByJobId, err := resolveRequeueJobIds(jobs)
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Invalid job list specified: %s.\n", err)
	}

	req := &protos.RequeueJobRequest{
		OperatorUid: uint32(os.Getuid()),
		JobIds:      jobList,
	}

	reply, err := stub.RequeueJob(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to requeue the job")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotRequeuedJobs) == 0 {
			return nil
		}
		return &util.CraneError{Code: util.ErrorBackend}
	}

	for _, jobId := range reply.RequeuedJobs {
		fmt.Printf("Job %s requeued successfully.\n", displayRequeueJobId(jobId, displayByJobId))
	}
	for jobId, reason := range reply.NotRequeuedJobs {
		fmt.Printf("Job %s failed to requeue: %s\n", displayRequeueJobId(jobId, displayByJobId), reason)
	}
	if len(reply.NotRequeuedJobs) > 0 {
		return &util.CraneError{Code: util.ErrorBackend}
	}
	return nil
}

func resolveRequeueJobIds(jobs string) ([]uint32, map[uint32]string, error) {
	selectors, err := util.ParseJobIdSelectorList(jobs, ",")
	if err != nil {
		return nil, nil, err
	}

	jobIds := make([]uint32, 0, len(selectors))
	displayByJobId := make(map[uint32]string, len(selectors))
	arraySelectors := make([]*protos.JobIdSelector, 0)
	for _, selector := range selectors {
		if len(selector.Steps) != 0 {
			return nil, nil, fmt.Errorf("step selector is not supported for requeue: %s",
				util.JobIdentifierFromSelector(selector).String())
		}
		if selector.ArrayTaskId == nil {
			jobIds = append(jobIds, selector.JobId)
			displayByJobId[selector.JobId] = util.FormatJobId(selector.JobId, nil)
			continue
		}
		arraySelectors = append(arraySelectors, selector)
	}

	if len(arraySelectors) == 0 {
		return jobIds, displayByJobId, nil
	}

	reply, err := getJobInfoReply(arraySelectors)
	if err != nil {
		return nil, nil, err
	}

	resolvedBySelector := make(map[util.JobIdentifier]*protos.JobInfo, len(reply.JobInfoList))
	for _, job := range reply.JobInfoList {
		resolvedBySelector[util.JobIdentifierFromJobInfo(job)] = job
	}

	for _, selector := range arraySelectors {
		identifier := util.JobIdentifierFromSelector(selector)
		job := resolvedBySelector[identifier]
		if job == nil {
			return nil, nil, fmt.Errorf("job %s is not running", identifier.String())
		}
		jobIds = append(jobIds, job.JobId)
		displayByJobId[job.JobId] = identifier.String()
	}

	return jobIds, displayByJobId, nil
}

func displayRequeueJobId(jobId uint32, displayByJobId map[uint32]string) string {
	if display, ok := displayByJobId[jobId]; ok {
		return display
	}
	return util.FormatJobId(jobId, nil)
}

func HoldReleaseJobs(jobs string, hold bool) error {
	jobIds, err := util.ParseJobIdSelectorList(jobs, ",")
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Invalid job list specified: %s.\n", err)
	}

	req := &protos.ModifyJobRequest{
		Uid:       uint32(os.Getuid()),
		JobIds:    jobIds,
		Attribute: protos.ModifyJobRequest_Hold,
	}
	if hold {
		// The default timer value for hold is unlimited.
		req.Value = &protos.ModifyJobRequest_HoldSeconds{HoldSeconds: math.MaxInt64}

		// If a time limit for hold constraint is specified, parse it.
		if FlagHoldTime != "" {
			seconds, err := util.ParseDurationStrToSeconds(FlagHoldTime)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, err.Error())
			}

			if seconds == 0 {
				return util.NewCraneErr(util.ErrorCmdArg, "Hold time must be greater than 0.")
			}

			req.Value = &protos.ModifyJobRequest_HoldSeconds{HoldSeconds: seconds}
		}
	} else {
		req.Value = &protos.ModifyJobRequest_HoldSeconds{HoldSeconds: 0}
	}

	reply, err := stub.ModifyJob(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to modify the job")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedJobs) == 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return SummarizeReply(reply)
}

func SuspendJobs(jobs string) error {
	jobIds, err := util.ParseJobIdSelectorList(jobs, ",")
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "invalid job list: %s", err)
	}

	req := &protos.ModifyJobRequest{
		Uid:       uint32(os.Getuid()),
		JobIds:    jobIds,
		Attribute: protos.ModifyJobRequest_Suspend,
	}

	reply, err := stub.ModifyJob(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to suspend jobs")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		// Check if there are any failures and return appropriate error code
		if len(reply.NotModifiedJobs) > 0 {
			return &util.CraneError{Code: util.ErrorBackend}
		}
		return nil
	}

	return SummarizeReply(reply)
}

func ResumeJobs(jobs string) error {
	jobIds, err := util.ParseJobIdSelectorList(jobs, ",")
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "invalid job list: %s", err)
	}

	req := &protos.ModifyJobRequest{
		Uid:       uint32(os.Getuid()),
		JobIds:    jobIds,
		Attribute: protos.ModifyJobRequest_Resume,
	}

	reply, err := stub.ModifyJob(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to resume jobs")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		// Check if there are any failures and return appropriate error code
		if len(reply.NotModifiedJobs) > 0 {
			return &util.CraneError{Code: util.ErrorBackend}
		}
		return nil
	}

	return SummarizeReply(reply)
}

func ChangeJobPriority(jobStr string, priority float64) error {
	if priority < 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "Priority must be greater than or equal to 0.")
	}

	jobIds, err := util.ParseJobIdSelectorList(jobStr, ",")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}

	rounded, _ := util.ParseFloatWithPrecision(strconv.FormatFloat(priority, 'f', 1, 64), 1)
	if rounded != priority {
		log.Warnf("Priority will be rounded to %.1f\n", rounded)
	}
	if rounded == 0 {
		log.Warnf("Mandated priority equals 0 means the scheduling priority will be calculated.")
	}

	req := &protos.ModifyJobRequest{
		Uid:       uint32(os.Getuid()),
		JobIds:    jobIds,
		Attribute: protos.ModifyJobRequest_Priority,
		Value: &protos.ModifyJobRequest_MandatedPriority{
			MandatedPriority: rounded,
		},
	}

	reply, err := stub.ModifyJob(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to change job priority")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedJobs) == 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return SummarizeReply(reply)
}

func ChangeJobExtraAttrs(jobStr string, valueMap map[UpdateJobParamFlags]string) error {
	selectors, err := util.ParseJobIdSelectorList(jobStr, ",")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}

	req := &protos.QueryJobsInfoRequest{
		FilterJobIds:               selectors,
		OptionIncludeCompletedJobs: false,
	}
	reply, err := util.QueryJobsInfo(context.Background(), stub, req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to query job information: %s", err)
	}

	if !reply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to retrieve information for job %s", jobStr))
	}

	if len(reply.JobInfoList) == 0 {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("%s is completed or does not exist", jobStr))
	}

	updateJobExtraAttr := func(origin string, JobParamvalMap map[UpdateJobParamFlags]string) (string, error) {
		var extraAttrsKeyMap = map[UpdateJobParamFlags]string{
			CommentTypeFlag:  "comment",
			MailUserTypeFlag: "mail.user",
			MailTypeTypeFlag: "mail.type",
		}
		var err error
		newJsonStr := origin
		for flag, key := range extraAttrsKeyMap {
			if newValue, exist := JobParamvalMap[flag]; exist {
				newJsonStr, err = sjson.Set(newJsonStr, key, newValue)
				if err != nil {
					return "", fmt.Errorf("set %s failed: %w", key, err)
				}
			}
		}
		return newJsonStr, nil
	}

	pdOrRJobMap := make(map[uint32]string)
	validJobList := map[util.JobIdentifier]bool{}
	for _, jobInfo := range reply.JobInfoList {
		if err := util.RequireJobDetails(jobInfo.QueryVisibility); err != nil {
			return err
		}
		newJsonStr, err := updateJobExtraAttr(jobInfo.ExtraAttr, valueMap)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Failed to set extra attributes JSON: %s", err)
		}
		pdOrRJobMap[jobInfo.JobId] = newJsonStr
		validJobList[util.JobIdentifierFromJobInfo(jobInfo)] = true
	}

	notGetInfoJobs := make([]string, 0)
	for _, selector := range selectors {
		id := util.JobIdentifierFromSelector(selector)
		if !validJobList[id] {
			notGetInfoJobs = append(notGetInfoJobs, id.String())
		}
	}
	if len(notGetInfoJobs) > 0 {
		log.Warnf("Job %s is completed or does not exist.\n", strings.Join(notGetInfoJobs, ", "))
	}

	request := &protos.ModifyJobsExtraAttrsRequest{
		Uid:            uint32(os.Getuid()),
		ExtraAttrsList: pdOrRJobMap,
	}

	rep, err := stub.ModifyJobsExtraAttrs(context.Background(), request)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to change job extra attrs")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(rep.NotModifiedJobs) == 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return SummarizeReply(rep)
}

func ChangeNodeState(nodeRegex string, state string, reason string) error {
	nodeNames, ok := util.ParseHostList(nodeRegex)
	if !ok {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid node pattern: %s.", nodeRegex))
	}

	if len(nodeNames) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "No node provided.")
	}

	var req = &protos.ModifyCranedStateRequest{}
	req.Uid = uint32(os.Getuid())
	req.CranedIds = nodeNames
	state = strings.ToLower(state)
	switch state {
	case "drain":
		if reason == "" {
			return util.NewCraneErr(util.ErrorCmdArg, "You must specify a reason when draining a node.")
		}
		req.NewState = protos.CranedControlState_CRANE_DRAIN
		req.Reason = reason
	case "resume":
		req.NewState = protos.CranedControlState_CRANE_NONE
	case "on":
		req.NewState = protos.CranedControlState_CRANE_POWERON
	case "off":
		req.NewState = protos.CranedControlState_CRANE_POWEROFF
	case "sleep":
		req.NewState = protos.CranedControlState_CRANE_SLEEP
	case "wake":
		req.NewState = protos.CranedControlState_CRANE_WAKE
	default:
		p := []string{"drain", "resume", "on", "off", "sleep", "wake"}
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid state given: %s. Valid states are: %s.", state, strings.Join(p, ", ")))
	}

	reply, err := stub.ModifyNode(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to modify node state")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedNodes) == 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return SummarizeReply(reply)
}

func ModifyPartitionAcl(partition string, isAllowedList bool, accounts string) error {
	var accountList []string
	accountList, _ = util.ParseStringParamList(accounts, ",")

	req := protos.ModifyPartitionAclRequest{
		Uid:           userUid,
		Partition:     partition,
		IsAllowedList: isAllowedList,
		Accounts:      accountList,
	}

	reply, err := stub.ModifyPartitionAcl(context.Background(), &req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to modify partition %s", partition)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if !reply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Modify partition %s failed: %s.",
			partition, util.ErrMsg(reply.GetCode())))
	}

	fmt.Printf("Modify partition %s succeeded.\n", partition)
	return nil
}

func CreateReservation() error {
	start_time, err := util.ParseTime(FlagStartTime)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}
	duration, err := util.ParseDurationStrToSeconds(FlagDuration)
	if err != nil || duration <= 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "Invalid duration specified.")
	}

	req := &protos.CreateReservationRequest{
		Uid:                  uint32(os.Getuid()),
		ReservationName:      FlagReservationName,
		StartTimeUnixSeconds: start_time.Unix(),
		DurationSeconds:      duration,
	}

	if FlagNodes != "" {
		req.CranedRegex = FlagNodes
	} else {
		if FlagPartitionName == "" {
			return util.NewCraneErr(util.ErrorCmdArg, "Partition name must be specified when no node regex is given.")
		}
	}

	if FlagPartitionName != "" {
		req.Partition = FlagPartitionName
	}

	if FlagNodeNum != 0 {
		req.NodeNum = FlagNodeNum
	}

	if FlagAccount != "" {
		req.AllowedAccounts, req.DeniedAccounts, err = util.ParsePosNegList(FlagAccount)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, err.Error())
		}
		if len(req.AllowedAccounts) > 0 && len(req.DeniedAccounts) > 0 {
			return util.NewCraneErr(util.ErrorCmdArg, "You can only specify either allowed or disallowed accounts.")
		}
		if len(req.AllowedAccounts) == 0 && len(req.DeniedAccounts) == 0 {
			return util.NewCraneErr(util.ErrorCmdArg, "Account can not be empty.")
		}
	}

	if FlagUser != "" {
		req.AllowedUsers, req.DeniedUsers, err = util.ParsePosNegList(FlagUser)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, err.Error())
		}
		if len(req.AllowedUsers) > 0 && len(req.DeniedUsers) > 0 {
			return util.NewCraneErr(util.ErrorCmdArg, "You can only specify either allowed or disallowed users.")
		}
	}

	reply, err := stub.CreateReservation(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to create reservation")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Printf("Reservation %s created successfully.\n", FlagReservationName)
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to create reservation: %s.", reply.GetReason()))
	}
}

func DeleteReservation(ReservationName string) error {
	if strings.ToUpper(ReservationName) == "ALL" {
		if !FlagForce {
			log.Errorf("To delete all reservations, you must set --force.")
			return &util.CraneError{Code: util.ErrorCmdArg}
		}
	}

	req := &protos.DeleteReservationRequest{
		Uid:             uint32(os.Getuid()),
		ReservationName: ReservationName,
		Force:           FlagForce,
	}
	reply, err := stub.DeleteReservation(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to delete reservation")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Printf("Reservation %s deleted successfully.\n", ReservationName)
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to delete reservation: %s.\n", reply.GetReason()))
	}

}

func ResetNextJobId(nextJobId uint32, nextJobDbId int64) error {
	req := &protos.ResetNextJobIdRequest{
		Uid:         uint32(os.Getuid()),
		NextJobId:   nextJobId,
		NextJobDbId: nextJobDbId,
	}

	reply, err := stub.ResetNextJobId(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to reset next job ID")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		}
		return &util.CraneError{Code: util.ErrorBackend}
	}

	if reply.GetOk() {
		fmt.Printf("Next job ID reset to %d (db_id=%d) successfully.\n", nextJobId, nextJobDbId)
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to reset next job ID: %s.", reply.GetReason()))
	}
	return nil
}

func ResetNextStepDbId() error {
	req := &protos.ResetNextStepDbIdRequest{
		Uid: uint32(os.Getuid()),
	}

	reply, err := stub.ResetNextStepDbId(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to reset next step DB ID")
	}

	if reply.GetOk() {
		fmt.Println("Step DB ID counters reset successfully.")
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to reset step DB ID: %s.", reply.GetReason()))
	}
	return nil
}

func PurgeJobHistory() error {
	req := &protos.PurgeJobHistoryRequest{
		Uid: uint32(os.Getuid()),
	}

	reply, err := stub.PurgeJobHistory(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to purge job history")
	}

	if reply.GetOk() {
		fmt.Println("Job history purged successfully.")
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to purge job history: %s.", reply.GetReason()))
	}
	return nil
}

func ResetPartitionAcl() error {
	req := &protos.ResetPartitionAclRequest{
		Uid:              uint32(os.Getuid()),
		ReloadFromConfig: true,
	}

	reply, err := stub.ResetPartitionAcl(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to reset partition ACLs")
	}

	if reply.GetOk() {
		fmt.Println("All partition ACLs reset successfully.")
	} else {
		return util.NewCraneErr(util.ErrorBackend, "Failed to reset partition ACLs.")
	}
	return nil
}

func UpdateTraceConfig(enabled *bool, level string, propagate bool) error {
	req := &protos.SetTraceConfigRequest{
		Uid:               uint32(os.Getuid()),
		Enabled:           enabled,
		PropagateToCraned: &propagate,
	}
	if level != "" {
		req.Level = &level
	}

	reply, err := stub.SetTraceConfig(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(
			util.ErrorNetwork, err, "Failed to update trace config")
	}
	if FlagJson {
		if util.IsSlurmOutputMode() {
			output, err := formatSlurmTraceConfigJSON(reply)
			if err != nil {
				return util.WrapCraneErr(util.ErrorInvalidFormat, "%v", err)
			}
			fmt.Println(output)
		} else {
			fmt.Println(util.FmtJson.FormatReply(reply))
		}
		return nil
	}
	if !reply.GetOk() {
		message := reply.GetReason()
		if len(reply.GetFailedCranedIds()) > 0 {
			failedNodesLabel := "failed craned nodes"
			if util.IsSlurmOutputMode() {
				failedNodesLabel = "failed nodes"
			}
			message += fmt.Sprintf("; %s: %s", failedNodesLabel,
				strings.Join(reply.GetFailedCranedIds(), ","))
		}
		return util.NewCraneErr(util.ErrorBackend, message)
	}

	fmt.Println("Trace config updated successfully.")
	printTraceConfig(reply.GetConfig())
	return nil
}

func EnableAutoPowerControl(nodeRegex string, enableStr string) error {
	nodeNames, ok := util.ParseHostList(nodeRegex)
	if !ok {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid node pattern: %s", nodeRegex))
	}

	if len(nodeNames) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "No node provided")
	}

	var enable bool
	enableStr = strings.ToLower(enableStr)
	switch enableStr {
	case "true", "yes", "1", "on", "enable":
		enable = true
	case "false", "no", "0", "off", "disable":
		enable = false
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid power-control value: %s. Valid values are: true/false, yes/no, 1/0, on/off, enable/disable", enableStr))
	}

	req := &protos.EnableAutoPowerControlRequest{
		Uid:       uint32(os.Getuid()),
		CranedIds: nodeNames,
		Enable:    enable,
	}

	reply, err := stub.EnableAutoPowerControl(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to modify node power control setting")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedNodes) > 0 {
			return &util.CraneError{Code: util.ErrorBackend}
		}
		return nil
	}

	if len(reply.ModifiedNodes) > 0 {
		action := "enabled for"
		if !enable {
			action = "disabled for"
		}
		modifiedNodesString := strings.Join(reply.ModifiedNodes, ", ")
		fmt.Printf("Auto power control %s nodes %s successfully.\n", action, modifiedNodesString)
	}

	if len(reply.NotModifiedNodes) > 0 {
		msg := ""
		for i := 0; i < len(reply.NotModifiedNodes); i++ {
			msg += fmt.Sprintf("Failed to modify node: %s. Reason: %s.\n",
				reply.NotModifiedNodes[i], reply.NotModifiedReasons[i])
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}

	return nil
}
