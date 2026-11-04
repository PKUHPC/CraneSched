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
	"CraneFrontEnd/internal/util"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

var (
	FlagNodeName        string
	FlagState           string
	FlagReason          string
	FlagPowerEnable     string
	FlagPartitionName   string
	FlagAllowedAccounts string
	FlagDeniedAccounts  string
	FlagJobId           uint32
	FlagJobIds          string
	FlagQueryAll        bool
	FlagTimeLimit       string
	FlagPriority        float64
	FlagHoldTime        string
	FlagConfigFilePath  string = util.DefaultConfigPath
	FlagJson            bool
	FlagForce           bool
	FlagReservationName string
	FlagStartTime       string
	FlagDuration        string
	FlagNodes           string
	FlagAccount         string
	FlagUser            string
	FlagNodeNum         uint32
	FlagDeadlineTime    string
)

var actionToExecute = map[string]func(command *CControlCommand) error{
	"show":    executeShowCommand,
	"update":  executeUpdateCommand,
	"hold":    executeHoldCommand,
	"release": executeReleaseCommand,
	"suspend": executeSuspendCommand,
	"resume":  executeResumeCommand,
	"requeue": executeRequeueCommand,
	"create":  executeCreateCommand,
	"delete":  executeDeleteCommand,
	"reset":   executeResetCommand,
}

func ParseCmdArgs(args []string) {
	commandArgs := preParseGlobalFlags(args[1:])
	if len(commandArgs) == 0 {
		showHelp()
		os.Exit(0)
	}

	cmdStr := getCmdStringByArgs(commandArgs)
	command, err := ParseCControlCommand(cmdStr)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("ccontrol: error: command format is incorrect %v", err))
		os.Exit(util.ErrorCmdArg)
	}

	result := executeCommand(command)
	if result != nil {
		var craneError *util.CraneError
		if errors.As(result, &craneError) {
			message := strings.TrimSpace(craneError.Message)
			if message != "" {
				fmt.Fprintln(os.Stderr, fmt.Sprintf("ccontrol: error: %s", message))
			}
			os.Exit(craneError.Code)
		}
		message := strings.TrimSpace(result.Error())
		if message != "" {
			fmt.Fprintln(os.Stderr, fmt.Sprintf("ccontrol: error: %s", message))
		}
		os.Exit(util.ErrorGeneric)
	} else {
		os.Exit(util.ErrorSuccess)
	}
}

func executeCommand(command *CControlCommand) error {
	action := command.GetAction()
	if action == "show" && (command.GetEntity() == "config" ||
		(command.GetEntity() == "hostnames" && resolveHostlistArgument(command) != "")) {
		return executeShowCommand(command)
	}

	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	userUid = uint32(os.Getuid())

	executeAction, exists := actionToExecute[action]
	if exists {
		return executeAction(command)
	} else {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown operation type: %s\n", action))
	}
}

func executeShowCommand(command *CControlCommand) error {
	entity := command.GetEntity()
	switch entity {
	case "node":
		return executeShowNodeCommand(command)
	case "partition":
		return executeShowPartitionCommand(command)
	case "job":
		return executeShowJobCommand(command)
	case "step":
		return executeShowStepCommand(command)
	case "reservation":
		return executeShowReservationCommand(command)
	case "lic":
		return executeShowLicenseCommand(command)
	case "hostnames":
		return executeShowHostnamesCommand(command)
	case "trace":
		return executeShowTraceCommand(command)
	case "config":
		return executeShowConfigCommand(command)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeShowHostnamesCommand(command *CControlCommand) error {
	hostlist := resolveHostlistArgument(command)
	if hostlist == "" {
		reply, err := getCranedNodesReply("")
		if err != nil {
			return err
		}
		if err := printHostnames(os.Stdout, nodeHostnames(reply.CranedInfoList)); err != nil {
			return util.WrapCraneErr(util.ErrorSystem, "failed to write hostnames: %s", err)
		}
		return nil
	}

	hostnames, err := expandHostlist(hostlist)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}
	if err := printHostnames(os.Stdout, hostnames); err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "failed to write hostnames: %s", err)
	}
	return nil
}

func resolveHostlistArgument(command *CControlCommand) string {
	if hostlist := unquoteIfQuoted(command.GetID()); hostlist != "" {
		return hostlist
	}
	for _, name := range []string{
		"SLURM_JOB_NODELIST",
		"SLURM_NODELIST",
		"CRANE_JOB_NODELIST",
		"CRANE_NODELIST",
	} {
		if hostlist := os.Getenv(name); hostlist != "" {
			return hostlist
		}
	}
	return ""
}

func executeShowNodeCommand(command *CControlCommand) error {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
		name = ""
	}
	err := ShowNodes(name, FlagQueryAll)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "show nodes failed: %s\n", err)
	}
	return nil
}

func executeShowPartitionCommand(command *CControlCommand) error {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
	}
	err := ShowPartitions(name, FlagQueryAll)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "show partitions failed: %s\n", err)
	}
	return nil
}

func executeShowJobCommand(command *CControlCommand) error {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
	}

	err := ShowJobs(name, FlagQueryAll)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "show job failed: %s\n", err)
	}
	return nil
}
func executeShowStepCommand(command *CControlCommand) error {
	name := command.GetID()

	if len(name) == 0 {
		FlagQueryAll = true

	}

	err := ShowSteps(name, FlagQueryAll)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "show steps failed: %s", err)
	}
	return nil
}

func executeShowReservationCommand(command *CControlCommand) error {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
		name = ""
	}
	err := ShowReservations(name, FlagQueryAll)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "show reservations failed: %s", err)
	}
	return nil
}

func executeShowLicenseCommand(command *CControlCommand) error {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
		name = ""
	}
	err := ShowLicenses(name, FlagQueryAll)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "show licenses failed: %s", err)
	}
	return nil
}

func executeShowTraceCommand(command *CControlCommand) error {
	if err := ShowTraceConfig(); err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "show trace failed: %s", err)
	}
	return nil
}

func executeShowConfigCommand(command *CControlCommand) error {
	// ShowConfig already returns *util.CraneError with a specific code.
	return ShowConfig(FlagConfigFilePath)
}

func executeUpdateCommand(command *CControlCommand) error {
	kvParams := command.GetKVMaps()
	for key := range kvParams {
		if strings.EqualFold(key, "trace") {
			delete(kvParams, key)
			return executeUpdateTraceCommand(kvParams)
		}
	}

	for key := range kvParams {
		lowerKey := strings.ToLower(key)
		if lowerKey == "node" || lowerKey == "nodename" {
			FlagNodeName = kvParams[key]
			return executeUpdateNodeCommand(command)
		}
	}
	for key := range kvParams {
		lowerKey := strings.ToLower(key)
		if lowerKey == "job" || lowerKey == "jobid" {
			FlagJobIds = kvParams[key]
			return executeUpdateJobCommand(command)
		}
	}
	for key := range kvParams {
		lowerKey := strings.ToLower(key)
		if lowerKey == "partition" || lowerKey == "partitionname" {
			FlagPartitionName = kvParams[key]
			return executeUpdatePartitionCommand(command)
		}
	}
	return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("unknown attribute to modify"))
}

func executeUpdateNodeCommand(command *CControlCommand) error {
	kvParams := command.GetKVMaps()

	if err := checkEmptyKVParams(kvParams, []string{"state", "reason"}); err != nil {
		return err
	}

	if FlagNodeName == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("node name not specified"))
	}

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "state":
			FlagState = value
		case "reason":
			FlagReason = value
		case "nodename", "node":
			continue
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown attribute to modify: %s\n", key))
		}
	}
	err := ChangeNodeState(FlagNodeName, FlagState, FlagReason)
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "change node state failed: %s\n", err)
	}
	return nil
}

func executeUpdateJobCommand(command *CControlCommand) error {
	kvParams := command.GetKVMaps()

	var craneError = util.NewCraneErr(util.ErrorSuccess, "")

	var jobParamFlags UpdateJobParamFlags
	jobParamValuesMap := make(map[UpdateJobParamFlags]string)
	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "priority":
			jobParamFlags |= PriorityTypeFlag
			jobParamValuesMap[PriorityTypeFlag] = value
		case "timelimit":
			jobParamFlags |= TimelimitTypeFlag
			jobParamValuesMap[TimelimitTypeFlag] = value
		case "comment":
			jobParamFlags |= CommentTypeFlag
			jobParamValuesMap[CommentTypeFlag] = value
		case "mailuser":
			jobParamFlags |= MailUserTypeFlag
			jobParamValuesMap[MailUserTypeFlag] = value
		case "mailtype":
			if !util.CheckMailType(value) {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid mailtype value to modify: %s\n", value))
			}
			jobParamFlags |= MailTypeTypeFlag
			jobParamValuesMap[MailTypeTypeFlag] = value
		case "deadline":
			jobParamFlags |= DeadlineTypeFlag
			jobParamValuesMap[DeadlineTypeFlag] = value
		case "jobid", "job":
			continue
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown attribute to modify: %s\n", key))
		}
	}

	if jobParamFlags&PriorityTypeFlag != 0 {
		value := jobParamValuesMap[PriorityTypeFlag]
		priority, err := strconv.ParseFloat(value, 64)
		if err != nil {
			craneError.Code = util.ErrorCmdArg
		}
		FlagPriority = priority
		result := ChangeJobPriority(FlagJobIds, FlagPriority)
		if result != nil {
			craneError.Message += fmt.Sprintf("change job priority failed: %s\n", result.Error())
			craneError.Code = util.ErrorGeneric
		}
	}

	if jobParamFlags&TimelimitTypeFlag != 0 {
		FlagTimeLimit = jobParamValuesMap[TimelimitTypeFlag]
		err := ChangeJobTimeLimit(FlagJobIds, FlagTimeLimit)
		if err != nil {
			craneError.Message += fmt.Sprintf("change job time limit failed: %s\n", err.Error())
			craneError.Code = util.ErrorGeneric
		}
	}

	if jobParamFlags&(CommentTypeFlag|MailUserTypeFlag|MailTypeTypeFlag) != 0 {
		err := ChangeJobExtraAttrs(FlagJobIds, jobParamValuesMap)
		if err != nil {
			craneError.Message += fmt.Sprintf("change job ExtraAttrs failed: %s\n", err.Error())
			craneError.Code = util.ErrorGeneric
		}
	}

	if jobParamFlags&DeadlineTypeFlag != 0 {
		FlagDeadlineTime = jobParamValuesMap[DeadlineTypeFlag]
		err := ChangeDeadlineTime(FlagJobIds, FlagDeadlineTime)
		if err != nil {
			craneError.Message += fmt.Sprintf("change task deadline failed: %s", err.Error())
			craneError.Code = util.ErrorGeneric
		}
	}

	return craneError
}

func executeUpdateTraceCommand(kvParams map[string]string) error {
	if len(kvParams) == 0 {
		return util.NewCraneErr(
			util.ErrorCmdArg,
			"trace update requires enabled=<bool> or level=<basic|detailed|debug>")
	}

	var enabled *bool
	level := ""
	propagate := true
	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "enabled":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return util.NewCraneErr(
					util.ErrorCmdArg,
					fmt.Sprintf("invalid enabled value: %s", value))
			}
			enabled = &parsed
		case "level":
			level = strings.ToLower(value)
			switch level {
			case "basic", "detailed", "debug":
			default:
				return util.NewCraneErr(
					util.ErrorCmdArg,
					fmt.Sprintf("invalid trace level: %s", value))
			}
		case "propagate", "propagatetocraned":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return util.NewCraneErr(
					util.ErrorCmdArg,
					fmt.Sprintf("invalid propagate value: %s", value))
			}
			propagate = parsed
		default:
			return util.NewCraneErr(
				util.ErrorCmdArg, fmt.Sprintf("unknown trace attribute: %s", key))
		}
	}

	return UpdateTraceConfig(enabled, level, propagate)
}

func executeUpdatePartitionCommand(command *CControlCommand) error {
	kvParams := command.GetKVMaps()

	if err := checkEmptyKVParams(kvParams, nil); err != nil {
		return err
	}

	if FlagPartitionName == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("partition name not specified"))
	}

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "accounts", "allowedaccounts":
			FlagAllowedAccounts = value
			if err := ModifyPartitionAcl(FlagPartitionName, true, FlagAllowedAccounts); err != nil {
				return util.WrapCraneErr(util.ErrorGeneric, "%s\n", err)
			}
		case "deniedaccounts":
			FlagDeniedAccounts = value
			if err := ModifyPartitionAcl(FlagPartitionName, false, FlagDeniedAccounts); err != nil {
				return util.WrapCraneErr(util.ErrorGeneric, "%s\n", err)
			}
			log.Warning("Hint: When using AllowedAccounts, DeniedAccounts will not take effect.")
		case "partitionname", "partition":
			continue
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown attribute to modify: %s\n", key))
		}
	}

	return nil
}

func executeHoldCommand(command *CControlCommand) error {
	jobIds := command.GetID()

	timeLimit := command.GetKVParamValue("timelimit")
	if len(timeLimit) == 0 {
		log.Debug("no time limit specified")
	}

	if jobIds == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no job id specified"))
	}

	FlagHoldTime = timeLimit

	err := HoldReleaseJobs(jobIds, true)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "hold jobs failed: %s\n", err)
	}
	return nil
}

func executeRequeueCommand(command *CControlCommand) error {
	jobIds := command.GetID()
	if jobIds == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no job id specified"))
	}
	err := RequeueJobs(jobIds)
	if err != nil {
		var craneErr *util.CraneError
		if errors.As(err, &craneErr) {
			return craneErr
		}
		return util.WrapCraneErr(util.ErrorGeneric, "requeue jobs failed: %s\n", err)
	}
	return nil
}

func executeReleaseCommand(command *CControlCommand) error {
	jobIds := command.GetID()
	if jobIds == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no job id specified"))
	}

	err := HoldReleaseJobs(jobIds, false)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "release jobs failed: %s\n", err)
	}
	return nil
}

func executeSuspendCommand(command *CControlCommand) error {
	jobIds := command.GetID()
	if jobIds == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no job id specified"))
	}

	err := SuspendJobs(jobIds)
	if err != nil {
		if FlagJson {
			return err
		}
		return util.WrapCraneErr(util.ErrorGeneric, "suspend jobs failed: %s\n", err)
	}
	return nil
}

func executeResumeCommand(command *CControlCommand) error {
	jobIds := command.GetID()
	if jobIds == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no job id specified"))
	}

	err := ResumeJobs(jobIds)
	if err != nil {
		if FlagJson {
			return err
		}
		return util.WrapCraneErr(util.ErrorGeneric, "resume jobs failed: %s\n", err)
	}
	return nil
}

func executeCreateCommand(command *CControlCommand) error {
	entity := command.GetEntity()
	switch entity {
	case "reservation":
		return executeCreateReservationCommand(command)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeCreateReservationCommand(command *CControlCommand) error {
	FlagReservationName = command.GetID()
	if len(FlagReservationName) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no reservation name specified"))
	}

	kvParams := command.GetKVMaps()

	if err := checkEmptyKVParams(kvParams, []string{"starttime", "duration", "account"}); err != nil {
		return err
	}

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "starttime":
			FlagStartTime = value
		case "partition":
			FlagPartitionName = value
		case "duration":
			FlagDuration = value
		case "nodes":
			FlagNodes = value
		case "account":
			FlagAccount = value
		case "user":
			FlagUser = value
		case "nodecnt":
			nodeNum, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid nodenum value: %s\n", value))
			}
			FlagNodeNum = uint32(nodeNum)
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown attribute to modify: %s\n", key))
		}
	}

	err := CreateReservation()
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "%s\n", err)
	}
	return nil
}

func executeDeleteCommand(command *CControlCommand) error {
	entity := command.GetEntity()
	switch entity {
	case "reservation":
		return executeDeleteReservationCommand(command)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeDeleteReservationCommand(command *CControlCommand) error {
	name := command.GetID()
	if len(name) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no reservation name specified"))
	}

	err := DeleteReservation(name)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "%s\n", err)
	}
	return nil
}

func executeResetCommand(command *CControlCommand) error {
	entity := command.GetEntity()
	switch entity {
	case "next-job-id":
		return executeResetNextJobIdCommand(command)
	case "next-job-db-id":
		return executeResetNextJobDbIdCommand(command)
	case "partition-acl":
		if err := ResetPartitionAcl(); err != nil {
			return util.WrapCraneErr(util.ErrorGeneric, "reset partition-acl failed: %s", err)
		}
		return nil
	case "next-step-db-id":
		if err := ResetNextStepDbId(); err != nil {
			return util.WrapCraneErr(util.ErrorGeneric, "reset next-step-db-id failed: %s", err)

		}
		return nil
	case "job-history":
		if err := PurgeJobHistory(); err != nil {
			return util.WrapCraneErr(util.ErrorGeneric, "reset job-history failed: %s", err)
		}
		return nil
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type for reset: %s", entity))
	}
}

func executeResetNextJobIdCommand(command *CControlCommand) error {
	var value uint32 = 1
	if id := command.GetID(); id != "" {
		v, err := strconv.ParseUint(id, 10, 32)
		if err != nil || v == 0 {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid value: %s (must be a positive integer)", id))
		}
		value = uint32(v)
	}

	// next_job_id = value, next_job_db_id = 0 (don't change)
	err := ResetNextJobId(value, 0)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "reset next-job-id failed: %s", err)
	}
	return nil
}

func executeResetNextJobDbIdCommand(command *CControlCommand) error {
	var value int64 = 1
	if id := command.GetID(); id != "" {
		v, err := strconv.ParseInt(id, 10, 64)
		if err != nil || v <= 0 {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid value: %s (must be a positive integer)", id))
		}
		value = v
	}

	// next_job_id = 0 (don't change), next_job_db_id = value
	err := ResetNextJobId(0, value)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "reset next-job-db-id failed: %s", err)
	}
	return nil
}

func checkEmptyKVParams(kvParams map[string]string, requiredFields []string) error {
	if len(kvParams) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no attributes to modify"))
	}

	if len(requiredFields) > 0 {
		missingFields := []string{}
		for _, field := range requiredFields {
			found := false
			for key := range kvParams {
				if strings.ToLower(key) == field {
					found = true
					break
				}
			}
			if !found {
				missingFields = append(missingFields, field)
			}
		}

		if len(missingFields) > 0 {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("missing required fields: %s\n", strings.Join(missingFields, ", ")))
		}
	}

	return nil
}

func init() {
	util.InitCraneLogger()
}
