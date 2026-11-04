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

package cacctmgr

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

var (
	FlagAccount protos.AccountInfo
	FlagUser    protos.UserInfo
	FlagQos     = protos.QosInfo{
		MaxJobsPerUser:     math.MaxUint32,
		MaxCpusPerUser:     util.UnlimitedCpuValue,
		MaxTimeLimitPerJob: util.MaxJobTimeLimit,
	}
	FlagWckey protos.WckeyInfo

	FlagResourceName string
	FlagServerName   string
	FlagClusters     string

	FlagQosFlags uint32

	// FlagPartition and FlagSetPartition are different.
	// FlagPartition limits the operation to a specific partition,
	// while the other is the partition to be added or deleted.
	FlagPartition    string
	FlagSetPartition string

	// FlagSetLevel and FlagLevel are different as
	// they have different default values.
	FlagLevel             string = "none"
	FlagSetLevel          string
	FlagSetDefaultAccount string

	// UserInfo does not have these fields (while AccountInfo does),
	// so we use separate flags for them.
	FlagUserCoordinator bool
	FlagUserDefaultQos  string
	FlagUserPartitions  []string
	FlagUserQosList     []string

	FlagForce                bool
	FlagFull                 bool
	FlagJson                 bool
	FlagConfigFilePath       string = util.DefaultConfigPath
	FlagPluginConfigFilePath string = util.DefaultPluginConfigPath

	// These flags are implemented,
	// but not added to any cmd!
	FlagNoHeader bool
	FlagNodeList string
	FlagNumLimit uint32

	FlagShowPartitionLimit bool

	FlagEntityName          string
	FlagEntityAccount       string
	FlagEntityPartitions    string
	FlagDefaultQos          string
	FlagAllowedQosList      string
	FlagAllowedPartitions   string
	FlagDeleteQosList       string
	FlagDeletePartitionList string
	FlagSetQosList          string
	FlagSetPartitionList    string
	FlagPartitions          string
	FlagQosList             string
	FlagMaxCpu              string
	FlagMaxTimeLimit        string
	FlagPriority            string
	FlagAdminLevel          string
	FlagDescription         string
	FlagFormat              string
)

var actionToExecute = map[string]func(command *CAcctMgrCommand) error{
	"add":     executeAddCommand,
	"delete":  executeDeleteCommand,
	"block":   executeBlockCommand,
	"unblock": executeUnblockCommand,
	"modify":  executeModifyCommand,
	"show":    executeShowCommand,
	"reset":   executeResetCommand,
	"update":  executeModifyCommand,
}

type ModifyParam struct {
	ModifyField protos.ModifyField
	NewValue    string
	RequestType protos.OperationType
}

func validateUintValue(value string, fieldName string, bitSize int) error {
	_, err := strconv.ParseUint(value, 10, bitSize)
	if err != nil {
		return fmt.Errorf("invalid argument %s for %s flag: %v", value, fieldName, err)
	}
	return nil
}

// TODO(preempt): expose REQUEUE and SUSPEND once supported.
func parsePreemptMode(value string) (protos.PreemptMode, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "OFF":
		return protos.PreemptMode_PREEMPT_MODE_OFF, nil
	case "CANCEL":
		return protos.PreemptMode_PREEMPT_MODE_CANCEL, nil
	default:
		return protos.PreemptMode_PREEMPT_MODE_OFF,
			fmt.Errorf("invalid preempt mode %q: valid values are OFF, CANCEL", value)
	}
}

func ParseCmdArgs(args []string) {
	commandArgs := preParseGlobalFlags(args[1:])

	if len(commandArgs) == 0 {
		showHelp()
		os.Exit(0)
	}
	cmdStr := getCmdStringByArgs(commandArgs)
	command, err := ParseCAcctMgrCommand(cmdStr)

	if err != nil {
		log.Errorf("cacctmgr: error: command format is incorrect %v", err)
		os.Exit(util.ErrorCmdArg)
	}

	result := executeCommand(command)
	if result != nil {
		var craneError *util.CraneError
		if errors.As(result, &craneError) {
			if craneError.Message != "" {
				log.Errorf("cacctmgr: error: %s", strings.TrimSpace(craneError.Message))
			}
			os.Exit(craneError.Code)
		}
		log.Errorf("cacctmgr: error: %s", strings.TrimSpace(result.Error()))
		os.Exit(util.ErrorGeneric)
	} else {
		os.Exit(util.ErrorSuccess)
	}

}

func executeCommand(command *CAcctMgrCommand) error {
	config = util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	userUid = uint32(os.Getuid())

	action := command.GetAction()
	executeAction, exists := actionToExecute[action]
	if exists {
		return executeAction(command)
	} else {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown operation type: %s", action))
	}
}

func executeAddCommand(command *CAcctMgrCommand) error {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeAddAccountCommand(command)
	case "user":
		return executeAddUserCommand(command)
	case "qos":
		return executeAddQosCommand(command)
	case "wckey":
		return executeAddWckeyCommand(command)
	case "resource":
		return executeAddResourceCommand(command)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeAddAccountCommand(command *CAcctMgrCommand) error {
	// Reset FlagAccount to default values
	FlagAccount = protos.AccountInfo{}
	FlagAccount.Name = command.GetID()

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "name":
			FlagAccount.Name = value
		case "description":
			FlagAccount.Description = value
		case "parent":
			FlagAccount.ParentAccount = value
		case "defaultqos":
			FlagAccount.DefaultQos = value
		case "partition":
			FlagAccount.AllowedPartitions = strings.Split(value, ",")
		case "qoslist":
			FlagAccount.AllowedQosList = strings.Split(value, ",")
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return AddAccount(&FlagAccount)
}

func executeAddUserCommand(command *CAcctMgrCommand) error {
	FlagUser = protos.UserInfo{}
	FlagUser.Name = command.GetID()
	FlagUserPartitions = []string{}
	FlagLevel = "none"
	FlagUserCoordinator = false

	KVParams := command.GetKVMaps()

	err := checkEmptyKVParams(KVParams, []string{"account"})
	if err != nil {
		return err
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagUser.Account = value
		case "coordinator":
			FlagUserCoordinator = value == "true"
		case "level":
			FlagLevel = value
		case "partition":
			FlagUserPartitions = strings.Split(value, ",")
		case "name":
			FlagUser.Name = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return AddUser(&FlagUser, FlagUserPartitions, FlagLevel, FlagUserCoordinator)
}

func executeAddQosCommand(command *CAcctMgrCommand) error {
	FlagQos = protos.QosInfo{
		MaxJobsPerUser:          math.MaxUint32,
		MaxCpusPerUser:          util.UnlimitedCpuValue,
		MaxSubmitJobsPerUser:    math.MaxUint32,
		MaxSubmitJobsPerAccount: math.MaxUint32,
		MaxJobsPerAccount:       math.MaxUint32,
		MaxJobs:                 math.MaxUint32,
		MaxSubmitJobs:           math.MaxUint32,
		MaxWall:                 0,
		MaxTimeLimitPerJob:      util.MaxJobTimeLimit,
		Flags:                   util.QosFlagNone,
	}
	FlagQos.Name = command.GetID()

	FlagQos.MaxTresPerUser, _ = util.ParseTres("")
	FlagQos.MaxTresPerAccount, _ = util.ParseTres("")
	FlagQos.MaxTres, _ = util.ParseTres("")

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "name":
			FlagQos.Name = value
		case "description":
			FlagQos.Description = value
		case "priority":
			if err := validateUintValue(value, "priority", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			priority, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.Priority = uint32(priority)
		case "maxjobsperuser":
			if err := validateUintValue(value, "maxJobsPerUser", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			maxJobs, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxJobsPerUser = uint32(maxJobs)
		case "maxcpusperuser":
			maxCpus, err := strconv.ParseFloat(value, 64)
			if err != nil || maxCpus < 0 || maxCpus > util.UnlimitedCpuThreshold {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid value for maxCpusPerUser: %s", value))
			}
			FlagQos.MaxCpusPerUser = maxCpus
		case "maxtimelimitperjob":
			if seconds, err := util.ParseDurationStrToSeconds(value); err != nil {
				if err = validateUintValue(value, "maxTimeLimitPerJob", 64); err != nil {
					return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
				}
				maxTimeLimit, _ := strconv.ParseUint(value, 10, 64)
				FlagQos.MaxTimeLimitPerJob = maxTimeLimit
			} else {
				FlagQos.MaxTimeLimitPerJob = uint64(seconds)
			}
		case "maxsubmitjobsperuser":
			if err := validateUintValue(value, "maxSubmitJobsPerUser", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			maxSubmitJobsPerUser, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxSubmitJobsPerUser = uint32(maxSubmitJobsPerUser)
		case "maxsubmitjobsperaccount":
			if err := validateUintValue(value, "maxSubmitJobsPerAccount", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			maxSubmitJobsPerAccount, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxSubmitJobsPerAccount = uint32(maxSubmitJobsPerAccount)
		case "maxjobsperaccount":
			if err := validateUintValue(value, "maxJobsPerAccount", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			maxJobsPerAccount, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxJobsPerAccount = uint32(maxJobsPerAccount)
		case "maxtresperuser":
			var err error
			FlagQos.MaxTresPerUser, err = util.ParseTres(value)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s for %s flag: %v", value, "MaxTresPerUser", err))
			}
		case "maxtresperaccount":
			var err error
			FlagQos.MaxTresPerAccount, err = util.ParseTres(value)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s for %s flag: %v", value, "MaxTresPerAccount", err))
			}
		case "maxtres":
			var err error
			FlagQos.MaxTres, err = util.ParseTres(value)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s for %s flag: %v", value, "MaxTres", err))
			}
		case "maxjobs":
			if err := validateUintValue(value, "maxjobs", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			maxJobs, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxJobs = uint32(maxJobs)
		case "maxsubmitjobs":
			if err := validateUintValue(value, "maxsubmitjobs", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			maxsubmitjobs, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxSubmitJobs = uint32(maxsubmitjobs)
		case "maxwall":
			if err := validateUintValue(value, "maxWall", 64); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			maxWall, _ := strconv.ParseUint(value, 10, 64)
			FlagQos.MaxWall = maxWall
		case "flags":
			var err error
			if FlagQosFlags, err = util.ParseFlags(value); err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s ,err: %v", value, err))
			}
			FlagQos.Flags = FlagQosFlags
		case "preempt":
			preemptList, err := util.ParseStringParamListAllowEmpty(value, ",")
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid preempt list %q: %v\n", value, err))
			}
			FlagQos.Preempt = preemptList
		case "preemptmode":
			mode, err := parsePreemptMode(value)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("%v\n", err))
			}
			FlagQos.PreemptMode = mode
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}
	return AddQos(&FlagQos)
}

func executeAddWckeyCommand(command *CAcctMgrCommand) error {
	FlagWckey = protos.WckeyInfo{}
	KVParams := command.GetKVMaps()

	FlagWckey.Name = command.GetID()
	if FlagWckey.Name == "" {
		return util.NewCraneErr(util.ErrorCmdArg, "required entity wckey not set")
	}

	err := checkEmptyKVParams(KVParams, []string{"user"})
	if err != nil {
		return err
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "user":
			FlagWckey.UserName = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return AddWckey(&FlagWckey)
}

func executeAddResourceCommand(command *CAcctMgrCommand) error {
	FlagOperators := make(map[protos.LicenseResource_Field]string, 0)

	FlagResourceName = command.GetID()
	KVParams := command.GetKVMaps()

	err := checkEmptyKVParams(KVParams, []string{"server"})
	if err != nil {
		return err
	}

	hasCluster := false
	hasAllowed := false

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "name":
			FlagResourceName = value
		case "server":
			FlagServerName = value
		case "count":
			if err := validateUintValue(value, "count", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			FlagOperators[protos.LicenseResource_Count] = value
			if value == "0" {
				log.Warning("The total count of the license you entered is 0.")
			}
		case "description":
			FlagOperators[protos.LicenseResource_Description] = value
		case "lastconsumed":
			if err := validateUintValue(value, "lastConsumed", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)

			}
			FlagOperators[protos.LicenseResource_LastConsumed] = value
		case "servertype":
			FlagOperators[protos.LicenseResource_ServerType] = value
		case "type":
			FlagOperators[protos.LicenseResource_ResourceType] = value
		case "allowed":
			if err := validateUintValue(value, "allowed", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			FlagOperators[protos.LicenseResource_Allowed] = value
			hasAllowed = true
		case "cluster":
			FlagClusters = value
			hasCluster = true
		case "flags":
			FlagOperators[protos.LicenseResource_Flags] = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	if FlagResourceName == "" {
		return util.NewCraneErr(util.ErrorCmdArg, "required entity resource not set")
	}

	if hasCluster != hasAllowed {
		return util.NewCraneErr(util.ErrorCmdArg, "'allowed' and 'cluster' must be specified together")
	}

	return AddLicenseResource(FlagResourceName, FlagServerName, FlagClusters, FlagOperators)
}

func executeDeleteCommand(command *CAcctMgrCommand) error {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeDeleteAccountCommand(command)
	case "user":
		return executeDeleteUserCommand(command)
	case "qos":
		return executeDeleteQosCommand(command)
	case "wckey":
		return executeDeleteWckeyCommand(command)
	case "resource":
		return executeDeleteResourceCommand(command)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeDeleteAccountCommand(command *CAcctMgrCommand) error {
	FlagEntityName = command.GetID()

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		}
	}

	if FlagEntityName == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("required entity account not set"))
	}
	return DeleteAccount(FlagEntityName)
}

func executeDeleteUserCommand(command *CAcctMgrCommand) error {
	// Reset related flags
	FlagEntityName = command.GetID()
	FlagEntityAccount = ""

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		case "name":
			FlagEntityName = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	if FlagEntityName == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("required entity user not set"))
	}

	return DeleteUser(FlagEntityName, FlagEntityAccount)
}

func executeDeleteQosCommand(command *CAcctMgrCommand) error {
	// Reset FlagEntityName
	FlagEntityName = command.GetID()

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		}
	}

	if FlagEntityName == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("required entity qos not set"))
	}
	return DeleteQos(FlagEntityName)
}

func executeDeleteWckeyCommand(command *CAcctMgrCommand) error {
	FlagWckey = protos.WckeyInfo{}
	KVParams := command.GetKVMaps()

	FlagWckey.Name = command.GetID()
	if FlagWckey.Name == "" {
		return util.NewCraneErr(util.ErrorCmdArg, "required entity wckey not set")
	}

	// When deleting ALL, user param is not required
	if strings.ToUpper(FlagWckey.Name) != "ALL" {
		err := checkEmptyKVParams(KVParams, []string{"user"})
		if err != nil {
			return err
		}
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "user":
			FlagWckey.UserName = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}
	return DeleteWckey(FlagWckey.Name, FlagWckey.UserName)
}

func executeDeleteResourceCommand(command *CAcctMgrCommand) error {
	FlagEntityName = command.GetID()
	FlagServer := ""
	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		if strings.EqualFold(key, "name") {
			FlagEntityName = value
		}
	}
	if FlagEntityName == "" {
		return util.NewCraneErr(util.ErrorCmdArg, "required entity resource not set")
	}

	// When deleting ALL, server param is not required
	if strings.ToUpper(FlagEntityName) != "ALL" {
		err := checkEmptyKVParams(KVParams, []string{"server"})
		if err != nil {
			return err
		}
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		case "server":
			FlagServer = value
		case "cluster":
			FlagClusters = value
		}
	}
	return DeleteLicenseResource(FlagEntityName, FlagServer, FlagClusters)
}

func executeBlockCommand(command *CAcctMgrCommand) error {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeBlockAccountCommand(command)
	case "user":
		return executeBlockUserCommand(command)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeBlockAccountCommand(command *CAcctMgrCommand) error {
	// Reset related flags
	Name := command.GetID()
	FlagEntityAccount = ""

	if Name == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("required entity account not set"))
	}

	KVParams := command.GetKVMaps()

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return BlockAccountOrUser(Name, protos.EntityType_Account, FlagEntityAccount)
}

func executeBlockUserCommand(command *CAcctMgrCommand) error {
	// Reset related flags
	FlagEntityName = command.GetID()
	FlagEntityAccount = ""

	if FlagEntityName == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("required entity user not set"))
	}

	KVParams := command.GetKVMaps()

	err := checkEmptyKVParams(KVParams, []string{"account"})
	if err != nil {
		return err
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return BlockAccountOrUser(FlagEntityName, protos.EntityType_User, FlagEntityAccount)
}

func executeUnblockCommand(command *CAcctMgrCommand) error {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeUnblockAccountCommand(command)
	case "user":
		return executeUnblockUserCommand(command)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeUnblockAccountCommand(command *CAcctMgrCommand) error {
	// Reset related flags
	FlagEntityName = command.GetID()
	FlagEntityAccount = ""

	if FlagEntityName == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("required entity account not set"))
	}

	KVParams := command.GetKVMaps()

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return UnblockAccountOrUser(FlagEntityName, protos.EntityType_Account, FlagEntityAccount)
}

func executeUnblockUserCommand(command *CAcctMgrCommand) error {
	// Reset related flags
	FlagEntityName = command.GetID()
	FlagEntityAccount = ""

	if FlagEntityName == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("required entity user not set"))
	}

	KVParams := command.GetKVMaps()

	err := checkEmptyKVParams(KVParams, []string{"account"})
	if err != nil {
		return err
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return UnblockAccountOrUser(FlagEntityName, protos.EntityType_User, FlagEntityAccount)
}

func executeModifyCommand(command *CAcctMgrCommand) error {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeModifyAccountCommand(command)
	case "user":
		return executeModifyUserCommand(command)
	case "qos":
		return executeModifyQosCommand(command)
	case "wckey":
		return executeModifyWckeyCommand(command)
	case "resource":
		return executeModifyResourceCommand(command)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeModifyAccountCommand(command *CAcctMgrCommand) error {
	// Reset related flags
	FlagEntityName = ""
	FlagDescription = ""
	FlagDefaultQos = ""
	FlagSetPartitionList = ""
	FlagSetQosList = ""
	FlagSetDefaultAccount = ""
	FlagAllowedPartitions = ""
	FlagAllowedQosList = ""
	FlagDeletePartitionList = ""
	FlagDeleteQosList = ""
	FlagPartition = ""

	WhereParams := command.GetWhereParams()
	SetParams, AddParams, DeleteParams := command.GetSetParams()

	if len(WhereParams) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("modify account command requires 'where' clause to specify which account to modify"))
	}

	err := checkEmptyKVParams(WhereParams, []string{"name"})
	if err != nil {
		return err
	}

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		case "partition":
			FlagPartition = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown where parameter '%s' for account modification\n", key))
		}
	}

	if len(SetParams) == 0 && len(AddParams) == 0 && len(DeleteParams) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("modify account command requires 'set' clause to specify what to modify"))
	}

	var params []ModifyParam

	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "description":
			FlagDescription = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Description,
				NewValue:    FlagDescription,
				RequestType: protos.OperationType_Overwrite,
			})
		case "defaultqos":
			FlagDefaultQos = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_DefaultQos,
				NewValue:    FlagDefaultQos,
				RequestType: protos.OperationType_Overwrite,
			})
		case "allowedpartition":
			FlagSetPartitionList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Partition,
				NewValue:    FlagSetPartitionList,
				RequestType: protos.OperationType_Overwrite,
			})
		case "allowedqos":
			FlagSetQosList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Qos,
				NewValue:    FlagSetQosList,
				RequestType: protos.OperationType_Overwrite,
			})
		case "defaultaccount":
			FlagSetDefaultAccount = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_DefaultAccount,
				NewValue:    FlagSetDefaultAccount,
				RequestType: protos.OperationType_Overwrite,
			})
		case "maxsubmitjobs":
			if FlagPartition == "" {
				return util.NewCraneErr(util.ErrorCmdArg, "'maxSubmitJobs' can only be set for a specific partition. Please specify the partition in the where clause.\n")
			}
			if err := validateUintValue(value, "maxSubmitJobs", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxSubmitJobs,
				NewValue:    value,
				RequestType: protos.OperationType_Overwrite,
			})
		case "maxjobs":
			if FlagPartition == "" {
				return util.NewCraneErr(util.ErrorCmdArg, "'maxJobs' can only be set for a specific partition. Please specify the partition in the where clause.\n")
			}
			if err := validateUintValue(value, "maxJobs", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxJobs,
				NewValue:    value,
				RequestType: protos.OperationType_Overwrite,
			})
		case "maxtres":
			if FlagPartition == "" {
				return util.NewCraneErr(util.ErrorCmdArg, "'maxTres' can only be set for a specific partition. Please specify the partition in the where clause.\n")
			}
			if _, err := util.ParseTres(value); err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s for maxTres flag: %v\n", value, err))
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxTres,
				NewValue:    value,
				RequestType: protos.OperationType_Overwrite,
			})
		case "maxtresperjob":
			if FlagPartition == "" {
				return util.NewCraneErr(util.ErrorCmdArg, "'maxTresPerJob' can only be set for a specific partition. Please specify the partition in the where clause.\n")
			}
			if _, err := util.ParseTres(value); err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s for maxTresPerJob flag: %v\n", value, err))
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxTresPerJob,
				NewValue:    value,
				RequestType: protos.OperationType_Overwrite,
			})
		case "maxwall":
			if FlagPartition == "" {
				return util.NewCraneErr(util.ErrorCmdArg, "'maxWall' can only be set for a specific partition. Please specify the partition in the where clause.\n")
			}
			if err := validateUintValue(value, "maxWall", 64); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxWall,
				NewValue:    value,
				RequestType: protos.OperationType_Overwrite,
			})
		case "maxwallperjob":
			if FlagPartition == "" {
				return util.NewCraneErr(util.ErrorCmdArg, "'maxWallPerJob' can only be set for a specific partition. Please specify the partition in the where clause.\n")
			}
			if err := validateUintValue(value, "maxWallPerJob", 64); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxWallDurationPerJob,
				NewValue:    value,
				RequestType: protos.OperationType_Overwrite,
			})
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown set parameter '%s' for account modification\n", key))
		}
	}

	for key, value := range AddParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagAllowedPartitions = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Partition,
				NewValue:    FlagAllowedPartitions,
				RequestType: protos.OperationType_Add,
			})
		case "allowedqos":
			FlagAllowedQosList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Qos,
				NewValue:    FlagAllowedQosList,
				RequestType: protos.OperationType_Add,
			})
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown add parameter '%s' for account modification\n", key))
		}
	}

	for key, value := range DeleteParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagDeletePartitionList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Partition,
				NewValue:    FlagDeletePartitionList,
				RequestType: protos.OperationType_Delete,
			})
		case "allowedqos":
			FlagDeleteQosList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Qos,
				NewValue:    FlagDeleteQosList,
				RequestType: protos.OperationType_Delete,
			})
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown delete parameter '%s' for account modification\n", key))
		}
	}

	return ModifyAccount(params, FlagPartition, FlagEntityName)
}

func executeModifyUserCommand(command *CAcctMgrCommand) error {
	FlagEntityName = ""
	FlagEntityAccount = ""
	FlagEntityPartitions = ""
	FlagSetPartitionList = ""
	FlagSetQosList = ""
	FlagSetDefaultAccount = ""
	FlagAdminLevel = ""
	FlagAllowedPartitions = ""
	FlagAllowedQosList = ""
	FlagDeletePartitionList = ""
	FlagDeleteQosList = ""

	WhereParams := command.GetWhereParams()
	SetParams, AddParams, DeleteParams := command.GetSetParams()

	if len(WhereParams) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("modify user command requires 'where' clause to specify which user to modify"))
	}

	err := checkEmptyKVParams(WhereParams, []string{"name"})
	if err != nil {
		return err
	}

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		case "account":
			FlagEntityAccount = value
		case "partition":
			FlagEntityPartitions = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown where parameter '%s' for user modification\n", key))
		}
	}

	if len(SetParams) == 0 && len(AddParams) == 0 && len(DeleteParams) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("modify user command requires 'set' clause to specify what to modify"))
	}

	var params []ModifyParam
	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagSetPartitionList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Partition,
				NewValue:    FlagSetPartitionList,
				RequestType: protos.OperationType_Overwrite,
			})
		case "allowedqos":
			FlagSetQosList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Qos,
				NewValue:    FlagSetQosList,
				RequestType: protos.OperationType_Overwrite,
			})
		case "defaultaccount":
			FlagSetDefaultAccount = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_DefaultAccount,
				NewValue:    FlagSetDefaultAccount,
				RequestType: protos.OperationType_Overwrite,
			})
		case "defaultqos":
			FlagUserDefaultQos = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_DefaultQos,
				NewValue:    FlagUserDefaultQos,
				RequestType: protos.OperationType_Overwrite,
			})
		case "adminlevel":
			FlagAdminLevel = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_AdminLevel,
				NewValue:    FlagAdminLevel,
				RequestType: protos.OperationType_Overwrite,
			})
		case "maxsubmitjobs":
			if FlagEntityPartitions == "" {
				return util.NewCraneErr(util.ErrorCmdArg, "'maxSubmitJobs' can only be set for a specific partition. Please specify the partition in the where clause.\n")
			}
			if err := validateUintValue(value, "maxSubmitJobs", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxSubmitJobs,
				NewValue:    value,
				RequestType: protos.OperationType_Overwrite,
			})
		case "maxjobs":
			if FlagEntityPartitions == "" {
				return util.NewCraneErr(util.ErrorCmdArg, "'maxJobs' can only be set for a specific partition. Please specify the partition in the where clause.\n")
			}
			if err := validateUintValue(value, "maxJobs", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxJobs,
				NewValue:    value,
				RequestType: protos.OperationType_Overwrite,
			})
		case "maxtres":
			if FlagEntityPartitions == "" {
				return util.NewCraneErr(util.ErrorCmdArg, "'maxTres' can only be set for a specific partition. Please specify the partition in the where clause.\n")
			}
			if _, err := util.ParseTres(value); err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s for maxTres flag: %v\n", value, err))
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxTres,
				NewValue:    value,
				RequestType: protos.OperationType_Overwrite,
			})
		case "maxtresperjob":
			if FlagEntityPartitions == "" {
				return util.NewCraneErr(util.ErrorCmdArg, "'maxTresPerJob' can only be set for a specific partition. Please specify the partition in the where clause.\n")
			}
			if _, err := util.ParseTres(value); err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s for maxTresPerJob flag: %v\n", value, err))
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxTresPerJob,
				NewValue:    value,
				RequestType: protos.OperationType_Overwrite,
			})
		case "maxwall":
			if FlagEntityPartitions == "" {
				return util.NewCraneErr(util.ErrorCmdArg, "'maxWall' can only be set for a specific partition. Please specify the partition in the where clause.\n")
			}
			if err := validateUintValue(value, "maxWall", 64); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxWall,
				NewValue:    value,
				RequestType: protos.OperationType_Overwrite,
			})
		case "maxwallperjob":
			if FlagEntityPartitions == "" {
				return util.NewCraneErr(util.ErrorCmdArg, "'maxWallPerJob' can only be set for a specific partition. Please specify the partition in the where clause.\n")
			}
			if err := validateUintValue(value, "maxWallPerJob", 64); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxWallDurationPerJob,
				NewValue:    value,
				RequestType: protos.OperationType_Overwrite,
			})
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown set parameter '%s' for user modification\n", key))
		}
	}

	for key, value := range AddParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagAllowedPartitions = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Partition,
				NewValue:    FlagAllowedPartitions,
				RequestType: protos.OperationType_Add,
			})
		case "allowedqos":
			FlagAllowedQosList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Qos,
				NewValue:    FlagAllowedQosList,
				RequestType: protos.OperationType_Add,
			})
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown add parameter '%s' for user modification\n", key))
		}
	}

	for key, value := range DeleteParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagDeletePartitionList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Partition,
				NewValue:    FlagDeletePartitionList,
				RequestType: protos.OperationType_Delete,
			})
		case "allowedqos":
			FlagDeleteQosList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Qos,
				NewValue:    FlagDeleteQosList,
				RequestType: protos.OperationType_Delete,
			})
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown delete parameter '%s' for user modification\n", key))
		}
	}

	return ModifyUser(params, FlagEntityName, FlagEntityAccount, FlagEntityPartitions)
}

func executeModifyWckeyCommand(command *CAcctMgrCommand) error {
	FlagWckey = protos.WckeyInfo{}

	WhereParams := command.GetWhereParams()
	SetParams, AddParams, DeleteParams := command.GetSetParams()

	if len(WhereParams) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "modify wckey command requires 'where' clause to specify which user to modify")
	}

	err := checkEmptyKVParams(WhereParams, []string{"user"})
	if err != nil {
		return err
	}

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "user":
			FlagWckey.UserName = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown where parameter '%s' for wckey modification\n", key))
		}
	}

	if len(SetParams) == 0 || len(AddParams) != 0 || len(DeleteParams) != 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "modify wckey command requires only 'set' clause (add/delete not supported)")
	}

	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "defaultwckey":
			FlagWckey.Name = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown set parameter '%s' for wckey modification", key))
		}
	}
	if FlagWckey.Name == "" {
		return util.NewCraneErr(util.ErrorCmdArg, "modify wckey command requires non-empty 'defaultwckey'")
	}
	return ModifyDefaultWckey(FlagWckey.Name, FlagWckey.UserName)
}

func executeModifyQosCommand(command *CAcctMgrCommand) error {
	FlagEntityName = ""
	FlagMaxCpu = ""
	FlagMaxTimeLimit = ""
	FlagPriority = ""
	FlagDescription = ""

	WhereParams := command.GetWhereParams()
	SetParams, _, _ := command.GetSetParams()

	if len(WhereParams) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("modify qos command requires 'where' clause to specify which qos to modify"))
	}

	err := checkEmptyKVParams(WhereParams, []string{"name"})
	if err != nil {
		return err
	}

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown where parameter '%s' for qos modification\n", key))
		}
	}

	if len(SetParams) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("modify qos command requires 'set' clause to specify what to modify"))
	}
	var params []ModifyParam
	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "maxcpusperuser":
			maxCpus, err := strconv.ParseFloat(value, 64)
			if err != nil || maxCpus < 0 || maxCpus > util.UnlimitedCpuThreshold {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid value for maxCpusPerUser: %s", value))
			}
			FlagMaxCpu = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxCpusPerUser,
				NewValue:    FlagMaxCpu,
			})
		case "maxjobsperuser":
			if err := validateUintValue(value, "maxJobsPerUser", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxJobsPerUser,
				NewValue:    value,
			})
		case "maxsubmitjobsperuser":
			if err := validateUintValue(value, "maxSubmitJobsPerUser", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxSubmitJobsPerUser,
				NewValue:    value,
			})
		case "maxjobsperaccount":
			if err := validateUintValue(value, "maxJobsPerAccount", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxJobsPerAccount,
				NewValue:    value,
			})
		case "maxsubmitjobsperaccount":
			if err := validateUintValue(value, "maxSubmitJobsPerAccount", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxSubmitJobsPerAccount,
				NewValue:    value,
			})
		case "maxtresperuser":
			_, err := util.ParseTres(value)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s for %s flag: %v", value, "MaxTresPerUser\n", err))
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxTresPerUser,
				NewValue:    value,
			})
		case "maxtresperaccount":
			_, err := util.ParseTres(value)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s for %s flag: %v", value, "MaxTresPerAccount\n", err))
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxTresPerAccount,
				NewValue:    value,
			})
		case "maxtres":
			_, err := util.ParseTres(value)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s for %s flag: %v", value, "MaxTres", err))
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxTres,
				NewValue:    value,
			})
		case "maxjobs":
			if err := validateUintValue(value, "maxjobs", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxJobs,
				NewValue:    value,
			})
		case "maxsubmitjobs":
			if err := validateUintValue(value, "maxsubmitjobs", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxSubmitJobs,
				NewValue:    value,
			})
		case "maxwall":
			if err := validateUintValue(value, "maxwall", 64); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxWall,
				NewValue:    value,
			})
		case "maxtimelimitperjob":
			if seconds, err := util.ParseDurationStrToSeconds(value); err != nil {
				if err = validateUintValue(value, "maxTimeLimitPerJob", 64); err != nil {
					return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
				}
				FlagMaxTimeLimit = value
			} else {
				FlagMaxTimeLimit = fmt.Sprint(seconds)
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxTimeLimitPerJob,
				NewValue:    FlagMaxTimeLimit,
			})
		case "priority":
			if err := validateUintValue(value, "priority", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%s\n", err)
			}
			FlagPriority = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Priority,
				NewValue:    FlagPriority,
			})
		case "description":
			FlagDescription = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Description,
				NewValue:    FlagDescription,
			})
		case "flags":
			var err error
			if FlagQosFlags, err = util.ParseFlags(value); err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s ,err: %v", value, err))
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Flags,
				NewValue:    strconv.FormatUint(uint64(FlagQosFlags), 10),
			})
		case "preempt":
			if _, err := util.ParseStringParamListAllowEmpty(value, ","); err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid preempt list %q: %v\n", value, err))
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Preempt,
				NewValue:    value,
			})
		case "preemptmode":
			if _, err := parsePreemptMode(value); err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("%v\n", err))
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_ModifyPreemptMode,
				NewValue:    strings.ToUpper(value),
			})
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown set parameter '%s' for qos modification\n", key))
		}
	}
	return ModifyQos(params, FlagEntityName)
}

func executeModifyResourceCommand(command *CAcctMgrCommand) error {

	FlagOperators := make(map[protos.LicenseResource_Field]string, 0)

	KvParams := command.GetKVMaps()
	WhereParams := command.GetWhereParams()
	SetParams, _, _ := command.GetSetParams()

	if len(KvParams) == 0 && len(WhereParams) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "modify resource command requires 'where' clause to specify which resource to modify")
	}

	if len(KvParams) > 0 {
		err := checkEmptyKVParams(KvParams, []string{"name", "server"})
		if err != nil {
			return err
		}
	}

	for key, value := range KvParams {
		switch strings.ToLower(key) {
		case "name":
			FlagResourceName = value
		case "server":
			FlagServerName = value
		case "cluster":
			FlagClusters = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown where parameter '%s' for resource modification\n", key))

		}
	}

	if len(WhereParams) > 0 {
		err := checkEmptyKVParams(WhereParams, []string{"name", "server"})
		if err != nil {
			return err
		}
	}

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "name":
			FlagResourceName = value
		case "server":
			FlagServerName = value
		case "cluster":
			FlagClusters = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown where parameter '%s' for resource modification\n", key))
		}
	}

	if len(SetParams) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, ("modify resource command requires 'set' clause to specify what to modify"))

	}

	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "count":
			if err := validateUintValue(value, "count", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%v\n", err)

			}
			FlagOperators[protos.LicenseResource_Count] = value
		case "lastconsumed":
			if err := validateUintValue(value, "lastConsumed", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%v\n", err)
			}
			FlagOperators[protos.LicenseResource_LastConsumed] = value
		case "description":
			FlagOperators[protos.LicenseResource_Description] = value
		case "flags":
			FlagOperators[protos.LicenseResource_Flags] = value
		case "type":
			FlagOperators[protos.LicenseResource_ResourceType] = value
		case "allowed":
			if len(FlagClusters) == 0 {
				return util.NewCraneErr(util.ErrorCmdArg, "modify 'allowed' requires 'cluster' clause to specify which cluster resource to modify")

			}
			if err := validateUintValue(value, "allowed", 32); err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "%v\n", err)
			}
			FlagOperators[protos.LicenseResource_Allowed] = value
		case "servertype":
			FlagOperators[protos.LicenseResource_ServerType] = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown set parameter '%s' for resource modification\n", key))
		}
	}

	return ModifyResource(FlagResourceName, FlagServerName, FlagClusters, FlagOperators)
}

func executeShowCommand(command *CAcctMgrCommand) error {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeShowAccountCommand(command)
	case "user":
		return executeShowUserCommand(command)
	case "qos":
		return executeShowQosCommand(command)
	case "transaction":
		return executeShowTxnLogCommand(command)
	case "resource":
		return executeShowResourceCommand(command)
	case "event":
		return executeShowEventCommand(command)
	case "wckey":
		return executeShowWckeyCommand(command)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeShowAccountCommand(command *CAcctMgrCommand) error {
	name := command.GetID()
	if nameParam := command.GetFilterValue("name"); nameParam != "" {
		name = nameParam
	}
	if name == "" {
		return ShowAccounts()
	}

	return FindAccount(name)
}

func executeShowWckeyCommand(command *CAcctMgrCommand) error {
	wckeyList := command.GetID()
	return ShowWckey(wckeyList)
}

func executeShowUserCommand(command *CAcctMgrCommand) error {
	name := command.GetID()
	if nameParam := command.GetFilterValue("name"); nameParam != "" {
		name = nameParam
	}

	account := command.GetFilterValue("account")
	return ShowUser(name, account)
}

func executeShowEventCommand(command *CAcctMgrCommand) error {
	var FlagMaxLines int = 0
	nodesStr := ""
	maxLinesStr := ""
	updateMaxLines := false

	WhereParams := command.GetWhereParams()

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "maxlines":
			maxLinesStr = value
			updateMaxLines = true
		case "nodes":
			nodesStr = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown where parameter '%s' for show event", key))
		}
	}

	if updateMaxLines {
		var err error
		FlagMaxLines, err = strconv.Atoi(maxLinesStr)
		if err != nil || FlagMaxLines <= 0 {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid maxlines: '%s'\n", maxLinesStr))
		}
	}

	return QueryEventInfoByNodes(nodesStr, FlagMaxLines)
}

func executeShowQosCommand(command *CAcctMgrCommand) error {
	name := command.GetID()
	if nameParam := command.GetFilterValue("name"); nameParam != "" {
		name = nameParam
	}

	return ShowQos(name)
}

// Reset cert
func executeResetCommand(command *CAcctMgrCommand) error {
	name := command.GetID()
	if nameParam := command.GetKVParamValue("name"); nameParam != "" {
		name = nameParam
	}

	return ResetUserCredential(name)
}

func executeShowTxnLogCommand(command *CAcctMgrCommand) error {
	FlagActor := ""
	FlagTarget := ""
	FlagAction := ""
	FlagInfo := ""
	FlagStartTime := ""

	WhereParams := command.GetWhereParams()

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "actor":
			FlagActor = value
		case "target":
			FlagTarget = value
		case "action":
			FlagAction = value
		case "info":
			FlagInfo = value
		case "starttime":
			FlagStartTime = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown where parameter '%s' for show transaction\n", key))
		}
	}

	return ShowTxn(FlagActor, FlagTarget, FlagAction, FlagInfo, FlagStartTime)
}

func executeShowResourceCommand(command *CAcctMgrCommand) error {

	FlagWithClusters := command.GetID()

	FlagServer := ""

	WhereParams := command.GetWhereParams()
	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		case "server":
			FlagServer = value
		case "cluster":
			FlagClusters = value
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown where parameter '%s' for show resource\n", key))
		}
	}

	hasWithClusters := false
	if strings.ToLower(FlagWithClusters) == "withclusters" {
		hasWithClusters = true
	}

	return ShowLicenseResources(FlagEntityName, FlagServer, FlagClusters, hasWithClusters)
}

func checkEmptyKVParams(kvParams map[string]string, requiredFields []string) error {
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
			if len(missingFields) == 1 {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("required argument %s not set\n", missingFields[0]))
			} else {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("required arguments %s not set\n", strings.Join(missingFields, "\", \"")))
			}
		}
	}

	return nil
}

func init() {
	util.InitCraneLogger()
}
