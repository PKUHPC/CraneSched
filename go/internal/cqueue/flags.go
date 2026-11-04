package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os/user"
)

type FilterProcessor interface {
	Process(req *protos.QueryJobsInfoRequest) error
}

// FlagFilterStates
type StatesFilterProcessor struct{}

func (p *StatesFilterProcessor) Process(req *protos.QueryJobsInfoRequest) error {
	if FlagFilterStates == "" {
		return nil
	}
	stateList, err := util.ParseInRamJobStatusList(FlagFilterStates)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}
	req.FilterStates = stateList
	return nil
}

// FlagSelf
type SelfProcessor struct{}

func (p *SelfProcessor) Process(req *protos.QueryJobsInfoRequest) error {
	if !FlagSelf {
		return nil
	}
	cu, err := user.Current()
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to get current username: %s.", err))
	}
	req.FilterUsers = []string{cu.Username}
	return nil
}

// FlagFilterJobNames
type JobNamesProcessor struct{}

func (p *JobNamesProcessor) Process(req *protos.QueryJobsInfoRequest) error {
	if FlagFilterJobNames == "" {
		return nil
	}
	filterJobNameList, err := util.ParseStringParamList(FlagFilterJobNames, ",")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid job name list specified: %s.", err))
	}
	req.FilterJobNames = filterJobNameList
	return nil
}

// FlagFilterUsers
type UserFilterProcessor struct{}

func (p *UserFilterProcessor) Process(req *protos.QueryJobsInfoRequest) error {
	if FlagFilterUsers == "" {
		return nil
	}
	filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid user list specified: %s.", err))
	}
	req.FilterUsers = filterUserList
	return nil
}

// FlagFilterQos
type QosProcessor struct{}

func (p *QosProcessor) Process(req *protos.QueryJobsInfoRequest) error {
	if FlagFilterQos == "" {
		return nil
	}
	filterJobQosList, err := util.ParseStringParamList(FlagFilterQos, ",")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid Qos list specified: %s.", err))
	}
	req.FilterQos = filterJobQosList
	return nil
}

// FlagFilterAccounts
type AccountProcessor struct{}

func (p *AccountProcessor) Process(req *protos.QueryJobsInfoRequest) error {
	if FlagFilterAccounts == "" {
		return nil
	}
	filterAccountList, err := util.ParseStringParamList(FlagFilterAccounts, ",")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid account list specified: %s.", err))
	}
	req.FilterAccounts = filterAccountList
	return nil
}

// FlagFilterPartitions
type PartitionsProcessor struct{}

func (p *PartitionsProcessor) Process(req *protos.QueryJobsInfoRequest) error {
	if FlagFilterPartitions == "" {
		return nil
	}
	filterPartitionList, err := util.ParseStringParamList(FlagFilterPartitions, ",")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid partition list specified: %s.", err))
	}
	req.FilterPartitions = filterPartitionList
	return nil
}

// FlagFilterJobIDs
type JobIDsProcessor struct{}

func (p *JobIDsProcessor) Process(req *protos.QueryJobsInfoRequest) error {
	if FlagFilterJobIDs == "" {
		return nil
	}
	selectors, err := util.ParseJobIdSelectorList(FlagFilterJobIDs, ",")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid job list specified: %s.", err))
	}
	for _, selector := range selectors {
		if len(selector.Steps) > 0 {
			commandName := "cqueue"
			if util.IsSlurmOutputMode() {
				commandName = "squeue"
			}
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf(
				"Step selector is unsupported in %s --job, use --step for step queries.", commandName))
		}
	}

	req.FilterJobIds = selectors
	if !FlagStep {
		req.NumLimit = uint32(len(selectors))
		for _, selector := range selectors {
			if selector.ArrayTaskId == nil {
				req.NumLimit = 0
				break
			}
		}
	} else {
		req.NumLimit = 0
	}
	return nil
}

type StepIDsProcessor struct{}

func (p *StepIDsProcessor) Process(req *protos.QueryJobsInfoRequest) error {
	if FlagFilterStepIDs == "" {
		return nil
	}
	selectors, err := util.ParseJobIdSelectorList(FlagFilterStepIDs, ",")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid step list specified: %s.", err))
	}

	req.FilterJobIds = append(req.FilterJobIds, selectors...)
	req.NumLimit = 0
	return nil
}

// FlagFilterJobTypes
type JobTypesProcessor struct{}

func (p *JobTypesProcessor) Process(req *protos.QueryJobsInfoRequest) error {
	if FlagFilterJobTypes == "" {
		return nil
	}
	filterJobTypeList, err := util.ParseJobTypeList(FlagFilterJobTypes)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid job type list specified: %s.", err))
	}
	req.FilterJobTypes = filterJobTypeList
	return nil
}

// FlagFilterNodeNames
type NodeNamesProcessor struct{}

func (p *NodeNamesProcessor) Process(req *protos.QueryJobsInfoRequest) error {
	if FlagFilterNodeNames == "" {
		return nil
	}
	filterNodenameList, ok := util.ParseHostList(FlagFilterNodeNames)
	if !ok {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid node pattern: %s.", FlagFilterNodeNames))
	}
	req.FilterNodenameList = filterNodenameList
	return nil
}

// FlagFilterLicenses
type LicensesProcessor struct{}

func (p *LicensesProcessor) Process(req *protos.QueryJobsInfoRequest) error {
	if FlagFilterLicenses == "" {
		return nil
	}
	filterLicensesList, err := util.ParseStringParamList(FlagFilterLicenses, ",")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid licenses list specified: %s.", err))
	}
	req.FilterLicenses = filterLicensesList
	return nil
}
