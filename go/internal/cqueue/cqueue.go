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

package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

var (
	stub protos.CraneCtldClient
)

func FillReqByCobraFlags() (*protos.QueryJobsInfoRequest, error) {
	req := protos.QueryJobsInfoRequest{
		OptionIncludeCompletedJobs: false,
		Mode:                       protos.QueryJobsInfoMode_QUERY_JOBS_INFO_QUEUE,
		OptionQueryStepsOnly:       FlagStep,
	}

	processors := []FilterProcessor{
		&StatesFilterProcessor{},
		&SelfProcessor{},
		&JobNamesProcessor{},
		&UserFilterProcessor{},
		&QosProcessor{},
		&AccountProcessor{},
		&PartitionsProcessor{},
		&JobIDsProcessor{},
		&StepIDsProcessor{},
		&LicensesProcessor{},
		&NodeNamesProcessor{},
	}

	for _, p := range processors {
		if err := p.Process(&req); err != nil {
			return nil, err
		}
	}

	if FlagNumLimit != 0 {
		req.NumLimit = FlagNumLimit
	}

	return &req, nil
}

func QueryJobsInfo() (*protos.QueryJobsInfoReply, error) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)

	req, err := FillReqByCobraFlags()
	if err != nil {
		return &protos.QueryJobsInfoReply{}, err
	}

	// set 10 seconds limit
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reply, err := util.QueryJobsInfo(ctx, stub, req)
	if err != nil {
		if rpcErr, ok := grpcstatus.FromError(err); ok {
			switch rpcErr.Code() {
			case grpccodes.DeadlineExceeded:
				return nil, util.NewCraneErrFromGrpc(util.ErrorBackend, err,
					"Query timed out due to large number of jobs. Please try with a smaller scope or use filters.",
				)
			case grpccodes.ResourceExhausted:
				return nil, util.NewCraneErrFromGrpc(util.ErrorNetwork, err,
					"Response too large for gRPC. Please reduce the query scope or avoid using -m with huge values.")
			default:
				return nil, util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query job queue")
			}
		} else {
			return nil, util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query job queue: %v", err)
		}
	}
	return reply, nil
}

func FillQueueStateSummaryReqByCobraFlags() (*protos.QueryQueueStateSummaryRequest, error) {
	stateList, err := util.ParseInRamJobStatusList(FlagFilterStates)
	if err != nil {
		return nil, util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}
	return &protos.QueryQueueStateSummaryRequest{FilterStates: stateList}, nil
}

func QueryQueueStateSummary() (*protos.QueryQueueStateSummaryReply, error) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)

	req, err := FillQueueStateSummaryReqByCobraFlags()
	if err != nil {
		return &protos.QueryQueueStateSummaryReply{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reply, err := stub.QueryQueueStateSummary(ctx, req)
	if err != nil {
		return nil, util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query queue state summary")
	}
	return reply, nil
}

func QueryQueueStateSummaryOutput() error {
	reply, err := QueryQueueStateSummary()
	if err != nil {
		return err
	}

	states := map[string]uint64{
		protos.JobStatus_Pending.String():    0,
		protos.JobStatus_Running.String():    0,
		protos.JobStatus_Completing.String(): 0,
	}
	for _, stateCount := range reply.GetStateCounts() {
		states[stateCount.GetState().String()] = stateCount.GetCount()
	}

	if FlagJson {
		out := struct {
			OK     bool              `json:"ok"`
			Total  uint64            `json:"total"`
			States map[string]uint64 `json:"states"`
		}{
			OK:     reply.GetOk(),
			Total:  reply.GetTotalCount(),
			States: states,
		}
		data, err := json.Marshal(out)
		if err != nil {
			return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to marshal queue count json: %v", err))
		}
		fmt.Println(string(data))
		return nil
	}

	if !FlagNoHeader {
		fmt.Println("STATE COUNT")
	}
	fmt.Printf("Total %d\n", reply.GetTotalCount())
	for _, stateCount := range reply.GetStateCounts() {
		fmt.Printf("%s %d\n", stateCount.GetState().String(), stateCount.GetCount())
	}
	return nil
}

func Query() error {
	if FlagCount {
		return QueryQueueStateSummaryOutput()
	}
	reply, err := QueryJobsInfo()
	if err != nil {
		return err
	}

	if FlagJson {
		return JsonOutput(reply)
	} else if FlagStep {
		return QueryStepsTableOutput(reply)
	} else {
		return QueryTableOutput(reply)
	}
}

func loopedQuery(iterate uint64) error {
	interval, err := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, "Invalid time interval.")
	}
	return loopedSubQuery(interval)
}

func loopedSubQuery(interval time.Duration) error {
	for {
		fmt.Println(time.Now().String()[0:19])
		if err := Query(); err != nil {
			return err
		}
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
