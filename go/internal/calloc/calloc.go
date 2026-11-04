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

package calloc

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GlobalVariables struct {
	user      *user.User
	cwd       string
	shellPath string

	globalCtx       context.Context
	globalCtxCancel context.CancelFunc

	connectionBroken bool
}

var gVars GlobalVariables

type StateOfCalloc int

const (
	ConnectCfored StateOfCalloc = 0
	ReqJobId      StateOfCalloc = 1
	WaitRes       StateOfCalloc = 2
	JobRunning    StateOfCalloc = 3
	JobKilling    StateOfCalloc = 4
	WaitAck       StateOfCalloc = 5
)

type ReplyReceiveItem struct {
	reply *protos.StreamCallocReply
	err   error
}

func ReplyReceiveRoutine(stream protos.CraneForeD_CallocStreamClient,
	replyChannel chan ReplyReceiveItem) {
	for {
		cforedReply, err := stream.Recv()
		replyChannel <- ReplyReceiveItem{
			reply: cforedReply,
			err:   err,
		}
		if err != nil {
			if err != io.EOF {
				log.Errorf("Failed to receive CforedReply: %s. "+
					"Connection to Cfored is broken. "+
					"ReplyReceiveRoutine is exiting...", err)
			}
			break
		}
	}
}

func StartCallocStream(job *protos.JobToCtld) error {
	config := util.ParseConfig(FlagConfigFilePath)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithKeepaliveParams(util.ClientKeepAliveParams))
	opts = append(opts, grpc.WithConnectParams(util.ClientConnectParams))

	unixSocketPath := "unix:///" + config.CranedCforedSockPath
	conn, err := grpc.NewClient(unixSocketPath, opts...)
	if err != nil {
		return util.NewCraneErr(util.ErrorBackend,
			fmt.Sprintf("Failed to connect to local unix socket %s: %s.", unixSocketPath, err))
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Errorf("Failed to close grpc conn: %s.", err)
			os.Exit(util.ErrorNetwork)
		}
	}(conn)

	client := protos.NewCraneForeDClient(conn)

	var stream protos.CraneForeD_CallocStreamClient
	var replyChannel chan ReplyReceiveItem

	var request *protos.StreamCallocRequest
	var jobId uint32
	var stepId uint32

	terminalExitChannel := make(chan bool, 1)
	cancelRequestChannel := make(chan bool, 1)

	state := ConnectCfored

CallocStateMachineLoop:
	for {
		switch state {
		case ConnectCfored:
			stream, err = client.CallocStream(gVars.globalCtx)
			if err != nil {
				log.Errorf("Failed to create CallocStream: %s.", err)
				break CallocStateMachineLoop
			}

			replyChannel = make(chan ReplyReceiveItem, 8)
			go ReplyReceiveRoutine(stream, replyChannel)

			request = &protos.StreamCallocRequest{
				Type: protos.StreamCallocRequest_JOB_REQUEST,
				Payload: &protos.StreamCallocRequest_PayloadJobReq{
					PayloadJobReq: &protos.StreamCallocRequest_JobReq{
						Job:       job,
						CallocPid: int32(os.Getpid()),
					},
				},
			}

			if err := stream.Send(request); err != nil {
				log.Errorf("Failed to send Job Request to CallocStream: %s. "+
					"Connection to calloc is broken", err)
				gVars.connectionBroken = true
				break CallocStateMachineLoop
			}

			state = ReqJobId

		case ReqJobId:
			item := <-replyChannel
			cforedReply, err := item.reply, item.err

			if err != nil {
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Errorf("Connection to Cfored broken when requesting "+
						"job id: %s. Exiting...", err)
					gVars.connectionBroken = true
					break CallocStateMachineLoop
				}
			}

			if cforedReply.Type != protos.StreamCallocReply_JOB_ID_REPLY {
				return util.NewCraneErr(util.ErrorBackend, "Expect type JOB_ID_REPLY.")
			}
			payload := cforedReply.GetPayloadJobIdReply()

			if payload.Ok {
				jobId = payload.JobId
				stepId = payload.StepId
				if !FlagQuiet {
					fmt.Printf("Job id allocated: %d\n", jobId)
				}
				state = WaitRes
			} else {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to allocate job id: %s.\n", payload.FailureReason)
				break CallocStateMachineLoop
			}

		case WaitRes:
			item := <-replyChannel
			cforedReply, err := item.reply, item.err

			if err != nil { // Failure Edge
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Errorf("Connection to Cfored broken when waiting "+
						"resource allocated: %s. Exiting...", err)
					gVars.connectionBroken = true
					break CallocStateMachineLoop
				}
			}

			switch cforedReply.Type {
			case protos.StreamCallocReply_JOB_RES_ALLOC_REPLY:
				cforedPayload := cforedReply.GetPayloadJobAllocReply()
				Ok := cforedPayload.Ok

				if Ok {
					if !FlagQuiet {
						nodeLabel := "craned nodes"
						if util.IsSlurmOutputMode() {
							nodeLabel = "nodes"
						}
						fmt.Printf("Allocated %s: %s.\n", nodeLabel, cforedPayload.AllocatedCranedRegex)
					}
					state = JobRunning
				} else {
					fmt.Println("Failed to allocate job resource. Exiting...")
					break CallocStateMachineLoop
				}

			case protos.StreamCallocReply_JOB_CANCEL_REQUEST:
				log.Tracef("Receive cancel request when wait res")
				state = JobKilling
			}

		case JobRunning:
			go StartTerminal(gVars.shellPath, jobId, cancelRequestChannel, terminalExitChannel)

			select {
			case <-terminalExitChannel:
				request = &protos.StreamCallocRequest{
					Type: protos.StreamCallocRequest_JOB_COMPLETION_REQUEST,
					Payload: &protos.StreamCallocRequest_PayloadJobCompleteReq{
						PayloadJobCompleteReq: &protos.StreamCallocRequest_JobCompleteReq{
							JobId:  jobId,
							StepId: stepId,
							Status: protos.JobStatus_Completed,
						},
					},
				}

				log.Debug("Sending JOB_COMPLETION_REQUEST with COMPLETED state...")
				if err := stream.Send(request); err != nil {
					log.Errorf("The connection to Cfored was broken: %s. "+
						"Exiting...", err)
					gVars.connectionBroken = true
					break CallocStateMachineLoop
				} else {
					state = WaitAck
				}

			case item := <-replyChannel:
				cforedReply, err := item.reply, item.err
				if err != nil {
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Errorf("The connection to Cfored was broken: %s. "+
							"Killing job...", err)
						gVars.connectionBroken = true
						state = JobKilling
					}
				} else {
					switch cforedReply.Type {
					case protos.StreamCallocReply_JOB_CANCEL_REQUEST:
						state = JobKilling

					case protos.StreamCallocReply_JOB_COMPLETION_ACK_REPLY:
						fmt.Println("Job failed ")
					}
				}

				cancelRequestChannel <- true
				<-terminalExitChannel
				if cforedReply.Type == protos.StreamCallocReply_JOB_COMPLETION_ACK_REPLY {
					break CallocStateMachineLoop
				}
			}

		case JobKilling:

			request = &protos.StreamCallocRequest{
				Type: protos.StreamCallocRequest_JOB_COMPLETION_REQUEST,
				Payload: &protos.StreamCallocRequest_PayloadJobCompleteReq{
					PayloadJobCompleteReq: &protos.StreamCallocRequest_JobCompleteReq{
						JobId:  jobId,
						StepId: stepId,
						Status: protos.JobStatus_Cancelled,
					},
				},
			}

			if gVars.connectionBroken {
				break CallocStateMachineLoop
			}

			log.Debug("Sending JOB_COMPLETION_REQUEST with CANCELLED state...")
			if err := stream.Send(request); err != nil {
				log.Errorf("The connection to Cfored was broken: %s. "+
					"Exiting...", err)
				gVars.connectionBroken = true
				break CallocStateMachineLoop
			} else {
				state = WaitAck
			}

		case WaitAck:
			item := <-replyChannel
			cforedReply, err := item.reply, item.err

			if err != nil {
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Errorf("The connection to Cfored was broken: %s. "+
						"Exiting...", err)
					gVars.connectionBroken = true
					break CallocStateMachineLoop
				}
			}

			if cforedReply.Type != protos.StreamCallocReply_JOB_COMPLETION_ACK_REPLY {
				return util.NewCraneErr(util.ErrorBackend,
					fmt.Sprintf("Expect type JOB_COMPLETION_ACK_REPLY. Received: %s", cforedReply.Type.String()))
			}

			if cforedReply.GetPayloadJobCompletionAckReply().Ok {
				println("Job completed.")
			} else {
				return util.NewCraneErr(util.ErrorBackend, "Failed to notify server of job completion")
			}

			break CallocStateMachineLoop
		}
	}
	// Check if connection finished normally
	if state != WaitAck || gVars.connectionBroken {
		return &util.CraneError{Code: util.ErrorNetwork}
	} else {
		return nil
	}
}

func MainCalloc(cmd *cobra.Command, args []string) error {
	util.InitDiagLogger(FlagDebugLevel)

	var err error
	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())

	if gVars.cwd, err = os.Getwd(); err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "Failed to get working directory: %s", err)
	}

	if gVars.user, err = user.Current(); err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "Failed to get current user: %s", err)
	}

	gids, err := util.CollectEffectiveGroups()
	if err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "%s", err)
	}

	uid, err := strconv.Atoi(gVars.user.Uid)
	if err != nil {
		return util.NewCraneErr(util.ErrorInvalidFormat, fmt.Sprintf("Failed to convert uid to int: %s", err))
	}

	if gVars.shellPath, err = util.NixShell(gVars.user.Uid); err != nil {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to get default shell of user %s: %s",
			gVars.user.Name, err))
	}

	job := &protos.JobToCtld{
		Name:            "Interactive",
		TimeLimit:       util.InvalidDuration(),
		PartitionName:   "",
		Type:            protos.JobType_Interactive,
		Uid:             uint32(uid),
		Gids:            gids,
		NodeNumMin:      0,
		NodeNumMax:      0,
		NtasksPerNode:   0,
		Ntasks:          0,
		RequeueIfFailed: false,
		Payload:         &protos.JobToCtld_InteractiveMeta{InteractiveMeta: nil},
		CmdLine:         strings.Join(os.Args, " "),
		Cwd:             gVars.cwd,
		Env:             make(map[string]string),
	}

	structExtraFromCli := &util.JobExtraAttrs{}

	if cmd.Flags().Changed("nodes") {
		if FlagNodes == 0 {
			return util.NewCraneErr(util.ErrorCmdArg, "Invalid argument: --nodes must be > 0")
		}
		job.NodeNumMin = FlagNodes
		job.NodeNumMax = FlagNodes
	}
	if cmd.Flags().Changed("ntasks-per-node") {
		if FlagNtasksPerNode == 0 {
			return util.NewCraneErr(util.ErrorCmdArg, "Invalid argument: --ntasks-per-node must be > 0")
		}
		job.NtasksPerNode = FlagNtasksPerNode
	}
	if cmd.Flags().Changed("ntasks") {
		if FlagNtasks == 0 {
			return util.NewCraneErr(util.ErrorCmdArg, "Invalid argument: --ntasks must be > 0")
		}
		job.Ntasks = FlagNtasks
	}
	if cmd.Flags().Changed("cpus-per-task") {
		cpuPerTask := float64(FlagCpuPerTask)
		job.CpusPerTask = &cpuPerTask
	}

	setGresGpusFlag := false
	if FlagGres != "" {
		gresMap, err := util.ParseGres(FlagGres)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --gres value '%s'", FlagGres))
		}
		job.GresPerNode = gresMap
		if _, exist := gresMap.NameGresMap[util.GresGpuName]; exist {
			setGresGpusFlag = true
		}
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --mem value '%s'", FlagMem))
		}
		job.MemPerNode = &memInByte
	}

	if FlagTime != "" {
		seconds, err := util.ParseDurationStrToSeconds(FlagTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --time value '%s'", FlagTime))
		}
		job.TimeLimit.Seconds = seconds
	}
	if FlagMemPerCpu != "" {
		memInBytePerCpu, err := util.ParseMemStringAsByte(FlagMemPerCpu)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --mem-per-cpu value '%s'", FlagMemPerCpu))
		}
		job.MemPerCpu = &memInBytePerCpu
	}
	if FlagPartition != "" {
		job.PartitionName = FlagPartition
	}
	if FlagJob != "" {
		job.Name = FlagJob
	}
	if FlagQos != "" {
		job.Qos = FlagQos
	}
	if FlagCwd != "" {
		job.Cwd = FlagCwd
	}
	if FlagAccount != "" {
		job.Account = FlagAccount
	}
	if FlagNodelist != "" {
		job.Nodelist = FlagNodelist
	}
	if FlagExcludes != "" {
		job.Excludes = FlagExcludes
	}
	if FlagGetUserEnv {
		job.GetUserEnv = true
	}
	if FlagExport != "" {
		job.Env["CRANE_EXPORT_ENV"] = FlagExport
	}
	if FlagReservation != "" {
		job.Reservation = FlagReservation
	}

	if FlagExtraAttr != "" {
		structExtraFromCli.ExtraAttr = FlagExtraAttr
	}
	if FlagMailType != "" {
		structExtraFromCli.MailType = FlagMailType
	}
	if FlagMailUser != "" {
		structExtraFromCli.MailUser = FlagMailUser
	}
	if FlagComment != "" {
		structExtraFromCli.Comment = FlagComment
	}
	if FlagExclusive {
		job.Exclusive = true
	}
	if FlagHold {
		job.Hold = true
	}
	if FlagDeadlineTime != "" {
		deadlineTime, err := util.ParseTime(FlagDeadlineTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --deadline value '%s'", FlagDeadlineTime))
		}
		job.DeadlineTime = timestamppb.New(deadlineTime)
	}
	if cmd.Flags().Changed("wckey") {
		job.Wckey = &FlagWckey
	}
	if FlagGpusPerNode != "" {
		if setGresGpusFlag {
			return util.NewCraneErr(util.ErrorCmdArg, "Cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
		}
		gpuDeviceMap, err := util.ParseGpusPerNodeStr(FlagGpusPerNode)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --gpus-per-node value '%s'", FlagGpusPerNode))

		}
		job.GresPerNode = gpuDeviceMap
	}
	if FlagDependency != "" {
		err := util.SetJobDependencies(job, FlagDependency)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid --dependency: %s", err)
		}
	}

	if FlagLicenses != "" {
		licCount, isLicenseOr, err := util.ParseLicensesString(FlagLicenses)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --licenses value '%s'", FlagLicenses))
		}
		job.LicensesCount = licCount
		job.IsLicensesOr = isLicenseOr
	}

	job.SubmitDir, err = os.Getwd()
	if err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "Get submit dir err: %s.", err)
	}

	job.SubmitHostname, err = os.Hostname()
	if err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "Get submit hostname err: %s.", err)
	}

	if FlagSignal != "" {
		signals, err := util.ParseSignalParamString(FlagSignal)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --signal value '%s'", FlagSignal))
		}
		for _, signal := range signals {
			if signal.SignalFlag == protos.Signal_BATCH_ONLY {
				return util.NewCraneErr(util.ErrorCmdArg, "Invalid --signal specification")
			}
			job.Signals = append(job.Signals, signal)
		}
	}

	// Marshal extra attributes
	if err := structExtraFromCli.Marshal(&job.ExtraAttr); err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s", err))
	}

	// Check the validity of the parameters
	if err := util.CheckJobArgs(job); err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s", err))
	}
	util.SetPropagatedEnviron(&job.Env, &job.GetUserEnv)

	return StartCallocStream(job)
}
