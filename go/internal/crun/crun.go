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

package crun

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"errors"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"

	"golang.org/x/sys/unix"
	"golang.org/x/term"

	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type StateOfCrun int

const (
	ConnectCfored StateOfCrun = 0
	ReqJobId      StateOfCrun = 1
	WaitRes       StateOfCrun = 2
	WaitForward   StateOfCrun = 3
	Forwarding    StateOfCrun = 4
	JobKilling    StateOfCrun = 5
	WaitAck       StateOfCrun = 6
	End           StateOfCrun = 7
)

const (
	FlagIOForwardALL  string = "all"
	FlagIOForwardNONE string = "none"

	MpiTypePmix string = "pmix"
)

type GlobalVariables struct {
	cwd string

	globalCtx       context.Context
	globalCtxCancel context.CancelFunc

	connectionBroken atomic.Bool
}

var gVars GlobalVariables

type ReplyReceiveItem struct {
	reply *protos.StreamCrunReply
	err   error
}

type StateMachineOfCrun struct {
	job    *protos.JobToCtld
	step   *protos.StepToCtld
	jobId  uint32 // This field will be set after ReqJobId state
	stepId uint32 // This field will be set after ReqJobId state

	cranedId      []string
	cranedTaskMap map[string][]uint32 // craned to task ids map
	ntasksTotal   uint32

	inputFlag  string // Crun --input flag, used to determine how to read input from stdin
	outputFlag string // Crun --output flag, used to determine how to write output to stdout
	errorFlag  string // Crun --err flag, used to determine how to write error to stderr

	state        StateOfCrun
	err          util.ExitCode // Hold the final Crane error of the state machine if any
	taskExitCode util.ExitCode // Hold the exit status reported by the user task

	// Hold grpc resources and will be freed in Close.
	conn   *grpc.ClientConn
	client protos.CraneForeDClient
	stream grpc.BidiStreamingClient[protos.StreamCrunRequest, protos.StreamCrunReply]
	sender *crunStreamSender

	sigs               chan os.Signal
	savedTerminalState *term.State
	savedStdinFlags    int
	terminalConfigured bool
	stdinFd            int
	stdinFlagsSaved    bool

	cforedReplyReceiver *CforedReplyReceiver

	// I/O forwarding spans resource allocation through completion acknowledgement.
	stopStepCtx context.Context
	stopStepCb  context.CancelFunc
	//stop step will stop reading from local stdin/file/x11
	stopReadCtx             context.Context
	stopReadCb              context.CancelFunc
	stopWriteCtx            context.Context
	stopWriteCb             context.CancelFunc
	inputWg                 sync.WaitGroup
	writerWg                sync.WaitGroup
	chanInputFromLocal      chan []byte
	chanOutputFromRemote    chan []byte
	chanErrOutputFromRemote chan []byte
	X11SessionMgr           *X11SessionMgr
	jobLifecycleHook        JobLifecycleHook
}
type CforedReplyReceiver struct {
	stream        protos.CraneForeD_CrunStreamClient
	replyChannel  chan ReplyReceiveItem
	expectedClose atomic.Bool
}

type JobLifecycleHook struct {
	CrunProlog          string
	CrunEpilog          string
	PrologTimeout       uint64
	EpilogTimeout       uint64
	PrologEpilogTimeout uint64
}

type RunCommandArgs struct {
	Program    string
	Args       []string
	Envs       map[string]string
	TimeoutSec uint64
}

func (r *CforedReplyReceiver) GetReplyChannel() chan ReplyReceiveItem {
	return r.replyChannel
}

func (r *CforedReplyReceiver) StartReplyReceiveRoutine(stream protos.CraneForeD_CrunStreamClient) {
	r.stream = stream
	r.replyChannel = make(chan ReplyReceiveItem, 8)
	go r.ReplyReceiveRoutine()
}

func (r *CforedReplyReceiver) MarkExpectedClose() {
	r.expectedClose.Store(true)
}

func (r *CforedReplyReceiver) ReplyReceiveRoutine() {
	for {
		cforedReply, err := r.stream.Recv()
		if err == nil && cforedReply.Type == protos.StreamCrunReply_STEP_COMPLETION_ACK_REPLY {
			r.MarkExpectedClose()
		}
		r.replyChannel <- ReplyReceiveItem{
			reply: cforedReply,
			err:   err,
		}
		if err != nil {
			if err != io.EOF && !r.expectedClose.Load() {
				log.Errorf("Failed to receive CforedReply: %s. "+
					"Connection to Cfored is broken. "+
					"ReplyReceiveRoutine is exiting...", err)
			}
			break
		}
	}
}

func (m *StateMachineOfCrun) Init(job *protos.JobToCtld, step *protos.StepToCtld) {
	m.job = job
	m.step = step
	m.state = ConnectCfored
	m.err = util.ErrorSuccess
	m.taskExitCode = 0
	m.initIOForwarding()

	m.sigs = make(chan os.Signal, 1)
	signal.Notify(m.sigs, syscall.SIGINT, syscall.SIGTTOU)
}

func (m *StateMachineOfCrun) Close() {
	if m.stopReadCb != nil {
		m.stopReadCb()
	}
	m.inputWg.Wait()
	if m.stopWriteCb != nil {
		m.stopWriteCb()
	}
	m.writerWg.Wait()

	if m.terminalConfigured {
		if err := term.Restore(int(os.Stdin.Fd()), m.savedTerminalState); err != nil {
			log.Errorf("Failed to restore stdin terminal state: %s", err)
		}
		if _, err := unix.FcntlInt(os.Stdin.Fd(), unix.F_SETFL, m.savedStdinFlags); err != nil {
			log.Errorf("Failed to restore stdin flags: %s", err)
		}
		m.terminalConfigured = false
	}

	if m.cforedReplyReceiver != nil {
		m.cforedReplyReceiver.MarkExpectedClose()
	}
	if gVars.globalCtxCancel != nil {
		gVars.globalCtxCancel()
	}
	if m.conn != nil {
		err := m.conn.Close()
		if err != nil {
			log.Errorf("Failed to close grpc conn: %s", err)
		}
	}
	if m.sender != nil {
		m.sender.Close()
	}
	signal.Stop(m.sigs)
}

func (m *StateMachineOfCrun) sendRequest(
	ctx context.Context,
	request *protos.StreamCrunRequest,
) error {
	if m.sender == nil {
		return errors.New("crun stream sender is not initialized")
	}
	return m.sender.Send(ctx, request)
}

func (m *StateMachineOfCrun) configureTerminal() error {
	if !FlagPty || m.terminalConfigured {
		return nil
	}
	flags, err := unix.FcntlInt(os.Stdin.Fd(), unix.F_GETFL, 0)
	if err != nil {
		return fmt.Errorf("get stdin flags: %w", err)
	}
	state, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return fmt.Errorf("set stdin raw mode: %w", err)
	}
	m.savedStdinFlags = flags
	m.savedTerminalState = state
	m.terminalConfigured = true
	return nil
}

func (m *StateMachineOfCrun) StateConnectCfored() {
	config := util.ParseConfig(FlagConfigFilePath)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithKeepaliveParams(util.ClientKeepAliveParams))
	opts = append(opts, grpc.WithConnectParams(util.ClientConnectParams))

	unixSocketPath := "unix:///" + config.CranedCforedSockPath

	var err error
	m.conn, err = grpc.NewClient(unixSocketPath, opts...)
	if err != nil {
		log.Errorf("Failed to connect to local unix socket %s: %s",
			unixSocketPath, err)

		m.err = util.ErrorBackend
		m.state = End
		return
	}
	m.jobLifecycleHook.CrunProlog = config.JobLifecycleHook.CrunProlog
	m.jobLifecycleHook.CrunEpilog = config.JobLifecycleHook.CrunEpilog
	m.jobLifecycleHook.PrologTimeout = config.JobLifecycleHook.PrologTimeout
	m.jobLifecycleHook.EpilogTimeout = config.JobLifecycleHook.EpilogTimeout
	m.jobLifecycleHook.PrologEpilogTimeout = config.JobLifecycleHook.PrologEpilogTimeout

	if FlagProlog != "" {
		m.jobLifecycleHook.CrunProlog = FlagProlog
	}
	if FlagEpilog != "" {
		m.jobLifecycleHook.CrunEpilog = FlagEpilog
	}

	if len(m.jobLifecycleHook.CrunProlog) > 0 {
		args := RunCommandArgs{
			Program:    m.jobLifecycleHook.CrunProlog,
			Args:       nil,
			TimeoutSec: 0,
		}
		if m.job != nil {
			args.Envs = m.job.Env
		} else {
			args.Envs = m.step.Env
		}
		if m.jobLifecycleHook.PrologTimeout > 0 {
			args.TimeoutSec = m.jobLifecycleHook.PrologTimeout
		}
		if m.jobLifecycleHook.PrologEpilogTimeout > 0 {
			args.TimeoutSec = m.jobLifecycleHook.PrologEpilogTimeout
		}
		ExitCode := m.RunCommand(args)
		if ExitCode != 0 {
			log.Errorf("Prolog '%s' failed (exit code %d).", m.jobLifecycleHook.CrunProlog, ExitCode)
			m.err = util.ErrorBackend
			m.state = End
			return
		}
		log.Tracef("Prolog '%s' finished successfully.", m.jobLifecycleHook.CrunProlog)
	}

	m.client = protos.NewCraneForeDClient(m.conn)

	log.Trace("Sending Job Req to Cfored")
	m.stream, err = m.client.CrunStream(gVars.globalCtx)
	if err != nil {
		log.Errorf("Failed to create CrunStream: %s.", err)
		m.err = util.ErrorNetwork
		m.state = End
		return
	}
	m.sender = newCrunStreamSender(gVars.globalCtx, m.stream)

	m.cforedReplyReceiver = new(CforedReplyReceiver)
	m.cforedReplyReceiver.StartReplyReceiveRoutine(m.stream)
	var request *protos.StreamCrunRequest
	if m.job != nil {
		request = &protos.StreamCrunRequest{
			Type: protos.StreamCrunRequest_JOB_REQUEST,
			Payload: &protos.StreamCrunRequest_PayloadJobReq{
				PayloadJobReq: &protos.StreamCrunRequest_JobReq{
					Job:     m.job,
					CrunPid: int32(os.Getpid()),
				},
			},
		}
	} else {
		request = &protos.StreamCrunRequest{
			Type: protos.StreamCrunRequest_STEP_REQUEST,
			Payload: &protos.StreamCrunRequest_PayloadStepReq{
				PayloadStepReq: &protos.StreamCrunRequest_StepReq{
					Step:    m.step,
					CrunPid: int32(os.Getpid()),
				},
			},
		}
	}

	if err := m.sendRequest(gVars.globalCtx, request); err != nil {
		log.Errorf("Failed to send Request to CrunStream: %s. "+
			"Connection to Crun is broken", err)
		gVars.connectionBroken.Store(true)

		m.state = End
		m.err = util.ErrorNetwork
		return
	}

	m.state = ReqJobId
}

func (m *StateMachineOfCrun) StateReqJobId() {
	log.Trace("Waiting JobId")
	select {
	case item := <-m.cforedReplyReceiver.GetReplyChannel():
		cforedReply, err := item.reply, item.err

		if err != nil {
			switch err {
			case io.EOF:
				fallthrough
			default:
				log.Errorf("Connection to Cfored broken when requesting "+
					"job id: %s. Exiting...", err)
				gVars.connectionBroken.Store(true)
				m.state = End
				m.err = util.ErrorNetwork
				return
			}
		}

		if cforedReply.Type != protos.StreamCrunReply_STEP_ID_REPLY {
			log.Errorln("Expect type JOB_ID_REPLY")
			m.state = End
			m.err = util.ErrorBackend
			return
		}
		payload := cforedReply.GetPayloadStepIdReply()

		if payload.Ok {
			m.jobId = payload.JobId
			m.stepId = payload.StepId
			if !FlagQuiet {
				if m.step == nil {
					fmt.Printf("Job id allocated: %d, waiting resources.\n", m.jobId)
				} else {
					fmt.Printf("Job %d step %d allocated, waiting resources.\n", m.jobId, m.stepId)
				}
			}
			m.state = WaitRes
		} else {
			if m.step == nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to allocate job id: %s\n", payload.FailureReason)
			} else {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to allocate step id: %s\n", payload.FailureReason)
			}
			m.state = End
			m.err = util.ErrorBackend
			return
		}
	case sig := <-m.sigs:
		if sig == syscall.SIGINT {
			log.Tracef("SIGINT Received. Not allowed to cancel job when ReqJobId")
		} else {
			log.Tracef("Unhanled sig %s", sig.String())
		}
	}
}

func (m *StateMachineOfCrun) StateWaitRes() {
	log.Trace("Waiting Res Alloc")
	select {
	case item := <-m.cforedReplyReceiver.GetReplyChannel():
		cforedReply, err := item.reply, item.err

		if err != nil { // Failure Edge
			switch err {
			case io.EOF:
				fallthrough
			default:
				log.Errorf("Connection to Cfored broken when waiting "+
					"resource allocated: %s. Exiting...", err)
				gVars.connectionBroken.Store(true)
				m.state = End
				m.err = util.ErrorNetwork
				return
			}
		}

		switch cforedReply.Type {
		case protos.StreamCrunReply_STEP_RES_ALLOC_REPLY:
			cforedPayload := cforedReply.GetPayloadStepAllocReply()
			Ok := cforedPayload.Ok

			if Ok {
				if !FlagQuiet {
					nodeLabel := "craned nodes"
					if util.IsSlurmOutputMode() {
						nodeLabel = "nodes"
					}
					fmt.Printf("Allocated %s: %s\n", nodeLabel, cforedPayload.AllocatedCranedRegex)
				}
				m.cranedId = cforedPayload.CranedIds
				m.cranedTaskMap = make(map[string][]uint32)
				for craned, tasks := range cforedPayload.CranedTaskMap {
					m.cranedTaskMap[craned] = tasks.TaskIds
				}
				m.ntasksTotal = cforedPayload.NtasksTotal

				m.state = WaitForward
			} else {
				log.Errorln("Failed to allocate job resource. Exiting...")
				m.state = End
				m.err = util.ErrorBackend
				return
			}
		case protos.StreamCrunReply_STEP_CANCEL_REQUEST:
			log.Tracef("Received Job Cancel Request when wait res")
			m.state = JobKilling
		}

	case sig := <-m.sigs:
		if sig == syscall.SIGINT {
			log.Tracef("SIGINT Received. Cancelling the job...")
			m.state = JobKilling
		} else {
			log.Tracef("Unhandled sig %s", sig.String())
			m.state = JobKilling
		}
	}
}

func (m *StateMachineOfCrun) StateWaitForward() {
	select {
	case item := <-m.cforedReplyReceiver.GetReplyChannel():
		cforedReply, err := item.reply, item.err
		if err != nil { // Failure Edge
			switch err {
			case io.EOF:
				fallthrough
			default:
				log.Errorf("Connection to Cfored broken when waiting for forwarding user i/o "+
					"%s. Exiting...", err)
				gVars.connectionBroken.Store(true)
				m.state = End
				m.err = util.ErrorNetwork
				return
			}
		}
		switch cforedReply.Type {
		case protos.StreamCrunReply_TASK_IO_FORWARD_READY:
			cforedPayload := cforedReply.GetPayloadTaskIoForwardReadyReply()
			Ok := cforedPayload.Ok
			if Ok {
				if !FlagQuiet {
					fmt.Println("Job io forward ready, waiting input.")
				}
				m.state = Forwarding
				return
			} else {
				log.Errorln("Failed to wait for job io forward ready. Exiting...")
				m.state = End
				m.err = util.ErrorBackend
				return
			}
		case protos.StreamCrunReply_STEP_CANCEL_REQUEST:
			m.startOutputForwarding()
			m.state = JobKilling
		case protos.StreamCrunReply_STEP_COMPLETION_ACK_REPLY:
			// Job launch failed !
			fmt.Println("Job failed ")
			m.state = End
			m.err = util.ErrorBackend
			return
		default:
			log.Errorf("Received unhandeled msg type %s", cforedReply.Type.String())
			m.startOutputForwarding()
			m.state = JobKilling
		}

	case sig := <-m.sigs:
		m.startOutputForwarding()
		if sig == syscall.SIGINT {
			m.state = JobKilling
		} else {
			log.Tracef("Unhanled sig %s", sig.String())
			m.state = JobKilling
		}
	}
}

func (m *StateMachineOfCrun) StateForwarding() {
	var request *protos.StreamCrunRequest
	var taskIdWithInput *uint32

	if err := m.configureTerminal(); err != nil {
		log.Errorf("Failed to initialize local terminal: %s; killing", err)
		m.state = JobKilling
		m.err = util.ErrorSystem
		return
	}

	m.startInputForwarding()
	m.startOutputForwarding()

	parsedId, err := strconv.ParseUint(m.inputFlag, 10, 32)
	if err == nil {
		if parsedId < uint64(m.ntasksTotal) {
			taskIdWithInput = new(uint32)
			*taskIdWithInput = uint32(parsedId)
		} else {
			log.Tracef("The task id %d specified in --input is out of range [0, %d), "+
				"consider it a file path, input is broadcasted.", parsedId, m.ntasksTotal)
		}
	}

	m.startInputRoutine(func() { m.forwardLocalRequests(taskIdWithInput) })
	if FlagPty {
		m.startInputRoutine(m.forwardTerminalResize)
	}

	for m.state == Forwarding {
		select {
		case <-m.stopStepCtx.Done():
			request = &protos.StreamCrunRequest{
				Type: protos.StreamCrunRequest_STEP_COMPLETION_REQUEST,
				Payload: &protos.StreamCrunRequest_PayloadStepCompleteReq{
					PayloadStepCompleteReq: &protos.StreamCrunRequest_StepCompleteReq{
						Status: protos.JobStatus_Completed,
					},
				},
			}

			log.Debug("Sending JOB_COMPLETION_REQUEST with COMPLETED state...")
			if err := m.sendRequest(gVars.globalCtx, request); err != nil {
				log.Errorf("The connection to Cfored was broken: %s. "+
					"Exiting...", err)
				gVars.connectionBroken.Store(true)
				m.state = JobKilling
				m.err = util.ErrorNetwork
			} else {
				m.state = WaitAck
			}

		case item := <-m.cforedReplyReceiver.replyChannel:
			cforedReply, err := item.reply, item.err
			if err != nil {
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Errorf("The connection to Cfored was broken: %s. "+
						"Killing job...", err)
					gVars.connectionBroken.Store(true)
					m.err = util.ErrorNetwork
					m.state = JobKilling
				}
			} else {
				switch cforedReply.Type {
				case protos.StreamCrunReply_TASK_IO_FORWARD:
					select {
					case m.chanOutputFromRemote <- cforedReply.GetPayloadTaskIoForwardReply().Msg:
					case <-m.stopWriteCtx.Done():
					}
				case protos.StreamCrunReply_TASK_ERR_OUTPUT_FORWARD:
					select {
					case m.chanErrOutputFromRemote <- cforedReply.GetPayloadTaskIoErrOutputForwardReply().Msg:
					case <-m.stopWriteCtx.Done():
					}

				case protos.StreamCrunReply_STEP_X11_CONN:
					fallthrough
				case protos.StreamCrunReply_STEP_X11_FORWARD:
					fallthrough
				case protos.StreamCrunReply_STEP_X11_EOF:
					if m.X11SessionMgr != nil {
						m.X11SessionMgr.X11ReplyChan <- cforedReply
					} else {
						log.Warningf("Received %s but X11 forwarding is not enabled, ignoring.",
							cforedReply.Type.String())
					}

				case protos.StreamCrunReply_TASK_EXIT_STATUS:
					m.handleTaskExitStatus(cforedReply.GetPayloadTaskExitStatusReply())

				case protos.StreamCrunReply_STEP_CANCEL_REQUEST:
					m.stopStepCb()
					log.Trace("Received JOB_CANCEL_REQUEST")
					m.state = JobKilling

				case protos.StreamCrunReply_STEP_COMPLETION_ACK_REPLY:
					log.Debug("Job completed.")
					m.state = End
				}
			}
		}
	}

}

func (m *StateMachineOfCrun) forwardLocalRequests(taskIdWithInput *uint32) {
	var x11Requests <-chan *protos.StreamCrunRequest
	if m.X11SessionMgr != nil {
		x11Requests = m.X11SessionMgr.X11RequestChan
	}

	for {
		select {
		case <-m.stopReadCtx.Done():
			return
		case msg, ok := <-m.chanInputFromLocal:
			eof := !ok || msg == nil
			request := &protos.StreamCrunRequest{
				Type: protos.StreamCrunRequest_TASK_IO_FORWARD,
				Payload: &protos.StreamCrunRequest_PayloadTaskIoForwardReq{
					PayloadTaskIoForwardReq: &protos.StreamCrunRequest_TaskIOForwardReq{
						Msg:    msg,
						Eof:    eof,
						TaskId: taskIdWithInput,
					},
				},
			}
			if err := m.sendRequest(m.stopReadCtx, request); err != nil {
				if m.stopReadCtx.Err() == nil {
					log.Errorf("Failed to send task input to Cfored: %s", err)
					gVars.connectionBroken.Store(true)
					m.stopStepCb()
				}
				return
			}
			if eof {
				return
			}
		case request := <-x11Requests:
			if err := m.sendRequest(m.stopReadCtx, request); err != nil {
				if m.stopReadCtx.Err() == nil {
					log.Errorf("Failed to send X11 input to Cfored: %s", err)
					gVars.connectionBroken.Store(true)
					m.stopStepCb()
				}
				return
			}
		}
	}
}

func (m *StateMachineOfCrun) forwardTerminalResize() {
	resizeSignals := make(chan os.Signal, 1)
	signal.Notify(resizeSignals, syscall.SIGWINCH)
	defer signal.Stop(resizeSignals)

	var previousRows, previousColumns uint32
	var interactiveMeta *protos.InteractiveJobAdditionalMeta
	if m.job != nil {
		interactiveMeta = m.job.GetInteractiveMeta()
	} else {
		interactiveMeta = m.step.GetInteractiveMeta()
	}
	if initialSize := interactiveMeta.GetTerminalSize(); initialSize != nil {
		previousRows = initialSize.GetRows()
		previousColumns = initialSize.GetColumns()
	}

	for {
		select {
		case <-m.stopReadCtx.Done():
			return
		case <-resizeSignals:
			// A one-element signal channel already coalesces bursts. Drain any signal
			// queued while GetSize was running before publishing the newest size.
			select {
			case <-resizeSignals:
			default:
			}
			columns, rows, err := term.GetSize(int(os.Stdin.Fd()))
			if err != nil {
				log.Warningf("Failed to read resized terminal dimensions: %s", err)
				continue
			}
			if !validTerminalSize(rows, columns) {
				log.Warningf("Ignoring invalid resized terminal dimensions %dx%d", rows, columns)
				continue
			}
			if uint32(rows) == previousRows && uint32(columns) == previousColumns {
				continue
			}

			request := &protos.StreamCrunRequest{
				Type: protos.StreamCrunRequest_TERMINAL_RESIZE,
				Payload: &protos.StreamCrunRequest_PayloadTerminalResizeReq{
					PayloadTerminalResizeReq: &protos.StreamCrunRequest_TerminalResizeReq{
						TerminalSize: &protos.TerminalSize{
							Rows:    uint32(rows),
							Columns: uint32(columns),
						},
						TaskId: 0,
					},
				},
			}
			if err := m.sendRequest(m.stopReadCtx, request); err != nil {
				if m.stopReadCtx.Err() == nil {
					log.Errorf("Failed to send terminal resize to Cfored: %s", err)
					gVars.connectionBroken.Store(true)
					m.stopStepCb()
				}
				return
			}
			previousRows, previousColumns = uint32(rows), uint32(columns)
		}
	}
}

func validTerminalSize(rows, columns int) bool {
	const maxTerminalDimension = int(^uint16(0))
	return rows > 0 && rows <= maxTerminalDimension &&
		columns > 0 && columns <= maxTerminalDimension
}

func (m *StateMachineOfCrun) handleTaskExitStatus(exitStatus *protos.StreamCrunReply_TaskExitStatusReply) {
	if exitStatus.ExitCode != 0 {
		if exitStatus.Signaled {
			fmt.Fprintf(os.Stderr, "error: task %d: Terminated\n", exitStatus.TaskId)
		} else {
			fmt.Fprintf(os.Stderr, "error: task %d: Exited with exit code %d\n",
				exitStatus.TaskId, exitStatus.ExitCode)
		}
		m.taskExitCode = int(exitStatus.ExitCode)
	}
}

func (m *StateMachineOfCrun) resultError() error {
	if m.err != util.ErrorSuccess {
		return &util.CraneError{Code: m.err}
	}
	if m.taskExitCode != 0 {
		return &util.CommandExitError{Code: m.taskExitCode}
	}
	return nil
}

func (m *StateMachineOfCrun) StateJobKilling() {
	request := &protos.StreamCrunRequest{
		Type: protos.StreamCrunRequest_STEP_COMPLETION_REQUEST,
		Payload: &protos.StreamCrunRequest_PayloadStepCompleteReq{
			PayloadStepCompleteReq: &protos.StreamCrunRequest_StepCompleteReq{
				Status: protos.JobStatus_Cancelled,
			},
		},
	}

	if gVars.connectionBroken.Load() {
		log.Errorf("The connection to Cfored was broken. Exiting...")
		m.err = util.ErrorNetwork
		m.state = End
		return
	}

	log.Debug("Sending JOB_COMPLETION_REQUEST with CANCELLED state...")
	if err := m.sendRequest(gVars.globalCtx, request); err != nil {
		log.Errorf("The connection to Cfored was broken: %s. Exiting...", err)
		gVars.connectionBroken.Store(true)
		m.err = util.ErrorNetwork
		m.state = End
		return
	} else {
		m.state = WaitAck
	}
}

func (m *StateMachineOfCrun) StateWaitAck() {
	log.Debug("Waiting Ctld JOB_COMPLETION_ACK_REPLY")
	item := <-m.cforedReplyReceiver.replyChannel
	cforedReply, err := item.reply, item.err

	if err != nil {
		switch err {
		case io.EOF:
			fallthrough
		default:
			log.Errorf("The connection to Cfored was broken: %s. "+
				"Exiting...", err)
			gVars.connectionBroken.Store(true)
			m.err = util.ErrorNetwork
			m.state = End
			return
		}
	}

	switch cforedReply.Type {
	case protos.StreamCrunReply_TASK_IO_FORWARD:
		select {
		case m.chanOutputFromRemote <- cforedReply.GetPayloadTaskIoForwardReply().Msg:
		case <-m.stopWriteCtx.Done():
		}
		return // Still in WaitAck state
	case protos.StreamCrunReply_TASK_ERR_OUTPUT_FORWARD:
		select {
		case m.chanErrOutputFromRemote <- cforedReply.GetPayloadTaskIoErrOutputForwardReply().Msg:
		case <-m.stopWriteCtx.Done():
		}
		return

	case protos.StreamCrunReply_STEP_X11_CONN:
		fallthrough
	case protos.StreamCrunReply_STEP_X11_FORWARD:
		fallthrough
	case protos.StreamCrunReply_STEP_X11_EOF:
		if m.X11SessionMgr != nil {
			m.X11SessionMgr.X11ReplyChan <- cforedReply
		} else {
			log.Warningf("Received %s but X11 forwarding is not enabled, ignoring.",
				cforedReply.Type.String())
		}
		return // Still in WaitAck state

	case protos.StreamCrunReply_TASK_EXIT_STATUS:
		m.handleTaskExitStatus(cforedReply.GetPayloadTaskExitStatusReply())
		return // Still in WaitAck state

	case protos.StreamCrunReply_STEP_CANCEL_REQUEST:
		log.Fatalf("Received JOB_CANCEL_REQUEST in WaitAck state, ignored.")
		return

	case protos.StreamCrunReply_STEP_COMPLETION_ACK_REPLY:
		if cforedReply.GetPayloadStepCompletionAckReply().GetOk() {
			log.Debug("Job completed.")
		} else {
			log.Error("Cfored reported job completion failure.")
		}
		m.cforedReplyReceiver.MarkExpectedClose()
		m.state = End
	default:
		log.Errorf("Unexpected message type %s in WaitAck state.", cforedReply.Type.String())
		m.err = util.ErrorBackend
		m.state = End
		return
	}

	if cforedReply.GetPayloadStepCompletionAckReply().Ok {
		log.Debug("Job completed.")
	} else {
		log.Errorln("Failed to notify server of job completion")
		m.err = util.ErrorBackend
	}

	if len(m.jobLifecycleHook.CrunEpilog) > 0 {
		args := RunCommandArgs{
			Program:    m.jobLifecycleHook.CrunEpilog,
			Args:       nil,
			TimeoutSec: 0,
		}
		if m.job != nil {
			args.Envs = m.job.Env
		} else {
			args.Envs = m.step.Env
		}
		if m.jobLifecycleHook.EpilogTimeout > 0 {
			args.TimeoutSec = m.jobLifecycleHook.EpilogTimeout
		}
		if m.jobLifecycleHook.PrologEpilogTimeout > 0 {
			args.TimeoutSec = m.jobLifecycleHook.PrologEpilogTimeout
		}
		ExitCode := m.RunCommand(args)
		if ExitCode != 0 {
			log.Errorf("Epilog '%s' failed (exit code %d).", m.jobLifecycleHook.CrunEpilog, ExitCode)
			m.err = util.ErrorBackend
			m.state = End
			return
		}
		log.Tracef("Epilog '%s' finished successfully.", m.jobLifecycleHook.CrunEpilog)
	}

	m.state = End
}

func (m *StateMachineOfCrun) Run() {
CrunStateMachineLoop:
	for {
		switch m.state {
		case ConnectCfored:
			m.StateConnectCfored()

		case ReqJobId:
			m.StateReqJobId()

		case WaitRes:
			m.StateWaitRes()

		case WaitForward:
			m.StateWaitForward()

		case Forwarding:
			m.StateForwarding()

		case JobKilling:
			m.StateJobKilling()

		case WaitAck:
			m.StateWaitAck()
		case End:
			{
				break CrunStateMachineLoop
			}
		}
	}
}

func (m *StateMachineOfCrun) forwardingSigHandlerRoutine() {
	signal.Ignore(syscall.SIGTTOU, syscall.SIGTTIN)

	lastSigint := time.Now().Add(-2 * time.Second)
loop:
	for {
		select {
		case <-m.stopReadCtx.Done():
			break loop
		case sig := <-m.sigs:
			switch sig {
			/*
				multiple sigint will cancel this job
			*/
			case syscall.SIGINT:
				log.Tracef("Recv signal: %v", sig)
				now := time.Now()
				if lastSigint.Add(time.Second).After(now) {
					m.stopStepCb()
					break loop
				} else {
					lastSigint = now
					fmt.Println("Send interrupt once more in 1s to abort.")
				}

			default:
				log.Tracef("Ignored signal: %v", sig)
			}
		}
	}
	log.Tracef("Signal processing goroutine exit.")
}

func (m *StateMachineOfCrun) StdoutWriterRoutine() {
	log.Trace("Starting StdoutWriterRoutine")
	writer := bufio.NewWriter(os.Stdout)

writing:
	for {
		select {
		case msg := <-m.chanOutputFromRemote:
			if _, err := writer.Write(msg); err != nil {
				fmt.Printf("Failed to write to fd: %v\n", err)
				break writing
			}
			if err := writer.Flush(); err != nil {
				fmt.Printf("Failed to flush to fd: %v\n", err)
				break writing
			}

		case <-m.stopWriteCtx.Done():
			// Drain remaining messages before exiting
			for {
				select {
				case msg := <-m.chanOutputFromRemote:
					writer.Write(msg)
				default:
					writer.Flush()
					return
				}
			}
		}
	}
}

func (m *StateMachineOfCrun) StderrWriterRoutine() {
	log.Trace("Starting StderrWriterRoutine")
	writer := bufio.NewWriter(os.Stderr)

writing:
	for {
		select {
		case msg := <-m.chanErrOutputFromRemote:
			if _, err := writer.Write(msg); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write to stderr: %v\n", err)
				break writing
			}
			if err := writer.Flush(); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to flush to stderr: %v\n", err)
				break writing
			}
		case <-m.stopWriteCtx.Done():
			// Drain remaining messages before exiting
			for {
				select {
				case msg := <-m.chanErrOutputFromRemote:
					writer.Write(msg)
				default:
					writer.Flush()
					return
				}
			}
		}
	}
}

func (m *StateMachineOfCrun) DiscardRoutine(src <-chan []byte, name string) {
	log.Tracef("Starting DiscardRoutine(%s)", name)
	for {
		select {
		case <-src:
		case <-m.stopWriteCtx.Done():
			return
		}
	}
}

func (m *StateMachineOfCrun) FileWriterRoutine(filePath string, src <-chan []byte) {
	if filePath == "" {
		return
	}

	if dir := filepath.Dir(filePath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Errorf("Failed to create output dir %s: %s", dir, err)
			m.stopStepCb()
			return
		}
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		log.Errorf("Failed to open file %s: %s", filePath, err)
		m.stopStepCb()
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Errorf("Failed to close file %s: %s", filePath, err)
		}
	}()

	writer := bufio.NewWriter(file)

	writeMessage := func(msg []byte) bool {
		if _, err := writer.Write(msg); err != nil {
			log.Errorf("Failed to write to file %s: %s", filePath, err)
			m.stopStepCb()
			return false
		}
		if err := writer.Flush(); err != nil {
			log.Errorf("Failed to flush file %s: %s", filePath, err)
			m.stopStepCb()
			return false
		}
		return true
	}

	for {
		select {
		case msg := <-src:
			if !writeMessage(msg) {
				return
			}
		case <-m.stopWriteCtx.Done():
			for {
				select {
				case msg := <-src:
					if !writeMessage(msg) {
						return
					}
				default:
					return
				}
			}
		}
	}
}

func (m *StateMachineOfCrun) StdoutFileWriterRoutine(filePattern string) {
	parsedFilePath, isLocalFile, err := m.ParseFilePattern(filePattern)
	if err != nil {
		log.Errorf("Failed to parse file pattern %s: %s", filePattern, err)
		m.stopStepCb()
		return
	}
	if !isLocalFile {
		log.Debugf("Output file pattern is remote-only, skip: %s", filePattern)
		return
	}
	log.Debugf("Writing stdout to file %s", parsedFilePath)
	m.FileWriterRoutine(parsedFilePath, m.chanOutputFromRemote)
}

func (m *StateMachineOfCrun) StderrFileWriterRoutine(filePattern string) {
	parsedFilePath, isLocalFile, err := m.ParseFilePattern(filePattern)
	if err != nil {
		log.Errorf("Failed to parse file pattern %s: %s", filePattern, err)
		m.stopStepCb()
		return
	}
	if !isLocalFile {
		log.Debugf("Error file pattern is remote-only, skip: %s", filePattern)
		return
	}
	log.Debugf("Writing stderr to file %s", parsedFilePath)
	m.FileWriterRoutine(parsedFilePath, m.chanErrOutputFromRemote)
}

func setFileNonblocking(fd int) (int, error) {
	originalFlags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		return 0, err
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFL, originalFlags|unix.O_NONBLOCK); err != nil {
		return 0, err
	}
	return originalFlags, nil
}

func restoreFileStatusFlags(fd int, flags int) error {
	_, err := unix.FcntlInt(uintptr(fd), unix.F_SETFL, flags)
	return err
}

func (m *StateMachineOfCrun) startStdinReader() {
	fd := int(os.Stdin.Fd())
	m.stdinFd = fd
	go m.StdinReaderRoutine()
}

func (m *StateMachineOfCrun) StdinReaderRoutine() {
	defer close(m.chanInputFromLocal)
	fd := m.stdinFd
	epfd, err := syscall.EpollCreate1(0)
	if err != nil {
		log.Tracef("EpollCreate1: %v", err)
		return
	}

	event := &syscall.EpollEvent{
		Events: syscall.EPOLLIN,
		Fd:     int32(fd),
	}

	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, fd, event); err != nil {
		log.Tracef("EpollCtl: %v", err)
		return
	}

	defer syscall.Close(epfd)
	events := make([]syscall.EpollEvent, 10)
	buf := make([]byte, 4096)
reading:
	for {
		select {
		case <-m.stopReadCtx.Done():
			break reading
		default:
		}

		n, err := syscall.EpollWait(epfd, events, 100) //100MS timeout
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				log.Trace("EpollWait interrupted by signal, retrying")
				continue
			}
			log.Tracef("EpollWait: %v", err)
			return
		}
		for i := 0; i < n; i++ {
			if events[i].Fd == int32(fd) && events[i].Events&syscall.EPOLLIN != 0 {
				nr, err := syscall.Read(fd, buf)
				if err != nil {
					if errors.Is(err, syscall.EAGAIN) {
						log.Trace("Read EAGAIN, no data available now")
						continue
					}
					if errors.Is(err, syscall.EINTR) {
						log.Trace("Read interrupted by signal, retrying")
						continue
					}
					if errors.Is(err, syscall.EIO) {
						log.Trace("Read EIO.")
						continue
					}
					return
				}
				if nr == 0 {
					log.Trace("Read 0 bytes (EOF), closing channel and exiting goroutine")
					return
				}
				data := append([]byte(nil), buf[:nr]...)
				select {
				case m.chanInputFromLocal <- data:
				case <-m.stopReadCtx.Done():
					break reading
				}
				log.Tracef("Sent %d bytes to channel", nr)
			}
		}
	}

}

func (m *StateMachineOfCrun) ParseFilePattern(pattern string) (string, bool, error) {
	log.Tracef("Parsefile pattern: %s", pattern)
	if pattern == "" {
		return pattern, true, nil
	}
	var uid uint32
	var name string
	if m.job != nil {
		uid = m.job.Uid
		name = m.job.Name
	} else {
		uid = m.step.Uid
		name = m.step.Name
	}
	currentUser, err := user.LookupId(fmt.Sprintf("%d", uid))
	if err != nil {
		return pattern, true, fmt.Errorf("failed to lookup user by uid %d: %s", uid, err)
	}
	// User input two backslash , but we will only get one.
	if strings.Contains(pattern, "\\") {
		return strings.ReplaceAll(pattern, "\\", ""), true, nil
	}

	remoteReplacements := map[string]struct{}{
		//Job array's master job allocation number.
		"%A": {},
		//Job array ID (index) number.
		"%a": {},
		//short hostname
		"%N": {},
		//Node identifier relative to current job (e.g. "0" is the first node of the running job)
		"%n": {},
		// task id in step
		"%t": {},
	}

	localReplacements := map[string]string{
		"%%": "%",
		//Job array's master job allocation number.
		//"%A": "",
		//Job array ID (index) number.
		//"%a": "",
		//jobid.stepid of the running job (e.g. "128.0")
		"%J": fmt.Sprintf("%d.%d", m.jobId, m.stepId),
		// job id
		"%j": fmt.Sprintf("%d", m.jobId),
		// step id
		"%s": fmt.Sprintf("%d", m.stepId),
		//User name
		"%u": currentUser.Username,
		// Job name
		"%x": name,
	}

	re := regexp.MustCompile(`%%|%(\d*)([AajJsNntuUx])`)

	isLocalFile := true

	result := re.ReplaceAllStringFunc(pattern, func(match string) string {
		parts := re.FindStringSubmatch(match)
		if parts[0] == "%%" {
			return "%"
		}
		if len(parts) < 3 {
			return match // fallback
		}

		padding := parts[1]   // '5' in '%5j'
		specifier := parts[2] // 'j' in '%5j'

		_, foundInRemote := remoteReplacements["%"+specifier]
		if foundInRemote {
			isLocalFile = false
		}

		value, found := localReplacements["%"+specifier]
		if !found {
			return match // fallback
		}

		if specifier == "j" || specifier == "s" {
			if padding == "" {
				return value
			}
			_, err := strconv.Atoi(padding)
			if err != nil {
				return value
			}
			paddedFormat := "%0" + padding + "v"
			return fmt.Sprintf(paddedFormat, value)
		}

		return value
	})
	if !isLocalFile {
		return "", false, nil
	} else {
		return result, true, nil
	}
}

func (m *StateMachineOfCrun) FileReaderRoutine(filePattern string) {
	defer func() {
		// File input producer owns closing the input channel.
		// The forwarder goroutine must handle channel close gracefully.
		close(m.chanInputFromLocal)
	}()

	parsedFilePath, isLocalFile, err := m.ParseFilePattern(filePattern)
	if err != nil {
		log.Errorf("Failed to parse file pattern %s: %s", filePattern, err)
		m.chanInputFromLocal <- nil
		m.stopStepCb()
		return
	}
	if !isLocalFile {
		log.Debugf("Input file is not a local file: %s", filePattern)
		m.chanInputFromLocal <- nil
		m.stopStepCb()
		return
	}
	file, err := os.Open(parsedFilePath)
	if err != nil {
		log.Errorf("Failed to open file %s: %s", parsedFilePath, err)
		m.chanInputFromLocal <- nil
		m.stopStepCb()
		return
	}
	log.Debugf("Reading from file %s", parsedFilePath)
	reader := bufio.NewReader(file)
	buffer := make([]byte, 4096)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf("Failed to close stdin file: %s.", err)
		}
	}(file)
reading:
	for {
		select {
		case <-m.stopReadCtx.Done():
			break reading
		default:
			n, err := reader.Read(buffer)
			if err != nil {
				if err == io.EOF {
					data := append([]byte(nil), buffer[:n]...)
					select {
					case m.chanInputFromLocal <- data:
					case <-m.stopReadCtx.Done():
					}
					break reading
				}
				log.Errorf("Failed to read from fd: %v", err)
				break reading
			}
			data := append([]byte(nil), buffer[:n]...)
			select {
			case m.chanInputFromLocal <- data:
			case <-m.stopReadCtx.Done():
				break reading
			}
		}
	}
}

func (m *StateMachineOfCrun) initIOForwarding() {
	m.stopStepCtx, m.stopStepCb = context.WithCancel(context.Background())
	m.stopReadCtx, m.stopReadCb = context.WithCancel(m.stopStepCtx)
	// stopWriteCtx is independent of stopStepCtx: it is only cancelled after
	// Run() returns, at which point no more data will be written to the output
	// channels.  This avoids a race where stopStepCtx is cancelled inside
	// StateForwarding (e.g. by TASK_CANCEL_REQUEST) while stderr data is still
	// sitting in the channel unread.  Writer goroutines drain the channel on
	// Done and then exit, guaranteeing all forwarded output reaches the terminal.
	m.stopWriteCtx, m.stopWriteCb = context.WithCancel(context.Background())

	m.chanInputFromLocal = make(chan []byte, 100)
	m.chanOutputFromRemote = make(chan []byte, 20)
	m.chanErrOutputFromRemote = make(chan []byte, 20)
}

func (m *StateMachineOfCrun) startOutputForwarding() {
	startWriter := func(f func()) {
		m.writerWg.Add(1)
		go func() {
			defer m.writerWg.Done()
			f()
		}()
	}

	if strings.ToLower(m.outputFlag) == FlagIOForwardALL {
		log.Debugf("Output to stdout")
		startWriter(m.StdoutWriterRoutine)
	} else if strings.ToLower(m.outputFlag) == FlagIOForwardNONE {
		log.Debugf("Output discarded")
		startWriter(func() { m.DiscardRoutine(m.chanOutputFromRemote, "stdout") })
	} else {
		taskId, err := strconv.ParseUint(m.outputFlag, 10, 32)
		if err != nil {
			log.Debugf("Output to file %s", m.outputFlag)
			startWriter(func() { m.StdoutFileWriterRoutine(m.outputFlag) })
		} else {
			if taskId < uint64(m.ntasksTotal) {
				log.Debugf("Output to stdout (filtered by sender task %d)", taskId)
				startWriter(m.StdoutWriterRoutine)
			} else {
				log.Debugf("Output to file %s (task id %d >= ntasksTotal %d)", m.outputFlag, taskId, m.ntasksTotal)
				startWriter(func() { m.StdoutFileWriterRoutine(m.outputFlag) })
			}
		}
	}

	if strings.ToLower(m.errorFlag) == FlagIOForwardALL {
		log.Debugf("Stderr output to stderr")
		startWriter(m.StderrWriterRoutine)
	} else if strings.EqualFold(m.errorFlag, "none") {
		log.Debugf("Stderr output discarded")
		startWriter(func() { m.DiscardRoutine(m.chanErrOutputFromRemote, "stderr") })
	} else {
		taskId, err := strconv.ParseUint(m.errorFlag, 10, 32)
		if err != nil {
			log.Debugf("Stderr output to file %s", m.errorFlag)
			startWriter(func() { m.StderrFileWriterRoutine(m.errorFlag) })
		} else {
			if taskId < uint64(m.ntasksTotal) {
				log.Debugf("Stderr output to stderr (filtered by sender task %d)", taskId)
				startWriter(m.StderrWriterRoutine)
			} else {
				log.Debugf("Stderr output to file %s (task id %d >= ntasksTotal %d)", m.errorFlag, taskId, m.ntasksTotal)
				startWriter(func() { m.StderrFileWriterRoutine(m.errorFlag) })
			}
		}
	}
}

func (m *StateMachineOfCrun) startInputForwarding() {
	go m.forwardingSigHandlerRoutine()
	if strings.ToLower(m.inputFlag) == FlagIOForwardALL {
		log.Debugf("Input from stdin to all tasks")
		m.startStdinReader()
	} else if strings.ToLower(m.inputFlag) == FlagIOForwardNONE {
		log.Debugf("No input forwarding")
	} else {
		taskId, err := strconv.ParseUint(m.inputFlag, 10, 32)
		if err != nil {
			log.Debugf("Input from file %s, filepath is not a number", m.inputFlag)
			go m.FileReaderRoutine(m.inputFlag)
		} else {
			if taskId < uint64(m.ntasksTotal) {
				log.Debugf("Input from stdin to %d", taskId)
				m.startStdinReader()
			} else {
				log.Debugf("Input from file %s, num but greater than ntasksTotal %d", m.inputFlag, m.ntasksTotal)
				go m.FileReaderRoutine(m.inputFlag)

			}
		}
	}

	var iaMeta *protos.InteractiveJobAdditionalMeta
	if m.job != nil {
		iaMeta = m.job.GetInteractiveMeta()
	} else {
		iaMeta = m.step.GetInteractiveMeta()
	}
	if iaMeta.X11 && iaMeta.GetX11Meta().EnableForwarding {
		m.X11SessionMgr = NewX11SessionMgr(iaMeta.GetX11Meta(), &m.stopReadCtx)
		m.startInputRoutine(m.X11SessionMgr.SessionMgrRoutine)
	}
}

func (m *StateMachineOfCrun) startInputRoutine(routine func()) {
	m.inputWg.Add(1)
	go func() {
		defer m.inputWg.Done()
		routine()
	}()
}

func (m *StateMachineOfCrun) RunCommand(runCommandArgs RunCommandArgs) int {
	ExitCode := 127

	ctx := context.Background()
	if runCommandArgs.TimeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(runCommandArgs.TimeoutSec)*time.Second)
		defer cancel()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	cmd := exec.CommandContext(ctx, runCommandArgs.Program, runCommandArgs.Args...)

	if len(runCommandArgs.Envs) > 0 {
		envs := os.Environ()
		for k, v := range runCommandArgs.Envs {
			envs = append(envs, fmt.Sprintf("%s=%s", k, v))
		}
		cmd.Env = envs
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		log.Errorf("Failed to start command: %v", err.Error())
		return -1
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case <-ctx.Done():
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)

	case <-sigCh:
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)

	case err := <-done:
		if err != nil {
			log.Errorf("Failed to execute command: %v", err.Error())
		}
	}

	if cmd.ProcessState != nil {
		if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok {
			if status.Exited() {
				ExitCode = status.ExitStatus()
			}
		}
	}

	return ExitCode
}

// shellJoinArgs serializes argv so a POSIX shell reconstructs each argument exactly.
func shellJoinArgs(args []string) string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = "'" + strings.ReplaceAll(arg, "'", "'\"'\"'") + "'"
	}
	return strings.Join(quoted, " ")
}

func MainCrun(cmd *cobra.Command, args []string) error {
	util.InitDiagLogger(FlagDebugLevel)

	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())

	var err error
	if gVars.cwd, err = os.Getwd(); err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "Failed to get working directory: %s.", err)
	}

	if len(args) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "Please specify program to run")
	}

	jobId, stepMode, err := util.ParseJobNestedEnv()
	if err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "Failed to parse env: %s.", err)
	}
	jobMode := !stepMode

	var job *protos.JobToCtld
	var step *protos.StepToCtld
	gids, err := util.CollectEffectiveGroups()
	if err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "%s", err)
	}

	if jobMode {
		job = &protos.JobToCtld{
			Name:            "Interactive",
			TimeLimit:       util.InvalidDuration(),
			PartitionName:   "",
			Type:            protos.JobType_Interactive,
			Uid:             uint32(os.Getuid()),
			Gids:            gids,
			NodeNumMin:      0,
			NodeNumMax:      0,
			NtasksPerNode:   0,
			Ntasks:          0,
			RequeueIfFailed: false,
			Payload: &protos.JobToCtld_InteractiveMeta{
				InteractiveMeta: &protos.InteractiveJobAdditionalMeta{},
			},
			ShScript: shellJoinArgs(args),
			IoMeta:   &protos.IoMeta{},
			CmdLine:  strings.Join(args, " "),
			Cwd:      gVars.cwd,

			Env: make(map[string]string), TaskProlog: FlagTaskProlog,
			TaskEpilog: FlagTaskEpilog,
		}
	} else {
		// Initialize step with default values
		step = &protos.StepToCtld{
			Name:            "InteractiveStep",
			TimeLimit:       util.InvalidDuration(),
			JobId:           jobId,
			Type:            protos.JobType_Interactive,
			Uid:             uint32(os.Getuid()),
			Gids:            gids,
			NodeNum:         0,
			NtasksPerNode:   0,
			Ntasks:          0,
			RequeueIfFailed: false,
			Payload: &protos.StepToCtld_InteractiveMeta{
				InteractiveMeta: &protos.InteractiveJobAdditionalMeta{},
			},
			ShScript: shellJoinArgs(args),
			IoMeta:   &protos.IoMeta{},
			CmdLine:  strings.Join(args, " "),
			Cwd:      gVars.cwd,

			Env: make(map[string]string), TaskProlog: FlagTaskProlog,
			TaskEpilog: FlagTaskEpilog,
		}
		step.ExternalLauncher = FlagExternalLauncher
		if err := setInheritedStepFieldsFromEnv(step); err != nil {
			return err
		}
	}
	structExtraFromCli := &util.JobExtraAttrs{}

	if jobMode {
		if cmd.Flags().Changed(NodesOptionStr) {
			if FlagNodes == 0 {
				return util.NewCraneErr(util.ErrorCmdArg, "Invalid argument: --nodes must be > 0")
			}
			job.NodeNumMin = FlagNodes
			job.NodeNumMax = FlagNodes
		}
		if cmd.Flags().Changed(NtasksPerNodeOptionStr) {
			if FlagNtasksPerNode == 0 {
				return util.NewCraneErr(util.ErrorCmdArg, "Invalid argument: --ntasks-per-node must be > 0")
			}
			job.NtasksPerNode = FlagNtasksPerNode
		}
		if cmd.Flags().Changed(NtasksOptionStr) {
			if FlagNtasks == 0 {
				return util.NewCraneErr(util.ErrorCmdArg, "Invalid argument: --ntasks must be > 0")
			}
			job.Ntasks = FlagNtasks
		}
		if cmd.Flags().Changed(CpuPerTaskOptionStr) {
			cpuPerTask := float64(FlagCpuPerTask)
			job.CpusPerTask = &cpuPerTask
		}
		job.Name = util.ExtractExecNameFromArgs(args)
		SubmitDir, err := os.Getwd()
		if err != nil {
			return util.WrapCraneErr(util.ErrorSystem, "Get submit dir err: %s.", err)
		}
		job.SubmitDir = SubmitDir
		submitHostname, err := os.Hostname()
		if err != nil {
			return util.WrapCraneErr(util.ErrorSystem, "Get submit hostname err: %s.", err)
		}
		job.SubmitHostname = submitHostname
	} else {
		if cmd.Flags().Changed(NodesOptionStr) {
			if FlagNodes == 0 {
				return util.NewCraneErr(util.ErrorCmdArg, "Invalid argument: --nodes must be > 0")
			}
			step.NodeNum = FlagNodes
		}
		if cmd.Flags().Changed(NtasksPerNodeOptionStr) {
			if FlagNtasksPerNode == 0 {
				return util.NewCraneErr(util.ErrorCmdArg, "Invalid argument: --ntasks-per-node must be > 0")
			}
			step.NtasksPerNode = FlagNtasksPerNode
		}
		if cmd.Flags().Changed(NtasksOptionStr) {
			if FlagNtasks == 0 {
				return util.NewCraneErr(util.ErrorCmdArg, "Invalid argument: --ntasks must be > 0")
			}
			step.Ntasks = FlagNtasks
		}
		if cmd.Flags().Changed(CpuPerTaskOptionStr) {
			cpuPerTask := float64(FlagCpuPerTask)
			step.CpusPerTask = &cpuPerTask
		}
		step.Name = util.ExtractExecNameFromArgs(args)
	}

	if FlagTime != "" {
		seconds, err := util.ParseDurationStrToSeconds(FlagTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --time value '%s'.", FlagTime))
		}
		if jobMode {
			job.TimeLimit.Seconds = seconds
		} else {
			step.TimeLimit.Seconds = seconds
		}
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --mem value '%s'.", FlagMem))
		}
		// Explicit CLI memory selects one dimension over inherited memory.
		// Clear the other dimension before mutual-exclusion validation.
		if jobMode {
			job.MemPerCpu = nil
			job.MemPerNode = &memInByte
		} else {
			step.MemPerCpu = nil
			step.MemPerNode = &memInByte
		}
	}
	if FlagMemPerCpu != "" {
		memInBytePerCpu, err := util.ParseMemStringAsByte(FlagMemPerCpu)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --mem-per-cpu value '%s'.", FlagMemPerCpu))
		}
		// Explicit CLI memory selects one dimension over inherited memory.
		// Clear the other dimension before mutual-exclusion validation.
		if jobMode {
			job.MemPerNode = nil
			job.MemPerCpu = &memInBytePerCpu
		} else {
			step.MemPerNode = nil
			step.MemPerCpu = &memInBytePerCpu
		}
	}
	setGresGpusFlag := false
	if FlagGres != "" {
		gresMap, err := util.ParseGres(FlagGres)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --gres value '%s'.", FlagGres))
		}
		if jobMode {
			job.GresPerNode = gresMap
			if _, exist := gresMap.NameGresMap[util.GresGpuName]; exist {
				setGresGpusFlag = true
			}
		} else {
			step.GresPerNode = gresMap
		}
	}
	if FlagPartition != "" {
		if jobMode {
			job.PartitionName = FlagPartition
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --partition is not supported in step."))
		}
	}

	if FlagJob != "" {
		if jobMode {
			job.Name = FlagJob
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --job is not supported in step."))

		}
	}

	if FlagQos != "" {
		if jobMode {
			job.Qos = FlagQos
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --qos is not supported in step."))

		}
	}

	if FlagCwd != "" {
		if jobMode {
			job.Cwd = FlagCwd
		} else {
			step.Cwd = FlagCwd
		}
	}
	if FlagAccount != "" {
		if jobMode {
			job.Account = FlagAccount
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --account is not supported in step."))
		}

	}

	if FlagNodelist != "" {
		if jobMode {
			job.Nodelist = FlagNodelist
		} else {
			step.Nodelist = FlagNodelist
		}
	}
	if FlagExcludes != "" {
		if jobMode {
			job.Excludes = FlagExcludes
		} else {
			step.Excludes = FlagExcludes
		}
	}
	if FlagGetUserEnv {
		if jobMode {
			job.GetUserEnv = true
		} else {
			step.GetUserEnv = true
		}
	}
	if FlagExport != "" {
		if jobMode {
			job.Env["CRANE_EXPORT_ENV"] = FlagExport
		} else {
			step.Env["CRANE_EXPORT_ENV"] = FlagExport
		}
	}
	if FlagReservation != "" {
		if jobMode {
			job.Reservation = FlagReservation
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --reservation is not supported in step."))
		}
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
		if jobMode {
			job.Exclusive = true
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --exclusive is not supported in step."))

		}
	}
	if FlagHold {
		if jobMode {
			job.Hold = true
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --hold is not supported in step."))
		}

	}
	if cmd.Flags().Changed("wckey") {
		if jobMode {
			job.Wckey = &FlagWckey
		}
	}
	if FlagDependency != "" {
		if jobMode {
			err := util.SetJobDependencies(job, FlagDependency)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s.", err))
			}
		}
	}

	if FlagLicenses != "" {
		licCount, isLicenseOr, err := util.ParseLicensesString(FlagLicenses)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid argument: %s.", err)
		}
		job.LicensesCount = licCount
		job.IsLicensesOr = isLicenseOr
	}
	if FlagGpusPerNode != "" {
		if setGresGpusFlag {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: "Cannot specify both --gres gpus and --gpus-per-node flags simultaneously",
			}
		}
		gpuDeviceMap, err := util.ParseGpusPerNodeStr(FlagGpusPerNode)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid argument: invalid --gpus-per-node: %s", err),
			}

		}
		if jobMode {
			job.GresPerNode = gpuDeviceMap
		} else {
			if len(gpuDeviceMap.NameGresMap) != 0 {
				step.GresPerNode = gpuDeviceMap
			}
		}

	}

	if FlagSignal != "" {
		if jobMode {
			signals, err := util.ParseSignalParamString(FlagSignal)
			if err != nil {
				return util.WrapCraneErr(util.ErrorCmdArg, "invalid argument: %s", err)
			}
			for _, sig := range signals {
				if sig.SignalFlag == protos.Signal_BATCH_ONLY {
					return util.NewCraneErr(util.ErrorCmdArg, "Invalid --signal specification")
				}
				job.Signals = append(job.Signals, sig)
			}
		}
	}
	if FlagDeadlineTime != "" {
		deadlineTime, err := util.ParseTime(FlagDeadlineTime)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid argument: --deadline: %s", err),
			}
		}
		job.DeadlineTime = timestamppb.New(deadlineTime)
	}
	// Marshal extra attributes
	if jobMode {
		if err := structExtraFromCli.Marshal(&job.ExtraAttr); err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s.", err))
		}
	} else {
		if err := structExtraFromCli.Marshal(&step.ExtraAttr); err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s.", err))
		}
	}

	if jobMode {
		util.SetPropagatedEnviron(&job.Env, &job.GetUserEnv)
	} else {
		util.SetPropagatedEnviron(&step.Env, &step.GetUserEnv)
	}

	// Check the validity of the parameters
	if jobMode {
		if err := util.CheckJobArgs(job); err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s.", err))
		}
	} else {
		if err := util.CheckStepArgs(step); err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s.", err))
		}
	}

	var iaMeta *protos.InteractiveJobAdditionalMeta
	if jobMode {
		iaMeta = job.GetInteractiveMeta()
	} else {
		iaMeta = step.GetInteractiveMeta()
	}
	iaMeta.Pty = FlagPty
	if FlagPty {
		columns, rows, err := term.GetSize(int(os.Stdin.Fd()))
		if err != nil {
			return util.NewCraneErr(
				util.ErrorSystem,
				fmt.Sprintf("Failed to read terminal size: %s.", err),
			)
		}
		if !validTerminalSize(rows, columns) {
			return util.NewCraneErr(
				util.ErrorSystem,
				fmt.Sprintf("Invalid terminal size %dx%d.", rows, columns),
			)
		}
		iaMeta.TerminalSize = &protos.TerminalSize{
			Rows:    uint32(rows),
			Columns: uint32(columns),
		}
	}

	if FlagX11 {
		target, port, err := util.GetX11DisplayEx(!FlagX11Fwd)
		if err != nil {
			return util.NewCraneErr(util.ErrorSystem, fmt.Sprintf("Error in reading X11 $DISPLAY: %s.", err))
		}

		if !FlagX11Fwd && (target == "" || target == "localhost") {
			if target, err = os.Hostname(); err != nil {
				return util.NewCraneErr(util.ErrorSystem, fmt.Sprintf("failed to get hostname: %s.", err))
			}
			log.Debugf("Host in $DISPLAY (%v) is invalid, using hostname: %s",
				port-util.X11TcpPortOffset, target)
		}

		cookie, err := util.GetX11AuthCookie()
		if err != nil {
			return util.NewCraneErr(util.ErrorSystem, fmt.Sprintf("Error in reading X11 xauth cookies: %s.", err))
		}

		iaMeta.X11 = true
		iaMeta.X11Meta = &protos.X11Meta{
			Cookie:           cookie,
			Target:           target,
			Port:             uint32(port),
			EnableForwarding: FlagX11Fwd,
		}

		log.Debugf("X11 forwarding enabled (%v:%d). ", target, port)
	}

	if FlagMpi != "" && FlagMpi != MpiTypePmix {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: unsupported MPI type %s.", FlagMpi))
	}

	iaMeta.Mpi = FlagMpi

	termEnv, exits := syscall.Getenv("TERM")
	if exits {
		iaMeta.TermEnv = termEnv
	}
	iaMeta.InteractiveType = protos.InteractiveJobType_Crun

	m := new(StateMachineOfCrun)
	m.inputFlag = FlagInput
	m.outputFlag = FlagOutput
	m.errorFlag = FlagErr

	if FlagPty {
		if cmd.Flags().Changed("input") || cmd.Flags().Changed("output") || cmd.Flags().Changed("err") {
			return util.NewCraneErr(util.ErrorCmdArg, "--input/--output/--err are incompatible with --pty.")
		} else {
			log.Debugf("Crun with pty, set input/output/error to 0/0/none")
			//For pty, we always set inputFlag to "0", only fwd for task 0
			m.inputFlag = "0"
			m.outputFlag = "0"
			m.errorFlag = "none"
		}
	}

	var ioMeta *protos.IoMeta
	if jobMode {
		ioMeta = job.IoMeta
	} else {
		ioMeta = step.IoMeta
	}
	ioMeta.OpenModeAppend = proto.Bool(true)
	ioMeta.InputFilePattern = m.inputFlag
	ioMeta.OutputFilePattern = m.outputFlag
	ioMeta.ErrorFilePattern = m.errorFlag

	m.Init(job, step)
	defer m.Close()
	m.Run()

	return m.resultError()
}
