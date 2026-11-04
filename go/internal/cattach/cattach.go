/**
 * Copyright (c) 2026 Peking University and Peking University
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

package cattach

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/pkg/term/termios"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type StateOfCattach int

const (
	ConnectCfored StateOfCattach = 0
	WaitForward   StateOfCattach = 1
	Forwarding    StateOfCattach = 2
	End           StateOfCattach = 3
)

type GlobalVariables struct {
	cwd string

	globalCtx       context.Context
	globalCtxCancel context.CancelFunc

	connectionBroken bool
}

var gVars GlobalVariables

// TaskOutputMsg wraps a task output message with its originating task id.
// It is used to pass output data together with metadata through internal channels,
// so that --label and --output-filter can work correctly.
type TaskOutputMsg struct {
	Data   []byte
	TaskId uint32
}

type ReplyReceiveItem struct {
	reply *protos.StreamCattachReply
	err   error
}

func stepHasTaskId(step *protos.CattachStepInfo, taskId uint32) bool {
	if step == nil {
		return false
	}
	for _, nodeTasks := range step.GetCranedTaskMap() {
		for _, id := range nodeTasks.GetTaskIds() {
			if id == taskId {
				return true
			}
		}
	}
	if len(step.GetCranedTaskMap()) > 0 {
		return false
	}

	return taskId < step.GetNtasks()
}

type CforedReplyReceiver struct {
	stream       protos.CraneForeD_CattachStreamClient
	replyChannel chan ReplyReceiveItem
}

func (r *CforedReplyReceiver) GetReplyChannel() chan ReplyReceiveItem {
	return r.replyChannel
}

func (r *CforedReplyReceiver) StartReplyReceiveRoutine(stream protos.CraneForeD_CattachStreamClient) {
	r.stream = stream
	r.replyChannel = make(chan ReplyReceiveItem, 8)
	go r.ReplyReceiveRoutine()
}

func (r *CforedReplyReceiver) ReplyReceiveRoutine() {
	for {
		cforedReply, err := r.stream.Recv()
		r.replyChannel <- ReplyReceiveItem{
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

type StateMachineOfCattach struct {
	jobId              uint32 // This field will be set after ReqTaskId state
	stepId             uint32
	step               *protos.CattachStepInfo
	invalidInputFilter bool

	state StateOfCattach
	err   util.ExitCode // Hold the final error of the state machine if any

	// Hold grpc resources and will be freed in Close.
	conn   *grpc.ClientConn
	client protos.CraneForeDClient
	stream grpc.BidiStreamingClient[protos.StreamCattachRequest, protos.StreamCattachReply]

	sigs         chan os.Signal
	savedPtyAttr unix.Termios

	cforedReplyReceiver *CforedReplyReceiver

	// These fields are used under Forwarding State.
	taskFinishCtx           context.Context
	taskFinishCb            context.CancelFunc
	taskErrCtx              context.Context
	taskErrCb               context.CancelFunc
	chanInputFromTerm       chan []byte
	chanOutputFromRemote    chan TaskOutputMsg
	chanErrOutputFromRemote chan []byte
}

func (m *StateMachineOfCattach) Init() {
	m.state = ConnectCfored
	m.err = util.ErrorSuccess

	m.sigs = make(chan os.Signal, 1)
	signal.Notify(m.sigs, syscall.SIGINT, syscall.SIGTTOU)
}

func (m *StateMachineOfCattach) Close() {
	if m.conn == nil {
		return
	}
	err := m.conn.Close()
	if err != nil {
		log.Errorf("Failed to close grpc conn: %s", err)
	}

	if FlagPty {
		err = termios.Tcsetattr(os.Stdin.Fd(), termios.TCSANOW, &m.savedPtyAttr)
		if err != nil {
			log.Errorf("Failed to restore stdin attr: %s", err.Error())
		}
	}
}

func (m *StateMachineOfCattach) StateConnectCfored() {
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

	m.client = protos.NewCraneForeDClient(m.conn)

	log.Trace("Connecting to Cfored....")
	m.stream, err = m.client.CattachStream(gVars.globalCtx)
	if err != nil {
		log.Errorf("Failed to create CattachStream: %s.", err)
		m.err = util.ErrorNetwork
		m.state = End
		return
	}

	m.cforedReplyReceiver = new(CforedReplyReceiver)
	m.cforedReplyReceiver.StartReplyReceiveRoutine(m.stream)

	request := &protos.StreamCattachRequest{
		Type: protos.StreamCattachRequest_STEP_CONNECT_REQUEST,
		Payload: &protos.StreamCattachRequest_PayloadStepConnectReq{
			PayloadStepConnectReq: &protos.StreamCattachRequest_StepConnectReq{
				CattachPid: int32(os.Getpid()),
				JobId:      m.jobId,
				StepId:     m.stepId,
				Uid:        uint32(os.Getuid()),
			},
		},
	}

	if err := m.stream.Send(request); err != nil {
		log.Errorf("Failed to send Task Request to CattachStream: %s. "+
			"Connection to Cattach is broken", err)
		gVars.connectionBroken = true

		m.state = End
		m.err = util.ErrorNetwork
		return
	}

	m.state = WaitForward
}

func (m *StateMachineOfCattach) StateWaitForward() {
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
				gVars.connectionBroken = true
				m.state = End
				m.err = util.ErrorNetwork
				return
			}
		}
		switch cforedReply.Type {
		case protos.StreamCattachReply_STEP_CONNECT_REPLY:
			cforedReply := cforedReply.GetPayloadStepConnectReply()
			Ok := cforedReply.Ok
			if Ok {
				m.step = cforedReply.StepInfo
				FlagPty = m.step.GetPty()
				// CattachStepInfo.InputTaskId is an optional uint32; HasInputTaskId()
				// returns true when crun stored an explicit task id via --input=<task_id>.
				// In that case cattach enters read-only mode (output only, no stdin).
				// PTY mode is excluded because it always uses task-0-only routing
				// internally yet requires full interactive control.
				FlagReadOnly = m.step.InputTaskId != nil && !FlagPty
				if FlagReadOnly {
					log.Debug("Step has exclusive stdin to one task; cattach entering read-only mode (output only)")
				}
				m.invalidInputFilter = FlagInputFilter >= 0 && !stepHasTaskId(m.step, uint32(FlagInputFilter))
				if FlagLayout {
					m.PrintStepLayout()
					m.state = End
					return
				}
			} else {
				log.Errorf("Failed to wait for step connect reply, reason: %s. Exiting...", cforedReply.FailureReason)
				m.state = End
				m.err = util.ErrorBackend
				return
			}
		case protos.StreamCattachReply_TASK_IO_FORWARD_READY:
			cforedPayload := cforedReply.GetPayloadTaskIoForwardReadyReply()
			Ok := cforedPayload.Ok
			if Ok {
				if !FlagQuiet {
					fmt.Println("Task io forward ready, waiting input.")
				}
				m.state = Forwarding
				return
			} else {
				log.Errorln("Failed to wait for task io forward ready. Exiting...")
				m.state = End
				m.err = util.ErrorBackend
				return
			}
		case protos.StreamCattachReply_STEP_COMPLETION_ACK_REPLY:
			// Step COMPLETION
			m.state = End
			return
		default:
			log.Errorf("Received unhandeled msg type %s", cforedReply.Type.String())
			m.state = End
		}

	case sig := <-m.sigs:
		if sig == syscall.SIGINT {
			m.state = End
		} else {
			log.Tracef("Unhanled sig %s", sig.String())
			m.state = End
		}
	}
}

func (m *StateMachineOfCattach) StateForwarding() {
	var request *protos.StreamCattachRequest

	if m.step.GetPty() {
		ptyAttr := unix.Termios{}
		err := termios.Tcgetattr(os.Stdin.Fd(), &ptyAttr)
		if err != nil {
			log.Errorf("Failed to get stdin attr: %s,killing", err.Error())
			m.state = End
			m.err = util.ErrorSystem
			return
		}
		m.savedPtyAttr = ptyAttr

		termios.Cfmakeraw(&ptyAttr)
		termios.Cfmakecbreak(&ptyAttr)
		err = termios.Tcsetattr(os.Stdin.Fd(), termios.TCSANOW, &ptyAttr)
		if err != nil {
			log.Errorf("Failed to get stdin attr: %s,killing", err.Error())
			m.state = End
			m.err = util.ErrorSystem
			return
		}
	}

	m.StartIOForward()

	// Forward terminal stdin to cfored. When --input-filter targets a missing
	// task, keep reading stdin so every attempted input reports an error, but
	// drop it instead of forwarding.
	if !FlagReadOnly || m.invalidInputFilter {
		go func() {
			for {
				select {
				case msg, ok := <-m.chanInputFromTerm:
					// ok==false means the channel was closed (e.g. by StdinReaderRoutine's
					// defer close); msg==nil is the explicit EOF sentinel sent by
					// FileReaderRoutine.  Both cases mean "end of input": send one EOF
					// request and exit so we do not spin sending endless EOFs.
					eof := !ok || msg == nil
					if m.invalidInputFilter {
						if !eof {
							log.Errorf("cattach: error: a valid task id must be specified for --input-filter")
						}
						if eof {
							return
						}
						continue
					}
					ioFwdReq := &protos.StreamCattachRequest_TaskIOForwardReq{
						Msg: msg,
						Eof: eof,
					}
					// --input-filter: direct stdin to the specified task only.
					// FlagInputFilter == -1 means "not set" (broadcast to all tasks).
					if FlagInputFilter >= 0 {
						taskId := uint32(FlagInputFilter)
						ioFwdReq.TaskId = &taskId
					}
					request = &protos.StreamCattachRequest{
						Type: protos.StreamCattachRequest_TASK_IO_FORWARD,
						Payload: &protos.StreamCattachRequest_PayloadTaskIoForwardReq{
							PayloadTaskIoForwardReq: ioFwdReq,
						},
					}
					if err := m.stream.Send(request); err != nil {
						log.Errorf("Failed to send Task IO Forward to CattachStream: %s. "+
							"Connection to Cattach is broken", err)
						gVars.connectionBroken = true
						return
					}
					if eof {
						// EOF forwarded once; stop the goroutine to avoid re-reading
						// a closed channel and spinning with endless EOF requests.
						return
					}

				case <-m.taskFinishCtx.Done():
					return
				}
			}
		}()
	}

	for m.state == Forwarding {
		select {
		case <-m.taskFinishCtx.Done():
			// taskFinishCb() was already called (e.g. by SIGINT handler or Ctrl+C in PTY mode).
			// All IO goroutines observe taskFinishCtx.Done() and exit on their own.
			m.state = End
			return
		case <-m.taskErrCtx.Done():
			// taskErrCb() was triggered (e.g. by FileReaderRoutine on open error).
			// Signal all IO goroutines to stop via taskFinishCb so they don't leak.
			m.taskFinishCb()
			m.state = End
			return
		case item := <-m.cforedReplyReceiver.replyChannel:
			cforedReply, err := item.reply, item.err
			if err != nil {
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Errorf("The connection to Cfored was broken: %s.", err)
					gVars.connectionBroken = true
					m.err = util.ErrorNetwork
					// Signal all IO goroutines (StdinReaderRoutine, StdoutWriterRoutine,
					// input-forward goroutine) to exit so they don't leak.
					m.taskFinishCb()
					m.state = End
				}
			} else {
				switch cforedReply.Type {
				case protos.StreamCattachReply_TASK_IO_FORWARD:
					// Use a select so that taskFinishCtx.Done() (triggered by SIGINT
					// or connection break) can interrupt the send when chanOutputFromRemote
					// is full.  Without this guard a slow terminal fills the channel and
					// permanently blocks the StateForwarding loop, preventing a clean exit.
					fwdReply := cforedReply.GetPayloadTaskIoForwardReply()
					select {
					case m.chanOutputFromRemote <- TaskOutputMsg{
						Data:   fwdReply.Msg,
						TaskId: fwdReply.TaskId,
					}:
					case <-m.taskFinishCtx.Done():
						// Discard remaining output and exit cleanly.
						m.taskFinishCb() // idempotent - safe to call again
						m.state = End
						return
					}
				case protos.StreamCattachReply_TASK_ERR_OUTPUT_FORWARD:
					select {
					case m.chanErrOutputFromRemote <- cforedReply.GetPayloadTaskIoErrOutputForwardReply().Msg:
					case <-m.taskFinishCtx.Done():
						m.taskFinishCb()
						m.state = End
						return
					}
				case protos.StreamCattachReply_STEP_COMPLETION_ACK_REPLY:
					log.Debug("Step completed.")
					// Signal all IO goroutines to exit cleanly before transitioning to End.
					m.taskFinishCb()
					m.state = End
					return
				default:
					log.Warnf("[Cattach] Received unhandled msg type %s in Forwarding state", cforedReply.Type.String())
				}
			}
		}
	}
}

func (m *StateMachineOfCattach) Run() {
CrunStateMachineLoop:
	for {
		switch m.state {
		case ConnectCfored:
			m.StateConnectCfored()

		case WaitForward:
			m.StateWaitForward()

		case Forwarding:
			m.StateForwarding()

		case End:
			break CrunStateMachineLoop
		}
	}
}

func (m *StateMachineOfCattach) StartIOForward() {
	m.taskFinishCtx, m.taskFinishCb = context.WithCancel(context.Background())
	m.taskErrCtx, m.taskErrCb = context.WithCancel(context.Background())

	m.chanInputFromTerm = make(chan []byte, 100)
	m.chanOutputFromRemote = make(chan TaskOutputMsg, 20)
	m.chanErrOutputFromRemote = make(chan []byte, 20)

	go m.forwardingSigHandlerRoutine()
	// In read-only mode (step has exclusive stdin, e.g. crun --input=<task_id>),
	// do not read from the local terminal. Invalid --input-filter is different:
	// read stdin so user input can be rejected with an error each time.
	if !FlagReadOnly || m.invalidInputFilter {
		go m.StdinReaderRoutine()
	}
	go m.StdoutWriterRoutine()
	go m.StderrWriterRoutine()
}

func (m *StateMachineOfCattach) StdinReaderRoutine() {
	fd := int(os.Stdin.Fd())

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
	defer close(m.chanInputFromTerm)
	events := make([]syscall.EpollEvent, 10)
	buf := make([]byte, 4096)
reading:
	for {
		if FlagPty {
			ptyAttr := unix.Termios{}
			err := termios.Tcgetattr(os.Stdin.Fd(), &ptyAttr)
			if err != nil {
				log.Errorf("Failed to get stdin attr")
			}
			termios.Cfmakeraw(&ptyAttr)
			err = termios.Tcsetattr(os.Stdin.Fd(), termios.TCSANOW, &ptyAttr)
			if err != nil {
				log.Errorf("Failed to set stdin attr")
			}
		}

		select {
		case <-m.taskFinishCtx.Done():
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

				if FlagPty {
					for i := 0; i < nr; i++ {
						if buf[i] == 0x03 { // Ctrl+C
							log.Trace("Local Ctrl+C detected, exiting cattach")
							m.taskFinishCb()
							return
						}
					}
				}

				data := make([]byte, nr)
				copy(data, buf[:nr])
				// Use select so that a concurrent taskFinishCb() (e.g. connection
				// broken, SIGINT) can interrupt the send when chanInputFromTerm is
				// full, preventing StdinReaderRoutine from blocking forever.
				select {
				case m.chanInputFromTerm <- data:
					log.Tracef("Sent %d bytes to channel", nr)
				case <-m.taskFinishCtx.Done():
					return
				}
			}
		}
	}

}

func (m *StateMachineOfCattach) forwardingSigHandlerRoutine() {
	signal.Ignore(syscall.SIGTTOU, syscall.SIGTTIN)

	for {
		select {
		case sig := <-m.sigs:
			switch sig {
			case syscall.SIGINT:
				log.Tracef("Recv signal: %v", sig)
				m.taskFinishCb()
				log.Tracef("Signal processing goroutine exit.")
				return
			default:
				log.Tracef("Ignored signal: %v", sig)
			}
		case <-m.taskFinishCtx.Done():
			// taskFinishCb() was called by another path (e.g. connection broken,
			// STEP_COMPLETION_ACK_REPLY received).  Exit so this goroutine does
			// not leak when StateForwarding returns without a SIGINT.
			log.Tracef("Signal processing goroutine exit (taskFinishCtx done).")
			return
		}
	}
}

// applyLabel processes raw output data for a given task, prepending "[task_id]: " to each
// complete line. Incomplete trailing bytes (no terminating newline yet) are buffered in
// labelBufs and flushed on the next call or when a newline is encountered.
func applyLabel(taskId uint32, data []byte, labelBufs map[uint32][]byte) []byte {
	prefix := fmt.Sprintf("%d: ", taskId)
	buf := labelBufs[taskId]

	var result []byte
	for _, b := range data {
		if b == '\n' {
			result = append(result, []byte(prefix)...)
			result = append(result, buf...)
			result = append(result, '\n')
			buf = buf[:0]
		} else {
			buf = append(buf, b)
		}
	}
	labelBufs[taskId] = buf
	return result
}

func (m *StateMachineOfCattach) StdoutWriterRoutine() {
	file := os.NewFile(os.Stdout.Fd(), "stdout")
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf("Failed to close stdout file: %s.", err)
		}
	}(file)

	log.Trace("Starting StdoutWriterRoutine")
	writer := bufio.NewWriter(file)

	// labelBufs holds per-task pending bytes that have not yet been terminated with '\n'.
	// Only used when --label is set.
	labelBufs := make(map[uint32][]byte)

writing:
	for {
		select {
		case msg := <-m.chanOutputFromRemote:
			// --output-filter: skip output from tasks other than the specified one.
			// FlagOutputFilter == -1 means "not set" (show all tasks).
			if FlagOutputFilter >= 0 && msg.TaskId != uint32(FlagOutputFilter) {
				continue writing
			}

			var data []byte
			if FlagLabel {
				data = applyLabel(msg.TaskId, msg.Data, labelBufs)
			} else {
				data = msg.Data
			}

			if len(data) == 0 {
				continue writing
			}

			_, err := writer.Write(data)
			if err != nil {
				fmt.Printf("Failed to write to fd: %v\n", err)
				break writing
			}
			err = writer.Flush()
			if err != nil {
				fmt.Printf("Failed to flush to fd: %v\n", err)
				break writing
			}

		case <-m.taskFinishCtx.Done():
			break writing
		}
	}
}

func (m *StateMachineOfCattach) StderrWriterRoutine() {
	log.Trace("Starting StderrWriterRoutine")
	writer := bufio.NewWriter(os.Stderr)
	// NOTE: --error-filter (FlagErrorFilter) is currently a no-op here because
	// the TaskErrOutputReq proto message does not carry a task_id field.
	// Per-task stderr filtering would require adding task_id to TaskErrOutputReq
	// in the protocol (Crane.proto) and propagating it through cfored/supervisor.
	// TODO: implement --error-filter once task_id is added to TaskErrOutputReq.

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
		case <-m.taskFinishCtx.Done():
			// Drain remaining messages before exiting.
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

func (m *StateMachineOfCattach) PrintStepLayout() {
	fmt.Printf("Job step layout:\n")
	fmt.Printf("        %d tasks, %d nodes (%s)\n\n",
		m.step.Ntasks, m.step.NodeNum, m.step.Nodelist)

	// Print per-node task breakdown, mirroring Slurm's sattach --layout output.
	nodeList, ok := util.ParseHostList(m.step.Nodelist)
	if ok && len(m.step.CranedTaskMap) > 0 {
		for i, node := range nodeList {
			nodeTasks, exists := m.step.CranedTaskMap[node]
			if !exists {
				continue
			}
			ids := nodeTasks.GetTaskIds()
			idStr := ""
			for _, tid := range ids {
				idStr += fmt.Sprintf(" %d", tid)
			}
			fmt.Printf("        Node %d (%s), %d task(s):%s\n",
				i, node, len(ids), idStr)
		}
	}
	fmt.Println()
}

func MainCattach(args []string) error {

	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())

	// TODO: implement --error-filter: currently this flag is a no-op because the stderr stream does not carry per-task metadata (task_id), which is needed to filter by task. Implementing this would require adding task_id to the TaskErrOutputReq proto message and propagating it through cfored/supervisor. For now we return an error if the user tries to use --error-filter to avoid confusion about why it doesn't work.
	if FlagErrorFilter >= 0 {
		return &util.CraneError{
			Code: util.ErrorCmdArg,
			Message: "--error-filter is not yet supported: the stderr stream does not carry " +
				"per-task metadata. Remove --error-filter or wait for protocol support.",
		}
	}

	var err error
	if gVars.cwd, err = os.Getwd(); err != nil {
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Failed to get working directory: %s.", err),
		}
	}

	if len(args) == 0 {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "Please specify taskid.stepid",
		}
	}

	m := new(StateMachineOfCattach)

	parts := strings.Split(args[0], ".")
	if len(parts) != 2 {
		return util.NewCraneErr(util.ErrorCmdArg,
			fmt.Sprintf("Failed to parse stepid from command line options: %s", args[0]))
	}

	jobId, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg,
			fmt.Sprintf("invalid job id %q: must be a non-negative integer in [0, 4294967295]", parts[0]))
	}
	m.jobId = uint32(jobId)

	stepId, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg,
			fmt.Sprintf("invalid step id %q: must be a non-negative integer in [0, 4294967295]", parts[1]))
	}
	m.stepId = uint32(stepId)

	m.Init()
	// defer m.Close() is placed before m.Run() so that resources are released
	// even if Run() panics. In Go, defer is executed when the enclosing function
	// returns (not when the defer statement is reached), so Close() will still
	// run after Run() completes normally.
	defer m.Close()
	m.Run()

	if m.err == util.ErrorSuccess {
		return nil
	}
	return &util.CraneError{Code: m.err}
}
