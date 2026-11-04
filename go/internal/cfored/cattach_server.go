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

package cfored

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"errors"
	"io"
	"math"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/peer"
)

type StateOfCattachServer int

const (
	CattachWaitConnectReq  StateOfCattachServer = 1
	CattachWaitStepMeta    StateOfCattachServer = 2
	CattachWaitIOForward   StateOfCattachServer = 3
	CattachWaitJobComplete StateOfCattachServer = 4
	DeadCattach            StateOfCattachServer = 5
	End                    StateOfCattachServer = 6
)

func (cforedServer *GrpcCforedServer) CattachStream(toCattachStream protos.CraneForeD_CattachStreamServer) error {
	if !startFrontEnd() {
		return nil
	}
	defer gVars.frontEndWG.Done()

	var cattachPid int32
	var jobId uint32
	var stepId uint32
	var uid uint32
	var reply *protos.StreamCattachReply

	var execCranedIds []string
	var cattachPty bool
	// Pre-ready history: output that arrived before TaskIoRequestChannel was
	// registered.  Captured atomically with channel registration in
	// setRemoteIoToFrontChannel to guarantee no overlap with live delivery.
	var cattachPreReadyHistory []*protos.StreamStepIORequest

	taskCranedMap := make(map[uint32]string)

	RequestChannel := make(chan grpcMessage[protos.StreamCattachRequest], 8)
	receiverDone := make(chan struct{})
	defer close(receiverDone)
	go grpcStreamReceiver[protos.StreamCattachRequest](toCattachStream, RequestChannel, receiverDone)

	// Use capacity 64 for TaskIoRequestChannel to reduce the drop rate in
	// forwardRemoteIoToFront when the supervisor produces output faster than
	// cattach consumes it.
	ctldReplyChannel := make(chan *protos.StreamCtldReply, 8)
	TaskIoRequestChannel := make(chan *protos.StreamStepIORequest, 64)
	jobId = math.MaxUint32
	cattachPid = -1

	defer func() {
		step := StepIdentifier{JobId: jobId, StepId: stepId}
		gVars.ctldReplyChannelMapMtx.Lock()
		if gVars.ctldReplyChannelMapForCattachByPid[cattachPid] == ctldReplyChannel {
			delete(gVars.ctldReplyChannelMapForCattachByPid, cattachPid)
		}
		channels := gVars.ctldReplyChannelMapForCattachByStep[step]
		ownsStep := channels[cattachPid] == ctldReplyChannel
		if ownsStep {
			delete(channels, cattachPid)
			if len(channels) == 0 {
				delete(gVars.ctldReplyChannelMapForCattachByStep, step)
			}
			gVars.pidStepMapMtx.Lock()
			delete(gVars.pidStepMap, cattachPid)
			gVars.pidStepMapMtx.Unlock()
		}
		gVars.ctldReplyChannelMapMtx.Unlock()
		if ownsStep {
			gSupervisorChanKeeper.cattachStopAndRemoveChannel(jobId, stepId, cattachPid)
		}
	}()

	state := CattachWaitConnectReq

CforedCattachStateMachineLoop:
	for {
		switch state {
		case CattachWaitConnectReq:
			log.Infof("[Cfored<->Cattach] Enter State WAIT_CONNECT_REQ")
			var item grpcMessage[protos.StreamCattachRequest]
			select {
			case item = <-RequestChannel:
			case <-gVars.globalCtx.Done():
				return nil
			}
			cattachRequest, err := item.message, item.err
			if err != nil { // Failure Edge
				log.Errorf("[Cfored<-Cattach] Stream error in WaitConnectReq: %s", err)
				break CforedCattachStateMachineLoop
			}

			if cattachRequest.Type != protos.StreamCattachRequest_STEP_CONNECT_REQUEST {
				log.Errorf("[Cfored<-Cattach] Expect STEP_CONNECT_REQUEST but got %s, closing stream",
					cattachRequest.Type)
				break CforedCattachStateMachineLoop
			}

			log.Debug("[Cfored<-Cattach] Receive STEP_CONNECT_REQUEST")

			ctx := toCattachStream.Context()
			p, ok := peer.FromContext(ctx)
			if ok {
				uid = cattachRequest.GetPayloadStepConnectReq().GetUid()
				if auth, ok := p.AuthInfo.(*util.UnixPeerAuthInfo); ok {
					if uid != auth.UID {
						log.Warnf("Security: UID mismatch - peer UID %d does not match task UID %d", auth.UID, uid)
						reply = &protos.StreamCattachReply{
							Type: protos.StreamCattachReply_STEP_CONNECT_REPLY,
							Payload: &protos.StreamCattachReply_PayloadStepConnectReply{
								PayloadStepConnectReply: &protos.StreamCattachReply_StepConnectReply{
									Ok:            false,
									FailureReason: "Permission denied: caller UID does not match task UID",
								},
							},
						}

						if err := toCattachStream.Send(reply); err != nil {
							log.Error(err)
						}

						break CforedCattachStateMachineLoop
					}
				}
			}

			if !gVars.ctldConnected.Load() {
				reply = &protos.StreamCattachReply{
					Type: protos.StreamCattachReply_STEP_CONNECT_REPLY,
					Payload: &protos.StreamCattachReply_PayloadStepConnectReply{
						PayloadStepConnectReply: &protos.StreamCattachReply_StepConnectReply{
							Ok:            false,
							FailureReason: "Cfored is not connected to CraneCtld.",
						},
					},
				}

				if err := toCattachStream.Send(reply); err != nil {
					// It doesn't matter even if the connection is broken here.
					// Just print a log.
					log.Error(err)
				}

				// No need to cleaning any data
				log.Infof("[Cfored<->Cattach]Cfored not connected to CraneCtld")
				break CforedCattachStateMachineLoop
			} else {
				cattachPid = cattachRequest.GetPayloadStepConnectReq().CattachPid
				jobId = cattachRequest.GetPayloadStepConnectReq().GetJobId()
				stepId = cattachRequest.GetPayloadStepConnectReq().GetStepId()

				gVars.ctldReplyChannelMapMtx.Lock()
				// STEP_META_REPLY is routed separately from crun/calloc replies.
				gVars.ctldReplyChannelMapForCattachByPid[cattachPid] = ctldReplyChannel
				gVars.ctldReplyChannelMapMtx.Unlock()

				cforedRequest := &protos.StreamCforedRequest{
					Type: protos.StreamCforedRequest_STEP_META_REQUEST,
					Payload: &protos.StreamCforedRequest_PayloadStepMetaReq{
						PayloadStepMetaReq: &protos.StreamCforedRequest_StepMetaReq{
							Uid:        uid,
							JobId:      jobId,
							StepId:     stepId,
							CattachPid: cattachPid,
						},
					},
				}

				if !sendCtldRequest(cforedRequest) {
					return nil
				}

				state = CattachWaitStepMeta
			}
		case CattachWaitStepMeta:
			log.Infof("[Cfored<->Cattach][Pid #%d] Enter State WAIT_STEP_META", cattachPid)
			select {
			case <-gVars.globalCtx.Done():
				return nil
			case item := <-RequestChannel:
				cattachRequest, err := item.message, item.err
				if err != nil {
					log.Debug("[Cattach->Cfored] Connection to cattach was broken.")
					// ctld has not yet replied, so ctldReplyChannelMapForCattachByPid still
					// holds the channel registered in CattachWaitConnectReq. Remove it here
					// to avoid a permanent leak (the normal removal path is inside the
					// STEP_META_REPLY branch below, which we will never reach).
					gVars.ctldReplyChannelMapMtx.Lock()
					delete(gVars.ctldReplyChannelMapForCattachByPid, cattachPid)
					gVars.ctldReplyChannelMapMtx.Unlock()
					state = DeadCattach
				} else if cattachRequest != nil {
					// Unexpected message received while waiting for STEP_META_REPLY.
					// This should not happen in normal operation; log a warning and keep waiting.
					log.Warnf("[Cattach->Cfored] Unexpected message %s while waiting for step meta, ignoring.",
						cattachRequest.Type)
				}

			case ctldReply := <-ctldReplyChannel:
				switch ctldReply.Type {
				case protos.StreamCtldReply_STEP_META_REPLY:
					Ok := ctldReply.GetPayloadStepMetaReply().Ok
					failureReason := ctldReply.GetPayloadStepMetaReply().FailureReason
					log.Tracef("[Ctld->Cfored->Cattach][Pid#%d] Receive StepMeta, Ok: %v", cattachPid, Ok)

					gVars.ctldReplyChannelMapMtx.Lock()
					delete(gVars.ctldReplyChannelMapForCattachByPid, cattachPid)
					var stepInfo *protos.CattachStepInfo
					if Ok {
						// node[03-04]
						stepInfo = ctldReply.GetPayloadStepMetaReply().StepInfo
						if stepInfo == nil {
							// Job is not an attachable crun step; cattach is not supported.
							log.Warnf("[Ctld->Cfored->Cattach][Step #%d.%d] Job has no step_info, cattach rejected.", jobId, stepId)
							Ok = false
							failureReason = "cattach only supports crun steps"
						} else {
							var ok bool
							execCranedIds, ok = util.ParseHostList(stepInfo.GetNodelist())
							if !ok {
								gVars.ctldReplyChannelMapMtx.Unlock()
								state = DeadCattach
								break
							}
							cattachPty = stepInfo.GetPty()
							if cattachPty {
								// For crun with pty, only execute on first node
								execCranedIds = []string{execCranedIds[0]}
							}
							if gVars.ctldReplyChannelMapForCattachByStep[StepIdentifier{JobId: jobId, StepId: stepId}] == nil {
								gVars.ctldReplyChannelMapForCattachByStep[StepIdentifier{JobId: jobId, StepId: stepId}] = make(map[int32]chan *protos.StreamCtldReply)
							}
							gVars.ctldReplyChannelMapForCattachByStep[StepIdentifier{JobId: jobId, StepId: stepId}][cattachPid] = ctldReplyChannel
							gVars.pidStepMapMtx.Lock()
							gVars.pidStepMap[cattachPid] = StepIdentifier{JobId: jobId, StepId: stepId}
							gVars.pidStepMapMtx.Unlock()
							cattachPreReadyHistory = gSupervisorChanKeeper.setRemoteIoToFrontChannel(cattachPid, jobId, stepId, TaskIoRequestChannel)
							for cranedName, tasks := range stepInfo.GetCranedTaskMap() {
								for _, taskId := range tasks.GetTaskIds() {
									taskCranedMap[taskId] = cranedName
								}
							}
						}
					}

					gVars.ctldReplyChannelMapMtx.Unlock()

					reply = &protos.StreamCattachReply{
						Type: protos.StreamCattachReply_STEP_CONNECT_REPLY,
						Payload: &protos.StreamCattachReply_PayloadStepConnectReply{
							PayloadStepConnectReply: &protos.StreamCattachReply_StepConnectReply{
								Ok:            Ok,
								StepInfo:      stepInfo,
								FailureReason: failureReason,
							},
						},
					}

					if err := toCattachStream.Send(reply); err != nil {
						log.Debugf("[Cfored<->Cattach][Step #%d.%d] Connection to cattach was broken.", jobId, stepId)
						state = DeadCattach
						break
					}

					if Ok {
						state = CattachWaitIOForward
					} else {
						// Cattach task req failed
						// channel was already removed from ctldReplyChannelMapForCattachByPid
						log.Infof("[Cfored<->Cattach][Pid #%d] Task connect failed", cattachPid)
						break CforedCattachStateMachineLoop
					}
				default:
					// Unexpected message type while waiting for STEP_META_REPLY.
					// Clean up the pre-registration entry and terminate gracefully to
					// avoid a goroutine leak (no other code path would clean this up).
					log.Warnf("[Ctld->Cfored->Cattach][Pid #%d] Unexpected reply type %s in WaitStepMeta, terminating.",
						cattachPid, ctldReply.Type.String())
					gVars.ctldReplyChannelMapMtx.Lock()
					delete(gVars.ctldReplyChannelMapForCattachByPid, cattachPid)
					gVars.ctldReplyChannelMapMtx.Unlock()
					state = DeadCattach
				}
			}
		case CattachWaitIOForward:
			log.Infof("[Cfored<->Cattach][Step #%d.%d] Enter State WAIT_TASK_IO_FORWARD.", jobId, stepId)

			stopWaiting := atomic.Bool{}
			stopWaiting.Store(false)
			readyChannel := make(chan bool, 1)
			// Use waitAnySupervisorReady instead of waitSupervisorChannelsReady:
			// with --input=<task_id>, tasks on nodes that receive no stdin exit early
			// and their supervisors unregister before cattach connects.  Requiring ALL
			// nodes would block forever; ANY active supervisor is enough to start I/O.
			go gSupervisorChanKeeper.waitAnySupervisorReady(execCranedIds, readyChannel, &stopWaiting, jobId, stepId)

			select {
			case <-gVars.globalCtx.Done():
				stopWaiting.Store(true)
				gSupervisorChanKeeper.broadcastStopWaiting()
				return nil
			case ctldReply := <-ctldReplyChannel:
				// Always stop the waitAnySupervisorReady goroutine before transitioning,
				// regardless of message type.
				stopWaiting.Store(true)
				// Wake up the goroutine in waitSupervisorChannelsReady that may be blocked
				// in toSupervisorChannelCV.Wait() so it can observe stopWaiting == true.
				gSupervisorChanKeeper.broadcastStopWaiting()
				if ctldReply.Type != protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY {
					log.Errorf("[Ctld->Cfored->Cattach][Step #%d.%d] Expect type JOB_COMPLETION_ACK_REPLY but got %s in WaitIOForward, transitioning to DeadCattach",
						jobId, stepId, ctldReply.Type)
				} else {
					log.Debugf("[Ctld->Cfored->Cattach][Step #%d.%d] Receive JOB_COMPLETION_ACK_REPLY", jobId, stepId)
				}
				// Transition to DeadCattach regardless of the message type: any ctld
				// message received here means the step is no longer valid for I/O.
				state = DeadCattach
			case <-readyChannel:
				reply = &protos.StreamCattachReply{
					Type: protos.StreamCattachReply_TASK_IO_FORWARD_READY,
					Payload: &protos.StreamCattachReply_PayloadTaskIoForwardReadyReply{
						PayloadTaskIoForwardReadyReply: &protos.StreamCattachReply_TaskIOForwardReadyReply{
							Ok: true,
						},
					},
				}

				if err := toCattachStream.Send(reply); err != nil {
					log.Debugf("[Cfored<->Cattach][Step #%d.%d] Failed to send TASK_IO_FORWARD_READY to cattach: %s. "+
						"The connection to cattach was broken.", jobId, stepId, err.Error())
					state = End
				} else {
					state = CattachWaitJobComplete
				}
			}
		case CattachWaitJobComplete:
			log.Debugf("[Cfored<->Cattach][Job #%d] Enter State Cattach_Wait_Step_Complete", jobId)
			history := cattachPreReadyHistory
			for _, taskMsg := range history {
				if taskMsg == nil {
					log.Errorf("[Supervisor->Cfored->Cattach][Step #%d.%d] One of Craneds [%v] down. Exit....",
						jobId, stepId, execCranedIds)
					// IO Channel from Craned was shut down unexpectedly.
					state = DeadCattach
					break
				}

				if err := forwardTaskMsgToCattach(jobId, stepId, taskMsg, toCattachStream); err != nil {
					state = DeadCattach
					break
				}
			}

			if state != CattachWaitJobComplete {
				continue CforedCattachStateMachineLoop
			}

			select {
			case <-gVars.globalCtx.Done():
				return nil
			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type == protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY {
					log.Debugf("[Ctld->Cfored->Cattach][Step #%d.%d] Receive JOB_COMPLETION_ACK_REPLY "+
						"(pre-forwarding check)", jobId, stepId)
					state = DeadCattach
					continue CforedCattachStateMachineLoop
				}
				// Unexpected type: log and fall through to forwarding loop.
				log.Warningf("[Ctld->Cfored->Cattach][Step #%d.%d] Unexpected pre-forwarding ctld reply type %s, ignored",
					jobId, stepId, ctldReply.Type)
			default:
			}

		forwarding:
			for {
				select {
				case <-gVars.globalCtx.Done():
					return nil
				case ctldReply := <-ctldReplyChannel:
					if ctldReply.Type != protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY {
						log.Warningf("[Ctld->Cfored->Cattach][Step #%d.%d] Expect type JOB_COMPLETION_ACK_REPLY but got %s, ignored",
							jobId, stepId, ctldReply.Type)
					} else {
						log.Debugf("[Ctld->Cfored->Cattach][Step #%d.%d] Receive JOB_COMPLETION_ACK_REPLY", jobId, stepId)
						state = DeadCattach
						break forwarding
					}

				case item := <-RequestChannel:
					cattachRequest, err := item.message, item.err
					if err != nil {
						switch err {
						case io.EOF:
							fallthrough
						default:
							log.Debugf("[Cattach->Cfored][Step #%d.%d] Connection to cattach was broken.", jobId, stepId)
							state = End
							break forwarding
						}
					} else {
						switch cattachRequest.Type {
						case protos.StreamCattachRequest_TASK_IO_FORWARD:
							req := cattachRequest.GetPayloadTaskIoForwardReq()
							if req.TaskId != nil {
								// --input-filter: route stdin to the specific task's node only,
								// mirroring how crun_server.go uses forwardCrunRequestToSingleSupervisor
								// when TaskId is set (looked up via task_craned_map).
								node := taskCranedMap[*req.TaskId]
								if node != "" {
									log.Debugf("[Cattach->Cfored->Supervisor][Step #%d.%d] Receive TASK_IO_FORWARD to task #%d"+
										" on node %s, msg size[%d], EOF [%v]",
										jobId, stepId, *req.TaskId, node, len(req.GetMsg()), req.Eof)
									gSupervisorChanKeeper.forwardCattachRequestToSingleSupervisor(jobId, stepId, node, cattachRequest)
								} else {
									log.Warnf("[Cattach->Cfored->Supervisor][Step #%d.%d] task_id %d not found in"+
										" task_craned_map, dropping stdin", jobId, stepId, *req.TaskId)
								}
							} else {
								log.Debugf("[Cattach->Cfored->Supervisor][Step #%d.%d] Receive TASK_IO_FORWARD Request to"+
									" all tasks, msg size[%d], EOF [%v]", jobId, stepId,
									len(req.GetMsg()), req.Eof)
								gSupervisorChanKeeper.forwardCattachRequestToSupervisor(jobId, stepId, cattachRequest)
							}

						case protos.StreamCattachRequest_STEP_COMPLETION_REQUEST:
							log.Debugf("[Cattach->Cfored->Ctld][Step #%d.%d] Receive StepCompletionRequest", jobId, stepId)
							state = End
							break forwarding
						default:
							log.Errorf("[Cattach->Cfored][Step #%d.%d] Unexpected request type %s, closing session.",
								jobId, stepId, cattachRequest.Type)
							state = End
							break forwarding
						}
					}

				case taskMsg := <-TaskIoRequestChannel:
					if taskMsg == nil {
						log.Errorf("[Supervisor->Cfored->Cattach][Step #%d.%d] One of Craneds [%v] down. Exit....",
							jobId, stepId, execCranedIds)
						// IO Channel from Craned was shut down unexpectedly.
						state = DeadCattach
						break forwarding
					}

					if err := forwardTaskMsgToCattach(jobId, stepId, taskMsg, toCattachStream); err != nil {
						state = End
						break forwarding
					}
				}
			}
		case DeadCattach:
			log.Infof("[Cfored<->Cattach][Step #%d.%d] Enter State DEAD_CATTACH", jobId, stepId)
			reply = &protos.StreamCattachReply{
				Type: protos.StreamCattachReply_STEP_COMPLETION_ACK_REPLY,
				Payload: &protos.StreamCattachReply_PayloadStepCompletionAckReply{
					PayloadStepCompletionAckReply: &protos.StreamCattachReply_StepCompletionAckReply{
						Ok: true,
					},
				},
			}

			if err := toCattachStream.Send(reply); err != nil {
				log.Errorf("[Cfored->Cattach] Failed to send CompletionAck to cattach: %s. "+
					"The connection to cattach was broken.", err.Error())
			} else {
				log.Debug("[Cfored->Cattach] STEP_COMPLETION_ACK_REPLY sent to Cattach")
			}
			state = End
		case End:
			log.Infof("[Cfored<->Cattach][Job #%d] Enter State End", jobId)

			break CforedCattachStateMachineLoop
		}
	}

	return nil
}

func forwardTaskMsgToCattach(
	taskId, stepId uint32,
	taskMsg *protos.StreamStepIORequest,
	toCattachStream protos.CraneForeD_CattachStreamServer,
) error {
	var reply *protos.StreamCattachReply

	switch taskMsg.Type {
	case protos.StreamStepIORequest_TASK_OUTPUT:
		outputReq := taskMsg.GetPayloadTaskOutputReq()
		reply = &protos.StreamCattachReply{
			Type: protos.StreamCattachReply_TASK_IO_FORWARD,
			Payload: &protos.StreamCattachReply_PayloadTaskIoForwardReply{
				PayloadTaskIoForwardReply: &protos.StreamCattachReply_TaskIOForwardReply{
					Msg:    outputReq.Msg,
					TaskId: outputReq.TaskId, // pass through task_id for --output-filter and --label
				},
			},
		}
		log.Tracef("[Supervisor->Cfored->Cattach][Step #%d.%d] forwarding msg size[%d] task_id[%d]",
			taskId, stepId, len(outputReq.GetMsg()), outputReq.TaskId)
	case protos.StreamStepIORequest_TASK_ERR_OUTPUT:
		errOutputReq := taskMsg.GetPayloadTaskErrOutputReq()
		reply = &protos.StreamCattachReply{
			Type: protos.StreamCattachReply_TASK_ERR_OUTPUT_FORWARD,
			Payload: &protos.StreamCattachReply_PayloadTaskIoErrOutputForwardReply{
				PayloadTaskIoErrOutputForwardReply: &protos.StreamCattachReply_TaskIOErrOutputForwardReply{
					Msg: errOutputReq.Msg,
				},
			},
		}
		log.Tracef("[Supervisor->Cfored->Cattach][Step #%d.%d] forwarding err msg size[%d]",
			taskId, stepId, len(errOutputReq.GetMsg()))
	case protos.StreamStepIORequest_TASK_EXIT_STATUS:
		log.Tracef("[Supervisor->Cfored->Cattach][Step #%d.%d] Received TASK_EXIT_STATUS, "+
			"skipping for cattach (completion is signaled via JOB_COMPLETION_ACK_REPLY).",
			taskId, stepId)
		return nil
	default:
		// Use Errorf instead of Fatalf to avoid crashing cfored on unexpected message types.
		log.Errorf("[Supervisor->Cfored->Cattach][Step #%d.%d] Unexpected message type %s, skipping.",
			taskId, stepId, taskMsg.Type.String())
		return errors.New("unexpected taskMsg.Type")
	}

	if err := toCattachStream.Send(reply); err != nil {
		log.Debugf("[Supervisor->Cfored->Cattach][Step #%d.%d] Failed to send reply to cattach: %s. "+
			"The connection to cattach was broken.", taskId, stepId, err.Error())
		return err
	}
	return nil
}
