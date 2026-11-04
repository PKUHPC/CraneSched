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

package cfored

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"errors"
	"io"
	"math"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/peer"

	log "github.com/sirupsen/logrus"
)

type StateOfCrunServer int

const (
	CrunWaitJobIdAllocReq  StateOfCrunServer = 0
	CrunWaitCtldAllocJobId StateOfCrunServer = 1
	CrunWaitCtldAllocRes   StateOfCrunServer = 2
	CrunWaitIOForward      StateOfCrunServer = 3
	CrunWaitJobComplete    StateOfCrunServer = 4
	CrunWaitJobCancel      StateOfCrunServer = 5
	CrunWaitCtldAck        StateOfCrunServer = 6
	CancelJobOfDeadCrun    StateOfCrunServer = 7

	crunTaskIOChannelCapacity = 4096
)

func HandleSupervisorRequest(jobId uint32, stepId uint32, msg *protos.StreamStepIORequest, reply **protos.StreamCrunReply) error {
	switch msg.Type {
	case protos.StreamStepIORequest_TASK_OUTPUT:
		*reply = &protos.StreamCrunReply{
			Type: protos.StreamCrunReply_TASK_IO_FORWARD,
			Payload: &protos.StreamCrunReply_PayloadTaskIoForwardReply{
				PayloadTaskIoForwardReply: &protos.StreamCrunReply_TaskIOForwardReply{
					Msg: msg.GetPayloadTaskOutputReq().Msg,
				},
			},
		}
		log.Tracef("[Supervisor->Cfored->Crun][Step #%d.%d] fowarding msg size[%d]",
			jobId, stepId, len(msg.GetPayloadTaskOutputReq().GetMsg()))
	case protos.StreamStepIORequest_TASK_ERR_OUTPUT:
		*reply = &protos.StreamCrunReply{
			Type: protos.StreamCrunReply_TASK_ERR_OUTPUT_FORWARD,
			Payload: &protos.StreamCrunReply_PayloadTaskIoErrOutputForwardReply{
				PayloadTaskIoErrOutputForwardReply: &protos.StreamCrunReply_TaskIOErrOutputForwardReply{
					Msg: msg.GetPayloadTaskErrOutputReq().Msg,
				},
			},
		}
		log.Tracef("[Supervisor->Cfored->Crun][Step #%d.%d] fowarding err msg size[%d]",
			jobId, stepId, len(msg.GetPayloadTaskErrOutputReq().GetMsg()))

	case protos.StreamStepIORequest_STEP_X11_CONN:
		req := msg.GetPayloadStepX11FwdConnReq()
		localId := req.LocalId
		cranedId := req.CranedId
		*reply = &protos.StreamCrunReply{
			Type: protos.StreamCrunReply_STEP_X11_CONN,
			Payload: &protos.StreamCrunReply_PayloadStepX11ConnReply{
				PayloadStepX11ConnReply: &protos.StreamCrunReply_StepX11ConnReply{
					LocalId:  localId,
					CranedId: cranedId,
				},
			},
		}
		log.Tracef("[Supervisor->Cfored->Crun][Step #%d.%d][X11 #%d] fowarding x11 conn request to craned %s",
			jobId, stepId, localId, cranedId)

	case protos.StreamStepIORequest_STEP_X11_OUTPUT:
		req := msg.GetPayloadStepX11OutputReq()
		*reply = &protos.StreamCrunReply{
			Type: protos.StreamCrunReply_STEP_X11_FORWARD,
			Payload: &protos.StreamCrunReply_PayloadStepX11ForwardReply{
				PayloadStepX11ForwardReply: &protos.StreamCrunReply_StepX11ForwardReply{
					Msg:      req.Msg,
					LocalId:  req.LocalId,
					CranedId: req.CranedId,
				},
			},
		}
		log.Tracef("[Supervisor->Cfored->Crun][Step #%d.%d][X11 #%d] fowarding x11 msg from craned %s len: [%d]",
			jobId, stepId, req.GetLocalId(), req.GetCranedId(), len(msg.GetPayloadStepX11OutputReq().Msg))

	case protos.StreamStepIORequest_STEP_X11_EOF:
		req := msg.GetPayloadStepX11EofReq()
		*reply = &protos.StreamCrunReply{
			Type: protos.StreamCrunReply_STEP_X11_EOF,
			Payload: &protos.StreamCrunReply_PayloadStepX11EofReply{
				PayloadStepX11EofReply: &protos.StreamCrunReply_StepX11EofReply{
					LocalId:  req.LocalId,
					CranedId: req.CranedId,
				},
			},
		}
		log.Tracef("[Supervisor->Cfored->Crun][Step #%d.%d][X11 #%d] fowarding x11 eof from craned %s",
			jobId, stepId, req.GetLocalId(), req.GetCranedId())

	case protos.StreamStepIORequest_TASK_EXIT_STATUS:
		req := msg.GetPayloadTaskExitStatusReq()
		*reply = &protos.StreamCrunReply{
			Type: protos.StreamCrunReply_TASK_EXIT_STATUS,
			Payload: &protos.StreamCrunReply_PayloadTaskExitStatusReply{
				PayloadTaskExitStatusReply: &protos.StreamCrunReply_TaskExitStatusReply{
					TaskId:   req.TaskId,
					ExitCode: req.ExitCode,
					Signaled: req.Signaled,
				},
			},
		}
		log.Tracef("[Supervisor->Cfored->Crun][Step #%d.%d][Task #%d] forwarding task exit msg ",
			jobId, stepId, req.TaskId)
	default:
		log.Fatalf("[Supervisor->Cfored->Crun][Step #%d.%d] Expect Type TASK_OUTPUT or STEP_X11_OUTPUT or TASK_EXIT_STATUS.",
			jobId, stepId)
		return errors.New("Unexpected Job IO Message Type")
	}
	return nil
}

func drainReadyStepIO(channel <-chan *protos.StreamStepIORequest,
	forward func(*protos.StreamStepIORequest) bool) bool {
	for {
		select {
		case message := <-channel:
			if !forward(message) {
				return false
			}
		default:
			return true
		}
	}
}

func (cforedServer *GrpcCforedServer) CrunStream(toCrunStream protos.CraneForeD_CrunStreamServer) error {
	if !startFrontEnd() {
		return nil
	}
	defer gVars.frontEndWG.Done()

	var crunPid int32
	var jobId uint32
	var stepId uint32
	var reply *protos.StreamCrunReply
	var step StepIdentifier

	var execCranedIds []string
	var crunPty bool
	var craned_tasks_map map[string][]uint32
	var task_craned_map map[uint32]string
	//Whether crun down before get jobid
	crunDownWithoutJobId := false
	crunRequestChannel := make(chan grpcMessage[protos.StreamCrunRequest], 8)
	receiverDone := make(chan struct{})
	defer close(receiverDone)
	go grpcStreamReceiver[protos.StreamCrunRequest](toCrunStream, crunRequestChannel, receiverDone)

	ctldReplyChannel := make(chan *protos.StreamCtldReply, 2)
	StepIoRequestChannel := make(chan *protos.StreamStepIORequest, crunTaskIOChannelCapacity)
	jobId = math.MaxUint32
	crunPid = -1
	forwardEstablished := atomic.Bool{}
	forwardEstablished.Store(false)

	defer func() {
		gVars.ctldReplyChannelMapMtx.Lock()
		if gVars.ctldReplyChannelMapByPid[crunPid] == ctldReplyChannel {
			delete(gVars.ctldReplyChannelMapByPid, crunPid)
		}
		ownsStep := gVars.ctldReplyChannelMapByStep[step] == ctldReplyChannel
		if ownsStep {
			delete(gVars.ctldReplyChannelMapByStep, step)
			gVars.pidStepMapMtx.Lock()
			delete(gVars.pidStepMap, crunPid)
			gVars.pidStepMapMtx.Unlock()
		}
		gVars.ctldReplyChannelMapMtx.Unlock()
		if ownsStep {
			gSupervisorChanKeeper.crunStepStopAndRemoveChannel(jobId, stepId)
		}
	}()

	state := CrunWaitJobIdAllocReq

CforedCrunStateMachineLoop:
	for {
		switch state {
		case CrunWaitJobIdAllocReq:
			log.Infof("[Cfored<->Crun] Enter State WAIT_JOB_ID_ALLOC_REQ")

			var item grpcMessage[protos.StreamCrunRequest]
			select {
			case item = <-crunRequestChannel:
			case <-gVars.globalCtx.Done():
				return nil
			}
			crunRequest, err := item.message, item.err
			// If crun down before req job id, do nothing
			if err != nil {
				return err
			}

			if crunRequest.Type != protos.StreamCrunRequest_JOB_REQUEST && crunRequest.Type != protos.StreamCrunRequest_STEP_REQUEST {
				log.Fatalf("[Cfored<-Crun] Expect JOB_REQUEST or STEP_REQUEST but got %s", crunRequest.Type)
				break
			}

			log.Debugf("[Cfored<-Crun] Receive %s", crunRequest.Type)

			ctx := toCrunStream.Context()
			p, ok := peer.FromContext(ctx)
			if ok {
				if auth, ok := p.AuthInfo.(*util.UnixPeerAuthInfo); ok {

					var uid uint32
					if crunRequest.Type == protos.StreamCrunRequest_JOB_REQUEST {
						uid = crunRequest.GetPayloadJobReq().Job.Uid
					} else {
						uid = crunRequest.GetPayloadStepReq().Step.Uid
					}
					if uid != auth.UID {
						log.Warnf("Security: UID mismatch - peer UID %d does not match job UID %d", auth.UID, crunRequest.GetPayloadJobReq().Job.Uid)
						reply = &protos.StreamCrunReply{
							Type: protos.StreamCrunReply_STEP_ID_REPLY,
							Payload: &protos.StreamCrunReply_PayloadStepIdReply{
								PayloadStepIdReply: &protos.StreamCrunReply_StepIdReply{
									Ok:            false,
									FailureReason: "Permission denied: caller UID does not match job UID",
								},
							},
						}

						if err := toCrunStream.Send(reply); err != nil {
							log.Error(err)
						}

						break CforedCrunStateMachineLoop
					}
				}
			}

			if !gVars.ctldConnected.Load() {
				reply = &protos.StreamCrunReply{
					Type: protos.StreamCrunReply_STEP_ID_REPLY,
					Payload: &protos.StreamCrunReply_PayloadStepIdReply{
						PayloadStepIdReply: &protos.StreamCrunReply_StepIdReply{
							Ok:            false,
							FailureReason: "Cfored is not connected to CraneCtld.",
						},
					},
				}

				if err := toCrunStream.Send(reply); err != nil {
					// It doesn't matter even if the connection is broken here.
					// Just print a log.
					log.Error(err)
				}

				// No need to cleaning any data
				log.Infof("[Cfored<->Crun]Cfored not connected to CraneCtld")
				break CforedCrunStateMachineLoop
			} else {
				if crunRequest.Type == protos.StreamCrunRequest_JOB_REQUEST {
					crunPid = crunRequest.GetPayloadJobReq().CrunPid
				} else {
					crunPid = crunRequest.GetPayloadStepReq().CrunPid
				}

				gVars.ctldReplyChannelMapMtx.Lock()
				gVars.ctldReplyChannelMapByPid[crunPid] = ctldReplyChannel
				gVars.ctldReplyChannelMapMtx.Unlock()
				var iaMeta *protos.InteractiveJobAdditionalMeta
				if crunRequest.Type == protos.StreamCrunRequest_JOB_REQUEST {
					iaMeta = crunRequest.GetPayloadJobReq().Job.GetInteractiveMeta()
				} else {
					iaMeta = crunRequest.GetPayloadStepReq().Step.GetInteractiveMeta()
				}
				iaMeta.CforedName = gVars.hostName
				crunPty = iaMeta.Pty
				var cforedRequest *protos.StreamCforedRequest
				if crunRequest.Type == protos.StreamCrunRequest_JOB_REQUEST {
					cforedRequest = &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_JOB_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadJobReq{
							PayloadJobReq: &protos.StreamCforedRequest_JobReq{
								CforedName: gVars.hostName,
								Pid:        crunPid,
								Job:        crunRequest.GetPayloadJobReq().Job,
							},
						},
					}
				} else {
					cforedRequest = &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_STEP_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadStepReq{
							PayloadStepReq: &protos.StreamCforedRequest_StepReq{
								CforedName: gVars.hostName,
								Pid:        crunPid,
								Step:       crunRequest.GetPayloadStepReq().Step,
							},
						},
					}
				}

				if !sendCtldRequest(cforedRequest) {
					return nil
				}

				state = CrunWaitCtldAllocJobId
			}

		case CrunWaitCtldAllocJobId:
			log.Infof("[Cfored<->Crun][Pid #%d] Enter State WAIT_CTLD_ALLOC_JOB_ID", crunPid)

			select {
			case <-gVars.globalCtx.Done():
				return nil
			case item := <-crunRequestChannel:
				crunRequest, err := item.message, item.err
				if err != nil {
					// EOF meaning Crun closed, otherwise RPC error.
					// Crun dead, cfored will cancel this job, keep waiting job id
					crunDownWithoutJobId = true
					log.Debug("[Crun->Cfored] Connection to crun was broken.")
					//Will wait for ctldReplyChannel to get job id
				} else if crunRequest != nil || err == nil {
					log.Fatal("[Crun->Cfored] Expect only nil (crun connection broken) here!")
				}

			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_JOB_ID_REPLY {
					log.Fatalf("[Ctld->Cfored->Crun][Pid#%d] Expect type JOB_ID_REPLY", crunPid)
				}

				Ok := ctldReply.GetPayloadJobIdReply().Ok
				jobId = ctldReply.GetPayloadJobIdReply().JobId
				stepId = ctldReply.GetPayloadJobIdReply().StepId
				step = StepIdentifier{JobId: jobId, StepId: stepId}
				log.Tracef("[Ctld->Cfored->Crun][Pid#%d] Receive JobIdReply, Ok: %v, JobId #%d, StepId #%d",
					crunPid, Ok, jobId, stepId)

				gVars.ctldReplyChannelMapMtx.Lock()
				delete(gVars.ctldReplyChannelMapByPid, crunPid)
				if Ok {
					gVars.ctldReplyChannelMapByStep[step] = ctldReplyChannel
					gVars.pidStepMapMtx.Lock()
					gVars.pidStepMap[crunPid] = step
					gVars.pidStepMapMtx.Unlock()
					gSupervisorChanKeeper.setRemoteIoToFrontChannel(crunPid, jobId, stepId, StepIoRequestChannel)
				}

				gVars.ctldReplyChannelMapMtx.Unlock()
				if crunDownWithoutJobId {
					if !Ok {
						// Submit not successful, no need to cancel
						break CforedCrunStateMachineLoop
					}
					// Crun was down when CrunWaitCtldAllocJobId, just cancel job.
					state = CancelJobOfDeadCrun
					break
				}

				reply = &protos.StreamCrunReply{
					Type: protos.StreamCrunReply_STEP_ID_REPLY,
					Payload: &protos.StreamCrunReply_PayloadStepIdReply{
						PayloadStepIdReply: &protos.StreamCrunReply_StepIdReply{
							Ok:            Ok,
							JobId:         jobId,
							StepId:        stepId,
							FailureReason: ctldReply.GetPayloadJobIdReply().FailureReason,
						},
					},
				}

				if err := toCrunStream.Send(reply); err != nil {
					log.Debugf("[Cfored<->Crun][Step #%d.%d] Connection to crun was broken.", jobId, stepId)
					if !Ok {
						break CforedCrunStateMachineLoop
					}
					state = CancelJobOfDeadCrun
					break
				}

				if Ok {
					state = CrunWaitCtldAllocRes
				} else {
					// Crun job req failed
					// channel was already removed from gVars.ctldReplyChannelMapByPid
					log.Infof("[Cfored<->Crun][Pid #%d] Job request failed", crunPid)
					break CforedCrunStateMachineLoop
				}

			}

		case CrunWaitCtldAllocRes:
			log.Infof("[Cfored<->Crun][Step #%d.%d] Enter State WAIT_CTLD_ALLOC_RES", jobId, stepId)

			select {
			case <-gVars.globalCtx.Done():
				return nil
			case item := <-crunRequestChannel:
				crunRequest, err := item.message, item.err
				if err != nil {
					log.Debugf("[Crun->Cfored][Step #%d.%d] Connection to crun was broken.", jobId, stepId)
					state = CancelJobOfDeadCrun
					break
				}

				if crunRequest != nil || err == nil {
					if crunRequest.Type != protos.StreamCrunRequest_STEP_COMPLETION_REQUEST {
						log.Fatalf("[Crun->Cfored][Step #%d.%d] Expect JobCompletionRequest here!", jobId, stepId)
					} else {
						log.Debugf("[Crun->Cfored][Step #%d.%d] Receive JobCompletionRequest", jobId, stepId)
						toCtldRequest := &protos.StreamCforedRequest{
							Type: protos.StreamCforedRequest_JOB_COMPLETION_REQUEST,
							Payload: &protos.StreamCforedRequest_PayloadJobCompleteReq{
								PayloadJobCompleteReq: &protos.StreamCforedRequest_JobCompleteReq{
									CforedName:      gVars.hostName,
									JobId:           jobId,
									StepId:          stepId,
									InteractiveType: protos.InteractiveJobType_Crun,
								},
							},
						}
						if !sendCtldRequest(toCtldRequest) {
							return nil
						}
						state = CrunWaitCtldAck
					}
				}

			case ctldReply := <-ctldReplyChannel:
				switch ctldReply.Type {
				case protos.StreamCtldReply_JOB_RES_ALLOC_REPLY:
					ctldPayload := ctldReply.GetPayloadJobResAllocReply()
					reply = &protos.StreamCrunReply{
						Type: protos.StreamCrunReply_STEP_RES_ALLOC_REPLY,
						Payload: &protos.StreamCrunReply_PayloadStepAllocReply{
							PayloadStepAllocReply: &protos.StreamCrunReply_StepResAllocatedReply{
								Ok:                   ctldPayload.Ok,
								AllocatedCranedRegex: ctldPayload.AllocatedCranedRegex,
								CranedIds:            ctldPayload.CranedIds,
								NtasksTotal:          ctldPayload.NtasksTotal,
							},
						},
					}

					// TODO: Difference
					if crunPty {
						// For crun with pty, only execute on first node
						execCranedIds = []string{ctldPayload.GetCranedIds()[0]}
					} else {
						execCranedIds = ctldPayload.GetCranedIds()
					}
					craned_tasks_map = make(map[string][]uint32)
					task_craned_map = make(map[uint32]string)
					for craned, tasks := range ctldPayload.CranedTaskMap {
						for _, task := range tasks.TaskIds {
							task_craned_map[task] = craned
						}
						craned_tasks_map[craned] = tasks.TaskIds
					}
					log.Tracef("[Ctld->Cfored->Crun][Step #%d.%d] Receive JobResAllocReply with node %v",
						jobId, stepId, execCranedIds)

					if err := toCrunStream.Send(reply); err != nil {
						log.Debug("[Cfored->Crun] Connection to crun was broken.")
						state = CancelJobOfDeadCrun
					} else {
						state = CrunWaitIOForward
					}

				case protos.StreamCtldReply_JOB_CANCEL_REQUEST:
					log.Debugf("[Ctld->Cfored->Crun][Step #%d.%d] Receive JobCancelRequest", jobId, stepId)
					state = CrunWaitJobCancel

				default:
					log.Fatalf("[Ctld->Cfored->Crun][Step #%d.%d] Expect type "+
						"JOB_RES_ALLOC_REPLY or JOB_CANCEL_REQUEST but got %s", jobId, stepId, ctldReply.Type)
				}
			}

		case CrunWaitIOForward:
			log.Infof("[Cfored<->Crun][Step #%d.%d] Enter State WAIT_TASK_IO_FORWARD.", jobId, stepId)

			stopWaiting := atomic.Bool{}
			stopWaiting.Store(false)
			readyChannel := make(chan bool, 1)
			go gSupervisorChanKeeper.waitSupervisorChannelsReady(execCranedIds, readyChannel, &stopWaiting, jobId, stepId)

			select {
			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_JOB_CANCEL_REQUEST {
					log.Fatalf("[Ctld->Cfored->Crun][Step #%d.%d] Expect type JOB_CANCEL_REQUEST but got %s, ignored",
						jobId, stepId, ctldReply.Type)
				} else {
					log.Debugf("[Ctld->Cfored->Crun][Step #%d.%d] Receive JobCancelRequest", jobId, stepId)
					state = CrunWaitJobCancel
				}
				stopWaiting.Store(true)
				// Wake up the goroutine in waitSupervisorChannelsReady that may be blocked
				// in toSupervisorChannelCV.Wait() so it can observe stopWaiting == true.
				gSupervisorChanKeeper.broadcastStopWaiting()

			case <-gVars.globalCtx.Done():
				stopWaiting.Store(true)
				gSupervisorChanKeeper.broadcastStopWaiting()
				return nil

			case item := <-crunRequestChannel:
				crunRequest, err := item.message, item.err
				if err != nil {
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Debugf("[Crun->Cfored][Step #%d.%d] Connection to crun was broken.", jobId, stepId)
						stopWaiting.Store(true)
						gSupervisorChanKeeper.broadcastStopWaiting()
						state = CancelJobOfDeadCrun
					}
					break
				}

				if crunRequest.Type != protos.StreamCrunRequest_STEP_COMPLETION_REQUEST {
					log.Fatalf("[Crun->Cfored][Step #%d.%d] Expect JOB_COMPLETION_REQUEST.", jobId, stepId)
				}

				log.Debugf("[Crun->Cfored->Ctld][Step #%d.%d] Receive JobCompletionRequest", jobId, stepId)
				toCtldRequest := &protos.StreamCforedRequest{
					Type: protos.StreamCforedRequest_JOB_COMPLETION_REQUEST,
					Payload: &protos.StreamCforedRequest_PayloadJobCompleteReq{
						PayloadJobCompleteReq: &protos.StreamCforedRequest_JobCompleteReq{
							CforedName:      gVars.hostName,
							JobId:           jobId,
							StepId:          stepId,
							InteractiveType: protos.InteractiveJobType_Crun,
						},
					},
				}
				stopWaiting.Store(true)
				gSupervisorChanKeeper.broadcastStopWaiting()
				if !sendCtldRequest(toCtldRequest) {
					return nil
				}
				state = CrunWaitCtldAck

			case <-readyChannel:
				reply = &protos.StreamCrunReply{
					Type: protos.StreamCrunReply_TASK_IO_FORWARD_READY,
					Payload: &protos.StreamCrunReply_PayloadTaskIoForwardReadyReply{
						PayloadTaskIoForwardReadyReply: &protos.StreamCrunReply_TaskIOForwardReadyReply{
							Ok: true,
						},
					},
				}
				forwardEstablished.Store(true)

				if err := toCrunStream.Send(reply); err != nil {
					log.Debugf("[Cfored<->Crun][Step #%d.%d] Failed to send TASK_IO_FORWARD_READY to crun: %s. "+
						"The connection to crun was broken.", jobId, stepId, err.Error())
					state = CancelJobOfDeadCrun
				} else {
					state = CrunWaitJobComplete
				}
			}

		case CrunWaitJobComplete:
			log.Debugf("[Cfored<->Crun][Step #%d.%d] Enter State CRUN_WAIT_JOB_COMPLETE", jobId, stepId)
		forwarding:
			for {
				select {
				case <-gVars.globalCtx.Done():
					return nil
				case ctldReply := <-ctldReplyChannel:
					if ctldReply.Type != protos.StreamCtldReply_JOB_CANCEL_REQUEST {
						log.Warningf("[Ctld->Cfored->Crun][Step #%d.%d] Expect type JOB_CANCEL_REQUEST but got %s, ignored",
							jobId, stepId, ctldReply.Type)
					} else {
						log.Debugf("[Ctld->Cfored->Crun][Step #%d.%d] Receive JobCancelRequest", jobId, stepId)
						state = CrunWaitJobCancel
						break forwarding
					}

				case item := <-crunRequestChannel:
					crunRequest, err := item.message, item.err
					if err != nil {
						switch err {
						case io.EOF:
							fallthrough
						default:
							log.Debugf("[Crun->Cfored][Step #%d.%d] Connection to crun was broken.", jobId, stepId)
							state = CancelJobOfDeadCrun
							break forwarding
						}
					} else {
						switch crunRequest.Type {
						case protos.StreamCrunRequest_TASK_IO_FORWARD:
							req := crunRequest.GetPayloadTaskIoForwardReq()

							if req.TaskId != nil {
								node := task_craned_map[*req.TaskId]
								log.Debugf("[Crun->Cfored->Supervisor][Step #%d.%d] Receive TASK_IO_FORWARD Request to task #%d"+
									"on node %s msg size[%d], EOF [%v]",
									jobId, stepId, *req.TaskId, node, len(req.GetMsg()), req.Eof)
								gSupervisorChanKeeper.forwardCrunRequestToSingleSupervisor(jobId, stepId,
									node, crunRequest)
							} else {
								log.Debugf("[Crun->Cfored->Supervisor][Step #%d.%d] Receive TASK_IO_FORWARD Request to tasks msg size[%d], EOF [%v]",
									jobId, stepId, len(req.GetMsg()), req.Eof)
								gSupervisorChanKeeper.forwardCrunRequestToSupervisor(jobId, stepId, crunRequest)
							}

						case protos.StreamCrunRequest_STEP_X11_FORWARD:

							payload := crunRequest.GetPayloadStepX11ForwardReq()
							log.Debugf("[Crun->Cfored->Supervisor][Step #%d.%d][X11 #%d] Receive Local STEP_X11_FORWARD to craned %s",
								jobId, stepId, payload.GetLocalId(), payload.GetCranedId())
							gSupervisorChanKeeper.forwardCrunRequestToSingleSupervisor(jobId, stepId, payload.GetCranedId(), crunRequest)

						case protos.StreamCrunRequest_TERMINAL_RESIZE:
							payload := crunRequest.GetPayloadTerminalResizeReq()
							taskId := payload.GetTaskId()
							node, ok := task_craned_map[taskId]
							if !ok {
								if shouldLogTerminalResizeDiagnostic() {
									log.Warningf(
										"[Crun->Cfored][Step #%d.%d] Ignoring terminal resize for unknown task #%d",
										jobId, stepId, taskId,
									)
								}
								break
							}
							log.Tracef(
								"[Crun->Cfored->Supervisor][Step #%d.%d] Forwarding terminal resize for task #%d to Craned %s",
								jobId, stepId, taskId, node,
							)
							gSupervisorChanKeeper.forwardTerminalResizeToSingleSupervisor(
								jobId, stepId, node, crunRequest,
							)

						case protos.StreamCrunRequest_STEP_COMPLETION_REQUEST:
							log.Debugf("[Crun->Cfored->Ctld][Step #%d.%d] Receive JobCompletionRequest", jobId, stepId)
							toCtldRequest := &protos.StreamCforedRequest{
								Type: protos.StreamCforedRequest_JOB_COMPLETION_REQUEST,
								Payload: &protos.StreamCforedRequest_PayloadJobCompleteReq{
									PayloadJobCompleteReq: &protos.StreamCforedRequest_JobCompleteReq{
										CforedName:      gVars.hostName,
										JobId:           jobId,
										StepId:          stepId,
										InteractiveType: protos.InteractiveJobType_Crun,
									},
								},
							}
							if !sendCtldRequest(toCtldRequest) {
								return nil
							}
							state = CrunWaitCtldAck
							break forwarding
						default:
							log.Fatalf("[Crun->Cfored][Step #%d.%d] Expect JOB_COMPLETION_REQUEST, TASK_IO_FORWARD, STEP_X11_FORWARD or TERMINAL_RESIZE",
								jobId, stepId)
							break forwarding
						}
					}

				case jobMsg := <-StepIoRequestChannel:
					if jobMsg == nil {
						log.Errorf("[Supervisor->Cfored->Crun][Step #%d.%d] One of Craneds [%v] down. Cancelling the job...",
							jobId, stepId, execCranedIds)
						// IO Channel from Craned was shut down unexpectedly.
						state = CrunWaitJobCancel
						break forwarding
					}

					err := HandleSupervisorRequest(jobId, stepId, jobMsg, &reply)
					if err != nil {
						break forwarding
					}

					if err = toCrunStream.Send(reply); err != nil {
						log.Debugf("[Cfored->Crun][Step #%d.%d] Failed to send %s to crun: %s. "+
							"The connection to crun was broken.", jobId, stepId, jobMsg.Type.String(), err.Error())
						state = CancelJobOfDeadCrun
						break forwarding
					}
				}
			}

		case CrunWaitJobCancel:
			log.Debugf("[Cfored<->Crun][Step #%d.%d] Enter State WAIT_CRUN_CANCEL. Sending JOB_CANCEL_REQUEST to Crun...",
				jobId, stepId)

			reply = &protos.StreamCrunReply{
				Type: protos.StreamCrunReply_STEP_CANCEL_REQUEST,
				Payload: &protos.StreamCrunReply_PayloadStepCancelRequest{
					PayloadStepCancelRequest: &protos.StreamCrunReply_StepCancelRequest{},
				},
			}

			if err := toCrunStream.Send(reply); err != nil {
				log.Debugf("[Cfored->Crun][Step #%d.%d]  Failed to send CancelRequest to crun: %s. "+
					"The connection to crun was broken.", jobId, stepId, err.Error())
				state = CancelJobOfDeadCrun
				break
			}

			crunErr := false
		waitingCompleteReq:
			for {
				select {
				case <-gVars.globalCtx.Done():
					return nil
				//If we don't process the Task IO from supervisor here, it may block the TaskIO server forever dur to channel full,
				// And supervisor unregister msg will not be processed, causing supervisor blocked.
				case jobMsg := <-StepIoRequestChannel:
					if jobMsg == nil {
						log.Errorf("[Supervisor->Cfored->Crun][Step #%d.%d] One of Craneds [%v] down during cancellation, ignored. ",
							jobId, stepId, execCranedIds)
						// IO Channel from Craned was shut down unexpectedly.
						break
					}

					// During cancellation, just forward the IO messages from supervisor to crun, ignore errors
					if err := HandleSupervisorRequest(jobId, stepId, jobMsg, &reply); err != nil {
						break
					}

					if err := toCrunStream.Send(reply); err != nil {
						log.Debugf("[Cfored->Crun][Step #%d.%d] Failed to send %s to crun: %s. "+
							"The connection to crun was broken.", jobId, stepId, jobMsg.Type.String(), err.Error())
						state = CancelJobOfDeadCrun
						break waitingCompleteReq
					}
				case item := <-crunRequestChannel:
					crunRequest, err := item.message, item.err
					if err != nil { // Failure Edge
						switch err {
						case io.EOF:
							fallthrough
						default:
							log.Debugf("[Cfored<->Crun][Step #%d.%d] Connection to crun was broken.", jobId, stepId)
							state = CancelJobOfDeadCrun
							crunErr = true
						}
						break waitingCompleteReq
					}
					if crunRequest.Type != protos.StreamCrunRequest_STEP_COMPLETION_REQUEST {
						log.Warningf("[Crun->Cfored][Step #%d.%d] Expect JOB_COMPLETION_REQUEST but %s received. Ignoring it...",
							jobId, stepId, crunRequest.Type)
					} else {
						log.Tracef("[Crun->Cfored][Step #%d.%d] JOB_COMPLETION_REQUEST received",
							jobId, stepId)
						break waitingCompleteReq
					}
				}

			}
			if crunErr {
				break
			}
			log.Debugf("[Crun->Cfored][Step #%d.%d] Receive JobCompletionRequest", jobId, stepId)

			toCtldRequest := &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_JOB_COMPLETION_REQUEST,
				Payload: &protos.StreamCforedRequest_PayloadJobCompleteReq{
					PayloadJobCompleteReq: &protos.StreamCforedRequest_JobCompleteReq{
						CforedName:      gVars.hostName,
						JobId:           jobId,
						StepId:          stepId,
						InteractiveType: protos.InteractiveJobType_Crun,
					},
				},
			}
			if !sendCtldRequest(toCtldRequest) {
				return nil
			}

			state = CrunWaitCtldAck

		case CrunWaitCtldAck:
			log.Infof("[Cfored<->Crun][Step #%d.%d] Enter State WAIT_CTLD_ACK", jobId, stepId)

			gotReply := false
			var completionAckReply *protos.StreamCrunReply
		waitingAck:
			for {
				select {
				case <-gVars.globalCtx.Done():
					return nil
				//If we don't process the Task IO from supervisor here, it may block the TaskIO server forever dur to channel full,
				// And supervisor unregister msg will not be processed, causing supervisor blocked.
				case jobMsg := <-StepIoRequestChannel:
					if jobMsg == nil {
						log.Errorf("[Supervisor->Cfored->Crun][Step #%d.%d] One of Craneds [%v] down during cancellation, ignored. ",
							jobId, stepId, execCranedIds)
						// IO Channel from Craned was shut down unexpectedly.
						break
					}

					// During cancellation, just forward the IO messages from supervisor to crun, ignore errors
					if err := HandleSupervisorRequest(jobId, stepId, jobMsg, &reply); err != nil {
						break
					}

					if err := toCrunStream.Send(reply); err != nil {
						log.Debugf("[Cfored->Crun][Step #%d.%d] Failed to send %s to crun: %s. "+
							"The connection to crun was broken.", jobId, stepId, jobMsg.Type.String(), err.Error())
						gotReply = false
						break waitingAck
					}

				case ctldReply := <-ctldReplyChannel:
					if ctldReply.Type != protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY {
						log.Warningf("[Ctld->Cfored->Crun][Step #%d.%d]  Expect JOB_COMPLETION_ACK_REPLY, "+
							"but %s received. Ignoring it...", jobId, stepId, ctldReply.Type)
						break
					} else {
						log.Tracef("[Ctld->Cfored->Crun][Step #%d.%d]  JOB_COMPLETION_ACK_REPLY received",
							jobId, stepId)
					}

					completionAckReply = &protos.StreamCrunReply{
						Type: protos.StreamCrunReply_STEP_COMPLETION_ACK_REPLY,
						Payload: &protos.StreamCrunReply_PayloadStepCompletionAckReply{
							PayloadStepCompletionAckReply: &protos.StreamCrunReply_StepCompletionAckReply{
								Ok: true,
							},
						},
					}
					gotReply = true
					break waitingAck
				}
			}
			// After receiving CraneCtld ACK, drain remaining supervisor I/O
			// (e.g. TASK_EXIT_STATUS) before sending ACK to crun. Wait until
			// all supervisors have unregistered (stepDoneCh closed) or timeout.
			drainFailed := false
			if gotReply {
				stepDoneCh := gSupervisorChanKeeper.getStepDoneChannel(jobId, stepId)
				drainTimeout := time.After(5 * time.Second)
				forwardDrainMessage := func(stepMsg *protos.StreamStepIORequest) bool {
					if stepMsg == nil {
						return false
					}
					var drainReply *protos.StreamCrunReply
					if err := HandleSupervisorRequest(jobId, stepId, stepMsg, &drainReply); err != nil {
						return false
					}
					if err := toCrunStream.Send(drainReply); err != nil {
						log.Debugf("[Cfored->Crun][Step #%d.%d] Drain: failed to send %s to crun: %s.",
							jobId, stepId, stepMsg.Type.String(), err.Error())
						return false
					}
					return true
				}
			drainLoop:
				for {
					select {
					case <-gVars.globalCtx.Done():
						return nil
					case stepMsg := <-StepIoRequestChannel:
						if !forwardDrainMessage(stepMsg) {
							drainFailed = true
							break drainLoop
						}
					case <-stepDoneCh:
						// SUPERVISOR_UNREGISTER is ordered after that supervisor's I/O.
						// Once all supervisors are done, drain everything already queued.
						if drainReadyStepIO(StepIoRequestChannel, forwardDrainMessage) {
							log.Debugf("[Cfored<->Crun][Step #%d.%d] All supervisors unregistered, drain complete.",
								jobId, stepId)
						} else {
							drainFailed = true
						}
						break drainLoop
					case <-drainTimeout:
						log.Warningf("[Cfored<->Crun][Step #%d.%d] Drain timeout (5s), proceeding with cleanup.",
							jobId, stepId)
						break drainLoop
					}
				}
			}

			if gotReply && drainFailed {
				log.Errorf("[Cfored<->Crun][Step #%d.%d] Supervisor I/O drain failed; sending a failed completion result.",
					jobId, stepId)
				completionAckReply.GetPayloadStepCompletionAckReply().Ok = false
			}
			if gotReply {
				if err := toCrunStream.Send(completionAckReply); err != nil {
					log.Errorf("[Cfored->Crun][Step #%d.%d] Failed to send CompletionAck to crun: %s. "+
						"The connection to crun was broken.", jobId, stepId, err.Error())
				} else {
					log.Debugf("[Cfored->Crun][Step #%d.%d] Completion result sent to Crun (ok=%v)",
						jobId, stepId, completionAckReply.GetPayloadStepCompletionAckReply().Ok)
				}
			} else {
				log.Debugf("[Cfored->Crun][Step #%d.%d] No need to send CompletionAck to crun as crun already down.", jobId, stepId)
			}
			if gotReply && drainFailed {
				log.Errorf("[Cfored<->Crun][Step #%d.%d] Step completion failed because supervisor I/O could not be drained.",
					jobId, stepId)
			} else {
				log.Infof("[Cfored<->Crun][Step #%d.%d] Step completed successfully", jobId, stepId)
			}

			break CforedCrunStateMachineLoop

		case CancelJobOfDeadCrun:
			log.Infof("[Cfored<->Crun][Step #%d.%d] Enter State CANCEL_JOB_OF_DEAD_CRUN", jobId, stepId)

			toCtldRequest := &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_JOB_COMPLETION_REQUEST,
				Payload: &protos.StreamCforedRequest_PayloadJobCompleteReq{
					PayloadJobCompleteReq: &protos.StreamCforedRequest_JobCompleteReq{
						CforedName:      gVars.hostName,
						JobId:           jobId,
						StepId:          stepId,
						InteractiveType: protos.InteractiveJobType_Crun,
					},
				},
			}
			if !sendCtldRequest(toCtldRequest) {
				return nil
			}

			// Drain StepIoRequestChannel concurrently while waiting for the ACK.
			// This prevents forwardRemoteIoToFront from dropping supervisor output
			// due to a full channel, and mirrors the same pattern used by
			// CrunWaitCtldAck and CrunWaitJobCancel.
		waitForDeadCrunAck:
			for {
				select {
				case <-gVars.globalCtx.Done():
					return nil
				case jobMsg := <-StepIoRequestChannel:
					if jobMsg == nil {
						log.Errorf("[Supervisor->Cfored->Crun][Step #%d.%d] Craned down during CancelJobOfDeadCrun, ignored.",
							jobId, stepId)
						break
					}
					// Discard IO messages during cancellation; crun is already gone.
					log.Tracef("[Supervisor->Cfored->Crun][Step #%d.%d] Discarding supervisor IO during CancelJobOfDeadCrun.",
						jobId, stepId)

				case ctldReply := <-ctldReplyChannel:
					if ctldReply.Type != protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY {
						log.Tracef("[Cfored<->Crun] Expect JOB_COMPLETION_ACK_REPLY from Ctld, "+
							"but %s received. Just ignore it...", ctldReply.Type.String())
					} else {
						break waitForDeadCrunAck
					}
				}
			}

			log.Infof("[Cfored<->Crun][Step #%d.%d] Job cancelled due to Crun down", jobId, stepId)

			break CforedCrunStateMachineLoop
		}
	}

	return nil

}
