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
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"

	log "github.com/sirupsen/logrus"
)

type GrpcCtldClient struct {
	config           *util.Config
	ctldConn         *grpc.ClientConn
	ctldClientStub   protos.CraneCtldForInternalClient
	ctldReplyChannel chan *protos.StreamCtldReply
}

type StateOfCtldClient int

const (
	StartReg        StateOfCtldClient = 0
	WaitReg         StateOfCtldClient = 1
	WaitChannelReq  StateOfCtldClient = 2
	WaitAllFrontEnd StateOfCtldClient = 3
	GracefulExit    StateOfCtldClient = 4
)

// The channel belongs to this stream. An old receiver cannot publish a late
// reply or EOF into a subsequent registration attempt.
func (client *GrpcCtldClient) CtldReplyReceiveRoutine(ctx context.Context, stream protos.CraneCtldForInternal_CforedStreamClient, replies chan<- *protos.StreamCtldReply) {
	for {
		m, err := stream.Recv()
		if err != nil {
			log.Infof("CtlClientStreamError: %s. Exiting CtldReplyReceiveRoutine...", err)
			m = nil
		}
		select {
		case replies <- m:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func deliverCtldReply(channel chan<- *protos.StreamCtldReply, reply *protos.StreamCtldReply) {
	select {
	case channel <- reply:
	case <-gVars.globalCtx.Done():
	}
}

// Route replies without holding the map lock across channel sends. If Ctld
// cancels an already-removed session, the caller sends its completion directly;
// enqueueing it on our own input channel could deadlock when that queue is full.
func dispatchCtldReply(reply *protos.StreamCtldReply) *protos.StreamCforedRequest {
	var channels []chan *protos.StreamCtldReply
	var completion *protos.StreamCforedRequest
	gVars.ctldReplyChannelMapMtx.Lock()
	switch reply.Type {
	case protos.StreamCtldReply_JOB_ID_REPLY:
		if c := gVars.ctldReplyChannelMapByPid[reply.GetPayloadJobIdReply().Pid]; c != nil {
			channels = append(channels, c)
		}
	case protos.StreamCtldReply_STEP_META_REPLY:
		if c := gVars.ctldReplyChannelMapForCattachByPid[reply.GetPayloadStepMetaReply().CattachPid]; c != nil {
			channels = append(channels, c)
		}
	case protos.StreamCtldReply_JOB_RES_ALLOC_REPLY,
		protos.StreamCtldReply_JOB_CANCEL_REQUEST,
		protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY:
		var step StepIdentifier
		switch reply.Type {
		case protos.StreamCtldReply_JOB_RES_ALLOC_REPLY:
			p := reply.GetPayloadJobResAllocReply()
			step = StepIdentifier{JobId: p.JobId, StepId: p.StepId}
		case protos.StreamCtldReply_JOB_CANCEL_REQUEST:
			p := reply.GetPayloadJobCancelRequest()
			step = StepIdentifier{JobId: p.JobId, StepId: p.StepId}
		case protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY:
			p := reply.GetPayloadJobCompletionAck()
			step = StepIdentifier{JobId: p.JobId, StepId: p.StepId}
		}
		if c := gVars.ctldReplyChannelMapByStep[step]; c != nil {
			channels = append(channels, c)
		} else if reply.Type == protos.StreamCtldReply_JOB_CANCEL_REQUEST {
			completion = &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_JOB_COMPLETION_REQUEST,
				Payload: &protos.StreamCforedRequest_PayloadJobCompleteReq{
					PayloadJobCompleteReq: &protos.StreamCforedRequest_JobCompleteReq{
						CforedName: gVars.hostName, JobId: step.JobId, StepId: step.StepId,
						InteractiveType: protos.InteractiveJobType_Crun,
					},
				},
			}
		}
		if reply.Type == protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY {
			for _, c := range gVars.ctldReplyChannelMapForCattachByStep[step] {
				channels = append(channels, c)
			}
		}
	default:
		log.Warnf("[Cfored<->Ctld] Unexpected reply %s", reply.Type)
	}
	gVars.ctldReplyChannelMapMtx.Unlock()

	for _, c := range channels {
		deliverCtldReply(c, reply)
	}
	return completion
}

func (client *GrpcCtldClient) StartCtldClientStream(wg *sync.WaitGroup) {
	defer wg.Done()
	defer gVars.ctldConnected.Store(false)

	var stream protos.CraneCtldForInternal_CforedStreamClient
	var cancelStream context.CancelFunc
	defer func() {
		if cancelStream != nil {
			cancelStream()
		}
		if client.ctldConn != nil {
			client.ctldConn.Close()
		}
	}()
	// Completion can be retried: Ctld suppresses repeated step termination and
	// acknowledges steps that have already gone. Keep it until the real ACK,
	// including when Send succeeds but the connection loses the reply.
	pendingCompletions := make(map[StepIdentifier]*protos.StreamCforedRequest)
	// Track only requests actually sent on this stream. Requests still in the
	// input queue can be sent once after reconnect; submissions must not be retried.
	pendingJobIDs := make(map[int32]chan *protos.StreamCtldReply)
	pendingMetadata := make(map[int32]chan *protos.StreamCtldReply)
	pendingAllocations := make(map[StepIdentifier]chan *protos.StreamCtldReply)
	sendRequest := func(request *protos.StreamCforedRequest) error {
		switch request.Type {
		case protos.StreamCforedRequest_JOB_REQUEST, protos.StreamCforedRequest_STEP_REQUEST:
			pid := request.GetPayloadJobReq().GetPid()
			if request.Type == protos.StreamCforedRequest_STEP_REQUEST {
				pid = request.GetPayloadStepReq().Pid
			}
			gVars.ctldReplyChannelMapMtx.Lock()
			channel := gVars.ctldReplyChannelMapByPid[pid]
			gVars.ctldReplyChannelMapMtx.Unlock()
			if channel == nil {
				return nil
			}
			pendingJobIDs[pid] = channel
		case protos.StreamCforedRequest_STEP_META_REQUEST:
			pid := request.GetPayloadStepMetaReq().CattachPid
			gVars.ctldReplyChannelMapMtx.Lock()
			channel := gVars.ctldReplyChannelMapForCattachByPid[pid]
			gVars.ctldReplyChannelMapMtx.Unlock()
			if channel == nil {
				return nil
			}
			pendingMetadata[pid] = channel
		}
		if request.Type == protos.StreamCforedRequest_JOB_COMPLETION_REQUEST {
			p := request.GetPayloadJobCompleteReq()
			step := StepIdentifier{JobId: p.JobId, StepId: p.StepId}
			delete(pendingAllocations, step)
			pendingCompletions[step] = request
		}
		// A flow-controlled Send must also unblock on process shutdown.
		stopCancel := context.AfterFunc(gVars.globalCtx, cancelStream)
		defer stopCancel()
		return stream.Send(request)
	}
	state := StartReg
	for {
		switch state {
		case StartReg:
			gVars.ctldConnected.Store(false)
			if gVars.globalCtx.Err() != nil {
				state = WaitAllFrontEnd
				continue
			}
			if cancelStream != nil {
				cancelStream()
				client.ctldConn.Close()
				// Ctld registration does not replay these replies. Fail only the
				// incomplete handshakes, keeping established I/O sessions alive.
				for pid, channel := range pendingJobIDs {
					deliverCtldReply(channel, &protos.StreamCtldReply{
						Type: protos.StreamCtldReply_JOB_ID_REPLY,
						Payload: &protos.StreamCtldReply_PayloadJobIdReply{
							PayloadJobIdReply: &protos.StreamCtldReply_JobIdReply{
								Pid: pid, FailureReason: "Connection to CraneCtld lost before the submission result; the job or step may have been accepted.",
							},
						},
					})
				}
				clear(pendingJobIDs)
				for pid, channel := range pendingMetadata {
					deliverCtldReply(channel, &protos.StreamCtldReply{
						Type: protos.StreamCtldReply_STEP_META_REPLY,
						Payload: &protos.StreamCtldReply_PayloadStepMetaReply{
							PayloadStepMetaReply: &protos.StreamCtldReply_StepMetaReply{
								CattachPid: pid, FailureReason: "Connection to CraneCtld lost before the step metadata reply.",
							},
						},
					})
				}
				clear(pendingMetadata)
				for step, channel := range pendingAllocations {
					// Let the handler follow its normal cancellation/completion path.
					deliverCtldReply(channel, &protos.StreamCtldReply{
						Type: protos.StreamCtldReply_JOB_CANCEL_REQUEST,
						Payload: &protos.StreamCtldReply_PayloadJobCancelRequest{
							PayloadJobCancelRequest: &protos.StreamCtldReply_JobCancelRequest{JobId: step.JobId, StepId: step.StepId},
						},
					})
				}
				clear(pendingAllocations)
				select {
				case <-gVars.globalCtx.Done():
					state = WaitAllFrontEnd
					continue
				case <-time.After(time.Second):
				}
			}
			log.Trace("[Cfored<->Ctld] Enter START_REG state.")
			// Retain the pre-existing connection retry policy: each registration
			// attempt gets a fresh transport and backoff state. Sessions live above it.
			client.ctldConn, client.ctldClientStub = util.GetStubToCtldForInternalByConfig(client.config)
			ctx, cancel := context.WithCancel(context.Background())
			cancelStream = cancel
			// Opening a stream may wait for a transport; shutdown must interrupt it.
			stopCancel := context.AfterFunc(gVars.globalCtx, cancel)
			var err error
			stream, err = client.ctldClientStub.CforedStream(ctx)
			if err == nil {
				err = stream.Send(&protos.StreamCforedRequest{
					Type: protos.StreamCforedRequest_CFORED_REGISTRATION,
					Payload: &protos.StreamCforedRequest_PayloadCforedReg{
						PayloadCforedReg: &protos.StreamCforedRequest_CforedReg{CforedName: gVars.hostName},
					},
				})
			}
			stopCancel()
			if err != nil {
				log.Errorf("[Cfored<->Ctld] Cannot register with CraneCtld: %s. Retrying...", err)
				continue
			}
			client.ctldReplyChannel = make(chan *protos.StreamCtldReply, 8)
			go client.CtldReplyReceiveRoutine(ctx, stream, client.ctldReplyChannel)
			state = WaitReg

		case WaitReg, WaitChannelReq:
			// Keep established sessions and their routes across a transport failure.
			// Requests queued while disconnected are sent only after registration.
			var requests <-chan *protos.StreamCforedRequest
			if state == WaitChannelReq {
				requests = gVars.cforedRequestCtldChannel
			}
			select {
			case <-gVars.globalCtx.Done():
				state = WaitAllFrontEnd
			case request := <-requests:
				if err := sendRequest(request); err != nil {
					log.Errorf("[Cfored<->Ctld] Failed to forward %s: %s. Reconnecting...", request.Type, err)
					state = StartReg
				}
			case reply := <-client.ctldReplyChannel:
				if reply == nil {
					state = StartReg
					continue
				}
				if reply.Type == protos.StreamCtldReply_CFORED_REGISTRATION_ACK {
					if !reply.GetPayloadCforedRegAck().Ok {
						log.Errorf("[Cfored<->Ctld] Registration rejected: %s", reply.GetPayloadCforedRegAck().FailureReason)
						gVars.globalCtxCancel()
						state = WaitAllFrontEnd
					} else {
						log.Infof("[Cfored<->Ctld] Cfored %s successfully registered.", gVars.hostName)
						gVars.ctldConnected.Store(true)
						state = WaitChannelReq
						for _, request := range pendingCompletions {
							if err := sendRequest(request); err != nil {
								log.Errorf("[Cfored<->Ctld] Failed to retry completion: %s", err)
								state = StartReg
								break
							}
						}
					}
					continue
				}
				if reply.Type == protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY {
					p := reply.GetPayloadJobCompletionAck()
					delete(pendingCompletions, StepIdentifier{JobId: p.JobId, StepId: p.StepId})
				}
				// Keep the original channel until allocation is delivered: the
				// handler may not yet have moved its PID route to the step map.
				var waiting chan *protos.StreamCtldReply
				switch reply.Type {
				case protos.StreamCtldReply_JOB_ID_REPLY:
					p := reply.GetPayloadJobIdReply()
					waiting = pendingJobIDs[p.Pid]
					delete(pendingJobIDs, p.Pid)
					if p.Ok && waiting != nil {
						pendingAllocations[StepIdentifier{JobId: p.JobId, StepId: p.StepId}] = waiting
					}
				case protos.StreamCtldReply_STEP_META_REPLY:
					p := reply.GetPayloadStepMetaReply()
					waiting = pendingMetadata[p.CattachPid]
					delete(pendingMetadata, p.CattachPid)
				case protos.StreamCtldReply_JOB_RES_ALLOC_REPLY, protos.StreamCtldReply_JOB_CANCEL_REQUEST, protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY:
					var step StepIdentifier
					switch reply.Type {
					case protos.StreamCtldReply_JOB_RES_ALLOC_REPLY:
						p := reply.GetPayloadJobResAllocReply()
						step = StepIdentifier{JobId: p.JobId, StepId: p.StepId}
					case protos.StreamCtldReply_JOB_CANCEL_REQUEST:
						p := reply.GetPayloadJobCancelRequest()
						step = StepIdentifier{JobId: p.JobId, StepId: p.StepId}
					case protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY:
						p := reply.GetPayloadJobCompletionAck()
						step = StepIdentifier{JobId: p.JobId, StepId: p.StepId}
					}
					waiting = pendingAllocations[step]
					delete(pendingAllocations, step)
				}
				if waiting != nil {
					deliverCtldReply(waiting, reply)
					continue
				}
				// Ctld replaces its stream writer before sending registration ACK;
				// an existing session's callback may therefore arrive first.
				if request := dispatchCtldReply(reply); request != nil {
					if err := sendRequest(request); err != nil {
						log.Errorf("[Cfored<->Ctld] Failed to complete removed session: %s", err)
						state = StartReg
					}
				}
			}

		case WaitAllFrontEnd:
			log.Trace("[Cfored<->Ctld] Enter WAIT_ALL_FRONT_END state.")
			gVars.ctldConnected.Store(false)
			// Only process shutdown reaches this state. Handlers observe globalCtx,
			// release their own maps/I/O channels, and then decrement the counter.
			waitFrontEnds()
			state = GracefulExit

		case GracefulExit:
			if stream == nil || stream.Context().Err() != nil {
				return
			}
			request := &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_CFORED_GRACEFUL_EXIT,
				Payload: &protos.StreamCforedRequest_PayloadGracefulExitReq{
					PayloadGracefulExitReq: &protos.StreamCforedRequest_GracefulExitReq{CforedName: gVars.hostName},
				},
			}
			if err := stream.Send(request); err != nil {
				log.Errorf("[Cfored<->Ctld] Failed to notify CraneCtld of shutdown: %s", err)
				return
			}
			for reply := range client.ctldReplyChannel {
				if reply == nil || reply.Type == protos.StreamCtldReply_CFORED_GRACEFUL_EXIT_ACK {
					return
				}
				// Other session replies may already be queued ahead of the exit ACK.
			}
		}
	}
}
