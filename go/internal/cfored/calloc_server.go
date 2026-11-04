package cfored

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"io"
	"math"

	"google.golang.org/grpc/peer"

	log "github.com/sirupsen/logrus"
)

type StateOfCallocServer int

const (
	WaitJobIdAllocReq     StateOfCallocServer = 0
	WaitCtldAllocJobId    StateOfCallocServer = 1
	WaitCtldAllocRes      StateOfCallocServer = 2
	WaitCallocComplete    StateOfCallocServer = 3
	WaitCallocCancel      StateOfCallocServer = 4
	WaitCtldAck           StateOfCallocServer = 5
	CancelJobOfDeadCalloc StateOfCallocServer = 6
)

func (cforedServer *GrpcCforedServer) CallocStream(toCallocStream protos.CraneForeD_CallocStreamServer) error {
	if !startFrontEnd() {
		return nil
	}
	defer gVars.frontEndWG.Done()

	var callocPid int32
	var jobId uint32
	var stepId uint32
	var reply *protos.StreamCallocReply
	var completionAck *protos.StreamCtldReply
	var step StepIdentifier
	callocDownWithoutJobId := false

	requestChannel := make(chan grpcMessage[protos.StreamCallocRequest], 8)
	receiverDone := make(chan struct{})
	defer close(receiverDone)
	go grpcStreamReceiver[protos.StreamCallocRequest](toCallocStream, requestChannel, receiverDone)

	ctldReplyChannel := make(chan *protos.StreamCtldReply, 2)

	jobId = math.MaxUint32
	stepId = math.MaxUint32
	callocPid = -1

	defer func() {
		gVars.ctldReplyChannelMapMtx.Lock()
		if gVars.ctldReplyChannelMapByPid[callocPid] == ctldReplyChannel {
			delete(gVars.ctldReplyChannelMapByPid, callocPid)
		}
		ownsStep := gVars.ctldReplyChannelMapByStep[step] == ctldReplyChannel
		if ownsStep {
			delete(gVars.ctldReplyChannelMapByStep, step)
			gVars.pidStepMapMtx.Lock()
			delete(gVars.pidStepMap, callocPid)
			gVars.pidStepMapMtx.Unlock()
		}
		gVars.ctldReplyChannelMapMtx.Unlock()
	}()

	state := WaitJobIdAllocReq

CforedStateMachineLoop:
	for {
		switch state {
		case WaitJobIdAllocReq:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_JOB_ID_ALLOC_REQ")

			var item grpcMessage[protos.StreamCallocRequest]
			select {
			case item = <-requestChannel:
			case <-gVars.globalCtx.Done():
				return nil
			}
			callocRequest, err := item.message, item.err
			if err != nil {
				return err
			}

			log.Debug("[Cfored<->Calloc] Receive JobIdAllocReq")

			if callocRequest.Type != protos.StreamCallocRequest_JOB_REQUEST {
				log.Fatal("[Cfored<->Calloc] Expect JOB_REQUEST")
			}

			ctx := toCallocStream.Context()
			p, ok := peer.FromContext(ctx)
			if ok {
				if auth, ok := p.AuthInfo.(*util.UnixPeerAuthInfo); ok {
					uid := callocRequest.GetPayloadJobReq().Job.Uid
					if uid != auth.UID {
						log.Warnf("Security: UID mismatch - peer UID %d does not match job UID %d", auth.UID, callocRequest.GetPayloadJobReq().Job.Uid)
						reply = &protos.StreamCallocReply{
							Type: protos.StreamCallocReply_JOB_ID_REPLY,
							Payload: &protos.StreamCallocReply_PayloadJobIdReply{
								PayloadJobIdReply: &protos.StreamCallocReply_JobIdReply{
									Ok:            false,
									FailureReason: "Permission denied: caller UID does not match job UID",
								},
							},
						}

						if err := toCallocStream.Send(reply); err != nil {
							log.Error(err)
						}

						break CforedStateMachineLoop
					}
				}
			}

			if !gVars.ctldConnected.Load() {
				reply = &protos.StreamCallocReply{
					Type: protos.StreamCallocReply_JOB_ID_REPLY,
					Payload: &protos.StreamCallocReply_PayloadJobIdReply{
						PayloadJobIdReply: &protos.StreamCallocReply_JobIdReply{
							Ok:            false,
							FailureReason: "Cfored is not connected to CraneCtld.",
						},
					},
				}

				if err := toCallocStream.Send(reply); err != nil {
					// It doesn't matter even if the connection is broken here.
					// Just print a log.
					log.Error(err)
				}

				// No need to cleaning any data
				break CforedStateMachineLoop
			} else {
				callocPid = callocRequest.GetPayloadJobReq().CallocPid

				gVars.ctldReplyChannelMapMtx.Lock()
				gVars.ctldReplyChannelMapByPid[callocPid] = ctldReplyChannel
				gVars.ctldReplyChannelMapMtx.Unlock()

				job := callocRequest.GetPayloadJobReq().Job
				job.GetInteractiveMeta().CforedName = gVars.hostName
				cforedRequest := &protos.StreamCforedRequest{
					Type: protos.StreamCforedRequest_JOB_REQUEST,
					Payload: &protos.StreamCforedRequest_PayloadJobReq{
						PayloadJobReq: &protos.StreamCforedRequest_JobReq{
							CforedName: gVars.hostName,
							Pid:        callocPid,
							Job:        job,
						},
					},
				}

				if !sendCtldRequest(cforedRequest) {
					return nil
				}

				state = WaitCtldAllocJobId
			}

		case WaitCtldAllocJobId:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CTLD_ALLOC_JOB_ID")

			select {
			case <-gVars.globalCtx.Done():
				return nil
			case item := <-requestChannel:
				callocRequest, err := item.message, item.err
				if callocRequest != nil || err == nil {
					log.Fatal("[Cfored<->Calloc] Expect only nil (calloc connection broken) here!")
				}
				log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")

				// Wait for the ID before cancelling an accepted submission.
				callocDownWithoutJobId = true

			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_JOB_ID_REPLY {
					log.Fatal("[Cfored<->Calloc] Expect type JOB_ID_REPLY")
				}

				Ok := ctldReply.GetPayloadJobIdReply().Ok
				jobId = ctldReply.GetPayloadJobIdReply().JobId
				stepId = ctldReply.GetPayloadJobIdReply().StepId
				log.Tracef("[Cfored<->Calloc][Pid #%d] JOB_ID_REPLY of received, Ok:%v, JobId:#%d, StepId:#%d",
					callocPid, Ok, jobId, stepId)

				reply = &protos.StreamCallocReply{
					Type: protos.StreamCallocReply_JOB_ID_REPLY,
					Payload: &protos.StreamCallocReply_PayloadJobIdReply{
						PayloadJobIdReply: &protos.StreamCallocReply_JobIdReply{
							Ok:            Ok,
							JobId:         jobId,
							StepId:        stepId,
							FailureReason: ctldReply.GetPayloadJobIdReply().FailureReason,
						},
					},
				}

				gVars.ctldReplyChannelMapMtx.Lock()
				delete(gVars.ctldReplyChannelMapByPid, callocPid)
				if Ok {
					step = StepIdentifier{JobId: jobId, StepId: stepId}
					gVars.ctldReplyChannelMapByStep[step] = ctldReplyChannel

					gVars.pidStepMapMtx.Lock()
					gVars.pidStepMap[callocPid] = step
					gVars.pidStepMapMtx.Unlock()
				}
				gVars.ctldReplyChannelMapMtx.Unlock()

				if callocDownWithoutJobId {
					if !Ok {
						return nil
					}
					state = CancelJobOfDeadCalloc
					break
				}

				if err := toCallocStream.Send(reply); err != nil {
					log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
					state = CancelJobOfDeadCalloc
				} else {
					state = WaitCtldAllocRes
				}
				if !Ok {
					break CforedStateMachineLoop
				}
			}

		case WaitCtldAllocRes:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CTLD_ALLOC_RES")

			select {
			case <-gVars.globalCtx.Done():
				return nil
			case item := <-requestChannel:
				callocRequest, err := item.message, item.err
				if callocRequest != nil || err == nil {
					log.Fatal("[Cfored<->Calloc] Expect only nil (calloc connection broken) here!")
				}
				log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")

				state = CancelJobOfDeadCalloc

			case ctldReply := <-ctldReplyChannel:
				switch ctldReply.Type {
				case protos.StreamCtldReply_JOB_RES_ALLOC_REPLY:
					ctldPayload := ctldReply.GetPayloadJobResAllocReply()
					reply = &protos.StreamCallocReply{
						Type: protos.StreamCallocReply_JOB_RES_ALLOC_REPLY,
						Payload: &protos.StreamCallocReply_PayloadJobAllocReply{
							PayloadJobAllocReply: &protos.StreamCallocReply_JobResAllocatedReply{
								Ok:                   ctldPayload.Ok,
								AllocatedCranedRegex: ctldPayload.AllocatedCranedRegex,
							},
						},
					}

					if err := toCallocStream.Send(reply); err != nil {
						log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
						state = CancelJobOfDeadCalloc
					} else {
						state = WaitCallocComplete
					}

				case protos.StreamCtldReply_JOB_CANCEL_REQUEST:
					state = WaitCallocCancel

				default:
					log.Fatal("[Cfored<->Calloc] Expect type " +
						"JOB_ID_ALLOC_REPLY or JOB_CANCEL_REQUEST")
				}
			}

		case WaitCallocComplete:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CALLOC_COMPLETE")

			select {
			case <-gVars.globalCtx.Done():
				return nil
			case ctldReply := <-ctldReplyChannel:
				log.Tracef("[Cfored<->Calloc] Receive %s from CraneCtld", ctldReply.Type.String())
				switch ctldReply.Type {
				case protos.StreamCtldReply_JOB_CANCEL_REQUEST:
					state = WaitCallocCancel

				case protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY:
					completionAck = ctldReply
					state = WaitCtldAck
				}

			case item := <-requestChannel:
				callocRequest, err := item.message, item.err
				if err != nil {
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
						state = CancelJobOfDeadCalloc
					}
				} else {
					if callocRequest.Type != protos.StreamCallocRequest_JOB_COMPLETION_REQUEST {
						log.Fatal("[Cfored<->Calloc] Expect JOB_COMPLETION_REQUEST")
					}

					log.Debug("[Cfored<->Calloc] Receive JobCompletionRequest")
					toCtldRequest := &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_JOB_COMPLETION_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadJobCompleteReq{
							PayloadJobCompleteReq: &protos.StreamCforedRequest_JobCompleteReq{
								CforedName:      gVars.hostName,
								JobId:           jobId,
								StepId:          stepId,
								InteractiveType: protos.InteractiveJobType_Calloc,
							},
						},
					}
					if !sendCtldRequest(toCtldRequest) {
						return nil
					}

					state = WaitCtldAck
				}
			}

		case WaitCallocCancel:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CALLOC_CANCEL. " +
				"Sending JOB_CANCEL_REQUEST...")

			reply = &protos.StreamCallocReply{
				Type: protos.StreamCallocReply_JOB_CANCEL_REQUEST,
				Payload: &protos.StreamCallocReply_PayloadJobCancelRequest{
					PayloadJobCancelRequest: &protos.StreamCallocReply_JobCancelRequest{},
				},
			}

			if err := toCallocStream.Send(reply); err != nil {
				log.Debugf("[Cfored<->Calloc] Failed to send CancelRequest to calloc: %s. "+
					"The connection to calloc was broken.", err.Error())
				state = CancelJobOfDeadCalloc
			} else {
				var item grpcMessage[protos.StreamCallocRequest]
				select {
				case item = <-requestChannel:
				case <-gVars.globalCtx.Done():
					return nil
				}
				callocRequest, err := item.message, item.err
				if err != nil { // Failure Edge
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
						state = CancelJobOfDeadCalloc
					}
				} else {
					if callocRequest.Type != protos.StreamCallocRequest_JOB_COMPLETION_REQUEST {
						log.Fatal("[Cfored<->Calloc] Expect JOB_COMPLETION_REQUEST")
					}

					log.Debug("[Cfored<->Calloc] Receive JobCompletionRequest")

					toCtldRequest := &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_JOB_COMPLETION_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadJobCompleteReq{
							PayloadJobCompleteReq: &protos.StreamCforedRequest_JobCompleteReq{
								CforedName:      gVars.hostName,
								JobId:           jobId,
								StepId:          stepId,
								InteractiveType: protos.InteractiveJobType_Calloc,
							},
						},
					}
					if !sendCtldRequest(toCtldRequest) {
						return nil
					}

					state = WaitCtldAck
				}
			}

		case WaitCtldAck:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CTLD_ACK")

			if completionAck == nil {
				select {
				case completionAck = <-ctldReplyChannel:
				case <-gVars.globalCtx.Done():
					return nil
				}
			}
			if completionAck.Type != protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY {
				log.Warningf("[Cfored<->Calloc] Expect JOB_COMPLETION_ACK_REPLY, "+
					"but %s received.", completionAck.Type)
				completionAck = nil
				continue
			} else {
				log.Tracef("[Cfored<->Calloc] JOB_COMPLETION_ACK_REPLY of job #%d received",
					jobId)
			}

			reply = &protos.StreamCallocReply{
				Type: protos.StreamCallocReply_JOB_COMPLETION_ACK_REPLY,
				Payload: &protos.StreamCallocReply_PayloadJobCompletionAckReply{
					PayloadJobCompletionAckReply: &protos.StreamCallocReply_JobCompletionAckReply{
						Ok: true,
					},
				},
			}

			if err := toCallocStream.Send(reply); err != nil {
				log.Errorf("[Cfored<->Calloc] The stream to calloc executing "+
					"job #%d is broken", jobId)
			}

			break CforedStateMachineLoop

		case CancelJobOfDeadCalloc:
			log.Debug("[Cfored<->Calloc] Enter State CANCEL_JOB_OF_DEAD_CALLOC")

			toCtldRequest := &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_JOB_COMPLETION_REQUEST,
				Payload: &protos.StreamCforedRequest_PayloadJobCompleteReq{
					PayloadJobCompleteReq: &protos.StreamCforedRequest_JobCompleteReq{
						CforedName:      gVars.hostName,
						JobId:           jobId,
						StepId:          stepId,
						InteractiveType: protos.InteractiveJobType_Calloc,
					},
				},
			}
			if !sendCtldRequest(toCtldRequest) {
				return nil
			}

			for {
				var ctldReply *protos.StreamCtldReply
				select {
				case ctldReply = <-ctldReplyChannel:
				case <-gVars.globalCtx.Done():
					return nil
				}
				if ctldReply.Type != protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY {
					log.Tracef("[Cfored<->Calloc] Expect JOB_COMPLETION_ACK_REPLY, "+
						"but %s received. Just ignore it...", ctldReply.Type.String())
				} else {
					break
				}
			}

			break CforedStateMachineLoop
		}
	}

	return nil
}
