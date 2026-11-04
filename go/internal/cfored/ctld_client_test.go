package cfored

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type testCtldStream struct {
	requests   chan *protos.StreamCforedRequest
	replies    chan *protos.StreamCtldReply
	disconnect chan struct{}
}

type testCtld struct {
	protos.UnimplementedCraneCtldForInternalServer
	streams chan *testCtldStream
	exits   chan struct{}
}

func (s *testCtld) CforedStream(stream protos.CraneCtldForInternal_CforedStreamServer) error {
	c := &testCtldStream{
		requests: make(chan *protos.StreamCforedRequest, 32),
		replies:  make(chan *protos.StreamCtldReply, 32), disconnect: make(chan struct{}),
	}
	s.streams <- c
	requests := make(chan *protos.StreamCforedRequest)
	go func() {
		defer close(requests)
		for {
			r, err := stream.Recv()
			if err != nil {
				return
			}
			select {
			case requests <- r:
			case <-stream.Context().Done():
				return
			}
		}
	}()
	for {
		select {
		case <-c.disconnect:
			return status.Error(codes.Unavailable, "test disconnect")
		case <-stream.Context().Done():
			return stream.Context().Err()
		case reply := <-c.replies:
			if err := stream.Send(reply); err != nil {
				return err
			}
		case r, ok := <-requests:
			if !ok {
				return nil
			}
			if r.Type == protos.StreamCforedRequest_CFORED_GRACEFUL_EXIT {
				s.exits <- struct{}{}
				return stream.Send(&protos.StreamCtldReply{
					Type: protos.StreamCtldReply_CFORED_GRACEFUL_EXIT_ACK,
					Payload: &protos.StreamCtldReply_PayloadGracefulExitAck{
						PayloadGracefulExitAck: &protos.StreamCtldReply_CforedGracefulExitAck{Ok: true},
					},
				})
			}
			c.requests <- r
		}
	}
}

func receiveTest[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
		var zero T
		return zero
	}
}

func eventuallyTest(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met")
}

func initCforedTest(t *testing.T) {
	t.Helper()
	gVars = GlobalVariables{}
	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())
	gVars.hostName = "cfored-test"
	gVars.cforedRequestCtldChannel = make(chan *protos.StreamCforedRequest, 8)
	gVars.ctldReplyChannelMapByPid = make(map[int32]chan *protos.StreamCtldReply)
	gVars.ctldReplyChannelMapForCattachByPid = make(map[int32]chan *protos.StreamCtldReply)
	gVars.ctldReplyChannelMapByStep = make(map[StepIdentifier]chan *protos.StreamCtldReply)
	gVars.ctldReplyChannelMapForCattachByStep = make(map[StepIdentifier]map[int32]chan *protos.StreamCtldReply)
	gVars.pidStepMap = make(map[int32]StepIdentifier)
	gSupervisorChanKeeper = NewCranedChannelKeeper()
	t.Cleanup(gVars.globalCtxCancel)
}

type cforedTest struct {
	ctld  *testCtld
	front protos.CraneForeDClient
	done  chan struct{}
}

func startCforedTest(t *testing.T) *cforedTest {
	t.Helper()
	initCforedTest(t)
	ctldListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctld := &testCtld{streams: make(chan *testCtldStream, 8), exits: make(chan struct{}, 1)}
	ctldServer := grpc.NewServer()
	protos.RegisterCraneCtldForInternalServer(ctldServer, ctld)
	go ctldServer.Serve(ctldListener)
	_, port, _ := net.SplitHostPort(ctldListener.Addr().String())
	client := &GrpcCtldClient{config: &util.Config{
		ControlMachine: "127.0.0.1", CraneCtldForInternalListenPort: port,
	}}
	var wg sync.WaitGroup
	wg.Add(1)
	go client.StartCtldClientStream(&wg)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	frontListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	frontServer := grpc.NewServer()
	protos.RegisterCraneForeDServer(frontServer, &GrpcCforedServer{})
	go frontServer.Serve(frontListener)
	conn, err := grpc.NewClient(frontListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	// Match production: shutdown closes inbound transports as well as waking
	// state-machine selects, so blocked network writes cannot prevent cleanup.
	go func(ctx context.Context) { <-ctx.Done(); frontServer.Stop() }(gVars.globalCtx)
	t.Cleanup(func() {
		gVars.globalCtxCancel()
		frontServer.Stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Cfored client did not exit")
		}
		conn.Close()
		ctldServer.Stop()
	})
	return &cforedTest{ctld: ctld, front: protos.NewCraneForeDClient(conn), done: done}
}

func registerTest(t *testing.T, ctld *testCtld) *testCtldStream {
	t.Helper()
	c := receiveTest(t, ctld.streams)
	if r := receiveTest(t, c.requests); r.Type != protos.StreamCforedRequest_CFORED_REGISTRATION {
		t.Fatalf("expected registration, got %v", r.Type)
	}
	c.replies <- registrationAckTest()
	eventuallyTest(t, gVars.ctldConnected.Load)
	return c
}

type registrationFailureHook chan string

func (registrationFailureHook) Levels() []log.Level { return []log.Level{log.ErrorLevel} }

func (h registrationFailureHook) Fire(entry *log.Entry) error {
	if strings.Contains(entry.Message, "Cannot register with CraneCtld:") {
		select {
		case h <- entry.Message:
		default:
		}
	}
	return nil
}

func TestCtldRecoversAfterListenerUnavailable(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "delayed_start"
		if restart {
			name = "restart_same_port"
		}
		t.Run(name, func(t *testing.T) {
			initCforedTest(t)
			failures := make(registrationFailureHook, 8)
			oldHooks := log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
			log.AddHook(failures)
			t.Cleanup(func() { log.StandardLogger().ReplaceHooks(oldHooks) })

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			_, port, _ := net.SplitHostPort(address)
			ctld := &testCtld{streams: make(chan *testCtldStream, 8), exits: make(chan struct{}, 1)}
			server := grpc.NewServer()
			protos.RegisterCraneCtldForInternalServer(server, ctld)
			t.Cleanup(func() { server.Stop() })
			if restart {
				go server.Serve(listener)
			} else if err := listener.Close(); err != nil {
				t.Fatal(err)
			}

			client := &GrpcCtldClient{config: &util.Config{
				ControlMachine: "127.0.0.1", CraneCtldForInternalListenPort: port,
			}}
			var wg sync.WaitGroup
			wg.Add(1)
			go client.StartCtldClientStream(&wg)
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			t.Cleanup(func() {
				gVars.globalCtxCancel()
				server.Stop()
				receiveTest(t, done)
			})
			if restart {
				registerTest(t, ctld)
				server.Stop() // Close the transport and listener, not just the RPC.
			}

			// Keep the port closed across retries. With the current connection
			// parameters, a reused ClientConn exhausts its nonzero dial budget
			// after the first failed attempt and cannot recover when Ctld returns.
			for range 3 {
				t.Log(receiveTest(t, failures))
			}
			listener, err = net.Listen("tcp", address)
			if err != nil {
				t.Fatal(err)
			}
			server = grpc.NewServer()
			protos.RegisterCraneCtldForInternalServer(server, ctld)
			go server.Serve(listener)
			registerTest(t, ctld)
			gVars.globalCtxCancel()
			receiveTest(t, ctld.exits)
			receiveTest(t, done)
		})
	}
}

func registrationAckTest() *protos.StreamCtldReply {
	return &protos.StreamCtldReply{
		Type: protos.StreamCtldReply_CFORED_REGISTRATION_ACK,
		Payload: &protos.StreamCtldReply_PayloadCforedRegAck{
			PayloadCforedRegAck: &protos.StreamCtldReply_CforedRegistrationAck{Ok: true},
		},
	}
}

func completionAckTest(jobID uint32) *protos.StreamCtldReply {
	return &protos.StreamCtldReply{
		Type: protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY,
		Payload: &protos.StreamCtldReply_PayloadJobCompletionAck{
			PayloadJobCompletionAck: &protos.StreamCtldReply_JobCompletionAckReply{JobId: jobID, StepId: 1},
		},
	}
}

func jobIDReplyTest(pid int32) *protos.StreamCtldReply {
	return &protos.StreamCtldReply{
		Type: protos.StreamCtldReply_JOB_ID_REPLY,
		Payload: &protos.StreamCtldReply_PayloadJobIdReply{
			PayloadJobIdReply: &protos.StreamCtldReply_JobIdReply{Ok: true, Pid: pid, JobId: uint32(pid), StepId: 1},
		},
	}
}

func crunRequestTest(pid int32) *protos.StreamCrunRequest {
	return &protos.StreamCrunRequest{
		Type: protos.StreamCrunRequest_JOB_REQUEST,
		Payload: &protos.StreamCrunRequest_PayloadJobReq{
			PayloadJobReq: &protos.StreamCrunRequest_JobReq{CrunPid: pid, Job: &protos.JobToCtld{
				Payload: &protos.JobToCtld_InteractiveMeta{InteractiveMeta: &protos.InteractiveJobAdditionalMeta{}},
			}},
		},
	}
}

func startCrunTest(t *testing.T, f *cforedTest, c *testCtldStream, pid int32) (protos.CraneForeD_CrunStreamClient, context.CancelFunc) {
	t.Helper()
	stream, cancel := submitCrunTest(t, f, c, crunRequestTest(pid))
	c.replies <- jobIDReplyTest(pid)
	r, err := stream.Recv()
	if err != nil || !r.GetPayloadStepIdReply().GetOk() {
		t.Fatalf("job ID reply: %v, %v", r, err)
	}
	return stream, cancel
}

func submitCrunTest(t *testing.T, f *cforedTest, c *testCtldStream, request *protos.StreamCrunRequest) (protos.CraneForeD_CrunStreamClient, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	stream, err := f.front.CrunStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	want := protos.StreamCforedRequest_JOB_REQUEST
	pid := request.GetPayloadJobReq().GetCrunPid()
	if request.Type == protos.StreamCrunRequest_STEP_REQUEST {
		want = protos.StreamCforedRequest_STEP_REQUEST
		pid = request.GetPayloadStepReq().CrunPid
	}
	r := receiveTest(t, c.requests)
	gotPid := r.GetPayloadJobReq().GetPid()
	if r.Type == protos.StreamCforedRequest_STEP_REQUEST {
		gotPid = r.GetPayloadStepReq().Pid
	}
	if r.Type != want || gotPid != pid {
		t.Fatalf("expected %s for PID %d, got %v", want, pid, r)
	}
	return stream, cancel
}

func submitCallocTest(t *testing.T, f *cforedTest, c *testCtldStream, pid int32) (protos.CraneForeD_CallocStreamClient, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	stream, err := f.front.CallocStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&protos.StreamCallocRequest{
		Type: protos.StreamCallocRequest_JOB_REQUEST,
		Payload: &protos.StreamCallocRequest_PayloadJobReq{PayloadJobReq: &protos.StreamCallocRequest_JobReq{
			CallocPid: pid, Job: crunRequestTest(pid).GetPayloadJobReq().Job,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if r := receiveTest(t, c.requests); r.Type != protos.StreamCforedRequest_JOB_REQUEST {
		t.Fatalf("expected job request, got %v", r.Type)
	}
	return stream, cancel
}

func allocateCrunTest(t *testing.T, c *testCtldStream, stream protos.CraneForeD_CrunStreamClient, jobID uint32) {
	t.Helper()
	c.replies <- &protos.StreamCtldReply{
		Type: protos.StreamCtldReply_JOB_RES_ALLOC_REPLY,
		Payload: &protos.StreamCtldReply_PayloadJobResAllocReply{
			PayloadJobResAllocReply: &protos.StreamCtldReply_JobResAllocatedReply{
				JobId: jobID, StepId: 1, Ok: true, CranedIds: []string{"node1"},
			},
		},
	}
	if r, err := stream.Recv(); err != nil || r.Type != protos.StreamCrunReply_STEP_RES_ALLOC_REPLY {
		t.Fatalf("resource reply: %v, %v", r, err)
	}
}

func assertNoSessionsTest(t *testing.T) {
	t.Helper()
	gVars.ctldReplyChannelMapMtx.Lock()
	defer gVars.ctldReplyChannelMapMtx.Unlock()
	if len(gVars.ctldReplyChannelMapByPid)+len(gVars.ctldReplyChannelMapByStep)+
		len(gVars.ctldReplyChannelMapForCattachByPid)+len(gVars.ctldReplyChannelMapForCattachByStep) != 0 {
		t.Fatal("session routing entries remain after shutdown")
	}
	gVars.pidStepMapMtx.RLock()
	defer gVars.pidStepMapMtx.RUnlock()
	if len(gVars.pidStepMap) != 0 {
		t.Fatal("PID entries remain after shutdown")
	}
	gSupervisorChanKeeper.stepIORequestChannelMtx.Lock()
	defer gSupervisorChanKeeper.stepIORequestChannelMtx.Unlock()
	if len(gSupervisorChanKeeper.stepIORequestChannelMap) != 0 {
		t.Fatal("I/O channels remain after shutdown")
	}
}

func TestCtldReconnectPreservesSessionsAwaitingCompletion(t *testing.T) {
	f := startCforedTest(t)
	c := registerTest(t, f.ctld)
	_, deadCancel := startCrunTest(t, f, c, 1001)
	active, _ := startCrunTest(t, f, c, 1002)
	allocateCrunTest(t, c, active, 1002)
	valid := &atomic.Bool{}
	valid.Store(true)
	gSupervisorChanKeeper.supervisorUpAndSetMsgToSupervisorChannel(1002, 1, "node1", nil, nil, valid, false)
	if r, err := active.Recv(); err != nil || r.Type != protos.StreamCrunReply_TASK_IO_FORWARD_READY {
		t.Fatalf("I/O ready reply: %v, %v", r, err)
	}
	for pid := int32(1003); pid <= 1005; pid++ {
		stream, _ := startCrunTest(t, f, c, pid)
		allocateCrunTest(t, c, stream, uint32(pid))
	}
	deadCancel()
	if r := receiveTest(t, c.requests); r.Type != protos.StreamCforedRequest_JOB_COMPLETION_REQUEST {
		t.Fatalf("expected dead crun completion, got %v", r.Type)
	}
	// Reproduce the original window: completion was consumed before the
	// stream broke, but its ACK has not yet reached the session.
	close(c.disconnect)
	eventuallyTest(t, func() bool { return !gVars.ctldConnected.Load() })
	c = receiveTest(t, f.ctld.streams)
	receiveTest(t, c.requests) // registration
	gVars.ctldReplyChannelMapMtx.Lock()
	if len(gVars.ctldReplyChannelMapByStep) != 5 {
		t.Error("disconnect removed existing sessions")
	}
	for _, ch := range gVars.ctldReplyChannelMapByStep {
		if len(ch) != 0 {
			t.Error("disconnect injected cancellation or fake ACK")
		}
	}
	gVars.ctldReplyChannelMapMtx.Unlock()
	// Existing supervisor output still reaches crun while Ctld registration
	// is pending; reconnect must not tear down the I/O routing either.
	gSupervisorChanKeeper.forwardRemoteIoToFront(1002, 1, &protos.StreamStepIORequest{
		Type: protos.StreamStepIORequest_TASK_OUTPUT,
		Payload: &protos.StreamStepIORequest_PayloadTaskOutputReq{
			PayloadTaskOutputReq: &protos.StreamStepIORequest_TaskOutputReq{Msg: []byte("still running")},
		},
	})
	if r, err := active.Recv(); err != nil || string(r.GetPayloadTaskIoForwardReply().GetMsg()) != "still running" {
		t.Fatalf("output during reconnect: %v, %v", r, err)
	}
	// Ctld can send an existing callback before registration ACK because it
	// installs the replacement writer first. The dead session must complete.
	c.replies <- completionAckTest(1001)
	c.replies <- registrationAckTest()
	eventuallyTest(t, func() bool {
		gVars.ctldReplyChannelMapMtx.Lock()
		defer gVars.ctldReplyChannelMapMtx.Unlock()
		return gVars.ctldConnected.Load() && len(gVars.ctldReplyChannelMapByStep) == 4
	})
	startCrunTest(t, f, c, 1006)
	gVars.globalCtxCancel()
	receiveTest(t, f.ctld.exits)
	receiveTest(t, f.done)
	assertNoSessionsTest(t)
}

func TestCtldReconnectRetriesUnacknowledgedCompletion(t *testing.T) {
	f := startCforedTest(t)
	c := registerTest(t, f.ctld)
	_, cancel := startCrunTest(t, f, c, 4001)
	cancel()
	original := receiveTest(t, c.requests)
	if original.Type != protos.StreamCforedRequest_JOB_COMPLETION_REQUEST {
		t.Fatalf("expected completion, got %v", original.Type)
	}
	// Lose the ACK twice. Each new registration must retry the same completion,
	// without requiring the exited crun to send another request.
	for range 2 {
		close(c.disconnect)
		c = registerTest(t, f.ctld)
		if retry := receiveTest(t, c.requests); !proto.Equal(original, retry) {
			t.Fatalf("completion changed on retry: %v", retry)
		}
	}
	c.replies <- completionAckTest(4001)
	eventuallyTest(t, func() bool {
		gVars.ctldReplyChannelMapMtx.Lock()
		defer gVars.ctldReplyChannelMapMtx.Unlock()
		return len(gVars.ctldReplyChannelMapByStep) == 0
	})
	close(c.disconnect)
	c = registerTest(t, f.ctld)
	// The next request must be this new submission, not another retry of an
	// acknowledged completion. Old stream receivers must not disrupt it.
	startCrunTest(t, f, c, 4002)
	gVars.globalCtxCancel()
	receiveTest(t, f.done)
	assertNoSessionsTest(t)
}

func TestCtldDisconnectFailsPendingReplies(t *testing.T) {
	for _, disconnected := range []bool{false, true} {
		name := "connected_frontends"
		if disconnected {
			name = "disconnected_frontends"
		}
		t.Run(name, func(t *testing.T) {
			f := startCforedTest(t)
			c := registerTest(t, f.ctld)
			crun, cancelCrun := submitCrunTest(t, f, c, crunRequestTest(5001))
			step, cancelStep := submitCrunTest(t, f, c, &protos.StreamCrunRequest{
				Type: protos.StreamCrunRequest_STEP_REQUEST,
				Payload: &protos.StreamCrunRequest_PayloadStepReq{PayloadStepReq: &protos.StreamCrunRequest_StepReq{
					CrunPid: 5002, Step: &protos.StepToCtld{JobId: 5001,
						Payload: &protos.StepToCtld_InteractiveMeta{InteractiveMeta: &protos.InteractiveJobAdditionalMeta{}}},
				}},
			})
			calloc, cancelCalloc := submitCallocTest(t, f, c, 5003)
			ctx, cancelAttach := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancelAttach()
			attach, err := f.front.CattachStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := attach.Send(&protos.StreamCattachRequest{
				Type: protos.StreamCattachRequest_STEP_CONNECT_REQUEST,
				Payload: &protos.StreamCattachRequest_PayloadStepConnectReq{
					PayloadStepConnectReq: &protos.StreamCattachRequest_StepConnectReq{CattachPid: 5004, JobId: 5001, StepId: 1},
				},
			}); err != nil {
				t.Fatal(err)
			}
			if r := receiveTest(t, c.requests); r.Type != protos.StreamCforedRequest_STEP_META_REQUEST {
				t.Fatalf("expected metadata request, got %v", r.Type)
			}
			if disconnected {
				cancelCrun()
				cancelStep()
				cancelCalloc()
				cancelAttach()
			}
			// Ctld consumed the requests, but every one-shot reply is lost.
			close(c.disconnect)
			if !disconnected {
				for _, stream := range []protos.CraneForeD_CrunStreamClient{crun, step} {
					r, err := stream.Recv()
					if err != nil || r.Type != protos.StreamCrunReply_STEP_ID_REPLY || r.GetPayloadStepIdReply().Ok || r.GetPayloadStepIdReply().FailureReason == "" {
						t.Fatalf("expected submission failure, got %v, %v", r, err)
					}
					if _, err := stream.Recv(); err != io.EOF {
						t.Fatalf("failed submission did not exit: %v", err)
					}
				}
				if r, err := calloc.Recv(); err != nil || r.Type != protos.StreamCallocReply_JOB_ID_REPLY || r.GetPayloadJobIdReply().Ok || r.GetPayloadJobIdReply().FailureReason == "" {
					t.Fatalf("expected calloc failure, got %v, %v", r, err)
				}
				if _, err := calloc.Recv(); err != io.EOF {
					t.Fatalf("failed calloc did not exit: %v", err)
				}
				if r, err := attach.Recv(); err != nil || r.Type != protos.StreamCattachReply_STEP_CONNECT_REPLY || r.GetPayloadStepConnectReply().Ok || r.GetPayloadStepConnectReply().FailureReason == "" {
					t.Fatalf("expected cattach failure, got %v, %v", r, err)
				}
				if _, err := attach.Recv(); err != io.EOF {
					t.Fatalf("failed cattach did not exit: %v", err)
				}
			}
			c = registerTest(t, f.ctld)
			eventuallyTest(t, func() bool {
				gVars.ctldReplyChannelMapMtx.Lock()
				defer gVars.ctldReplyChannelMapMtx.Unlock()
				return len(gVars.ctldReplyChannelMapByPid)+len(gVars.ctldReplyChannelMapForCattachByPid) == 0
			})
			assertNoSessionsTest(t)
			// No duplicate submission, metadata retry or completion with an unknown
			// ID may precede this new request after registration.
			startCrunTest(t, f, c, 5005)
		})
	}
}

func TestCtldDisconnectCancelsPendingAllocations(t *testing.T) {
	f := startCforedTest(t)
	c := registerTest(t, f.ctld)
	crun, _ := startCrunTest(t, f, c, 6001)
	calloc, _ := submitCallocTest(t, f, c, 6002)
	c.replies <- jobIDReplyTest(6002)
	if r, err := calloc.Recv(); err != nil || !r.GetPayloadJobIdReply().GetOk() {
		t.Fatalf("calloc ID reply: %v, %v", r, err)
	}
	close(c.disconnect) // Neither resource allocation reply was delivered.
	if r, err := crun.Recv(); err != nil || r.Type != protos.StreamCrunReply_STEP_CANCEL_REQUEST {
		t.Fatalf("crun cancellation: %v, %v", r, err)
	}
	if r, err := calloc.Recv(); err != nil || r.Type != protos.StreamCallocReply_JOB_CANCEL_REQUEST {
		t.Fatalf("calloc cancellation: %v, %v", r, err)
	}
	if err := crun.Send(&protos.StreamCrunRequest{Type: protos.StreamCrunRequest_STEP_COMPLETION_REQUEST}); err != nil {
		t.Fatal(err)
	}
	if err := calloc.Send(&protos.StreamCallocRequest{Type: protos.StreamCallocRequest_JOB_COMPLETION_REQUEST}); err != nil {
		t.Fatal(err)
	}
	c = registerTest(t, f.ctld)
	completed := make(map[uint32]bool)
	for range 2 {
		r := receiveTest(t, c.requests)
		p := r.GetPayloadJobCompleteReq()
		if r.Type != protos.StreamCforedRequest_JOB_COMPLETION_REQUEST || p.StepId != 1 || (p.JobId != 6001 && p.JobId != 6002) || completed[p.JobId] {
			t.Fatalf("unexpected completion: %v", r)
		}
		completed[p.JobId] = true
		c.replies <- completionAckTest(p.JobId)
	}
	if r, err := crun.Recv(); err != nil || !r.GetPayloadStepCompletionAckReply().GetOk() {
		t.Fatalf("crun completion: %v, %v", r, err)
	}
	if r, err := calloc.Recv(); err != nil || !r.GetPayloadJobCompletionAckReply().GetOk() {
		t.Fatalf("calloc completion: %v, %v", r, err)
	}
	if _, err := crun.Recv(); err != io.EOF {
		t.Fatalf("crun did not exit: %v", err)
	}
	if _, err := calloc.Recv(); err != io.EOF {
		t.Fatalf("calloc did not exit: %v", err)
	}
	assertNoSessionsTest(t)
}

func TestCtldReconnectSendsQueuedSubmissionOnce(t *testing.T) {
	f := startCforedTest(t)
	c := registerTest(t, f.ctld)
	close(c.disconnect)
	c = receiveTest(t, f.ctld.streams)
	receiveTest(t, c.requests) // Hold the new stream in WaitReg.
	// Model a submission admitted just before disconnect, still in the input
	// queue. It must survive another failed registration without being failed.
	replies := make(chan *protos.StreamCtldReply, 2)
	gVars.ctldReplyChannelMapMtx.Lock()
	gVars.ctldReplyChannelMapByPid[7001] = replies
	gVars.ctldReplyChannelMapMtx.Unlock()
	request := &protos.StreamCforedRequest{
		Type: protos.StreamCforedRequest_JOB_REQUEST,
		Payload: &protos.StreamCforedRequest_PayloadJobReq{
			PayloadJobReq: &protos.StreamCforedRequest_JobReq{Pid: 7001, Job: crunRequestTest(7001).GetPayloadJobReq().Job},
		},
	}
	sendCtldRequest(request)
	close(c.disconnect)
	c = registerTest(t, f.ctld)
	if r := receiveTest(t, c.requests); !proto.Equal(r, request) {
		t.Fatalf("queued submission changed: %v", r)
	}
	c.replies <- &protos.StreamCtldReply{
		Type: protos.StreamCtldReply_JOB_ID_REPLY,
		Payload: &protos.StreamCtldReply_PayloadJobIdReply{
			PayloadJobIdReply: &protos.StreamCtldReply_JobIdReply{Pid: 7001, FailureReason: "test rejection"},
		},
	}
	if r := receiveTest(t, replies); r.GetPayloadJobIdReply().FailureReason != "test rejection" {
		t.Fatalf("unsent submission was failed on disconnect: %v", r)
	}
	gVars.ctldReplyChannelMapMtx.Lock()
	delete(gVars.ctldReplyChannelMapByPid, 7001)
	gVars.ctldReplyChannelMapMtx.Unlock()
	close(c.disconnect)
	c = registerTest(t, f.ctld)
	startCrunTest(t, f, c, 7002) // No retry of the previous submission.
}

func TestShutdownCleansWaitingAndUnallocatedSessions(t *testing.T) {
	f := startCforedTest(t)
	c := registerTest(t, f.ctld)
	stream, _ := startCrunTest(t, f, c, 2001)
	if err := stream.Send(&protos.StreamCrunRequest{Type: protos.StreamCrunRequest_STEP_COMPLETION_REQUEST}); err != nil {
		t.Fatal(err)
	}
	receiveTest(t, c.requests) // leave CrunWaitCtldAck without a Ctld ACK
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Calloc has not received its ID yet.
	calloc, err := f.front.CallocStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := calloc.Send(&protos.StreamCallocRequest{
		Type: protos.StreamCallocRequest_JOB_REQUEST,
		Payload: &protos.StreamCallocRequest_PayloadJobReq{PayloadJobReq: &protos.StreamCallocRequest_JobReq{
			CallocPid: 2002, Job: crunRequestTest(2002).GetPayloadJobReq().Job,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	receiveTest(t, c.requests)
	// Cattach has not received metadata yet.
	attach, err := f.front.CattachStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := attach.Send(&protos.StreamCattachRequest{
		Type: protos.StreamCattachRequest_STEP_CONNECT_REQUEST,
		Payload: &protos.StreamCattachRequest_PayloadStepConnectReq{
			PayloadStepConnectReq: &protos.StreamCattachRequest_StepConnectReq{CattachPid: 2003, JobId: 2001, StepId: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	receiveTest(t, c.requests)
	gVars.globalCtxCancel()
	receiveTest(t, f.ctld.exits)
	receiveTest(t, f.done)
	assertNoSessionsTest(t)
}

func TestShutdownDuringReconnectWithFullRequestQueue(t *testing.T) {
	f := startCforedTest(t)
	c := registerTest(t, f.ctld)
	var streams []protos.CraneForeD_CrunStreamClient
	for pid := int32(3001); pid <= 3012; pid++ {
		stream, _ := startCrunTest(t, f, c, pid)
		allocateCrunTest(t, c, stream, uint32(pid))
		streams = append(streams, stream)
	}
	close(c.disconnect)
	eventuallyTest(t, func() bool { return !gVars.ctldConnected.Load() })
	// Keep the new connection in WaitReg while more than eight existing
	// sessions publish completion. Some producers must block on the queue.
	c = receiveTest(t, f.ctld.streams)
	receiveTest(t, c.requests)
	for _, stream := range streams {
		// Complete while still waiting for supervisors. A full Ctld queue must
		// not prevent stopping either the handler or its readiness waiter.
		if err := stream.Send(&protos.StreamCrunRequest{Type: protos.StreamCrunRequest_STEP_COMPLETION_REQUEST}); err != nil {
			t.Fatal(err)
		}
	}
	eventuallyTest(t, func() bool { return len(gVars.cforedRequestCtldChannel) == cap(gVars.cforedRequestCtldChannel) })
	gVars.globalCtxCancel()
	receiveTest(t, f.done)
	assertNoSessionsTest(t)
}

func TestFrontEndWaitRequiresCleanupAndRejectsNewAdmissions(t *testing.T) {
	initCforedTest(t)
	if !startFrontEnd() {
		t.Fatal("initial admission failed")
	}
	done := make(chan struct{})
	go func() { waitFrontEnds(); close(done) }()
	eventuallyTest(t, func() bool {
		gVars.ctldReplyChannelMapMtx.Lock()
		defer gVars.ctldReplyChannelMapMtx.Unlock()
		return gVars.frontEndsStopping
	})
	if startFrontEnd() {
		t.Fatal("admitted a session after shutdown began")
	}
	select {
	case <-done:
		t.Fatal("wait returned before the existing session finished cleanup")
	default:
	}
	// Cleanup can take the mapping lock while Wait is blocked.
	if !gVars.ctldReplyChannelMapMtx.TryLock() {
		t.Fatal("Wait holds the mapping lock")
	}
	gVars.ctldReplyChannelMapMtx.Unlock()
	gVars.frontEndWG.Done()
	receiveTest(t, done)
}
