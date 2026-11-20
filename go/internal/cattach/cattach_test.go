/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

package cattach

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/util"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type cattachTestStream struct {
	protos.CraneForeD_CattachStreamClient
	recv func() (*protos.StreamCattachReply, error)
}

func (s *cattachTestStream) Recv() (*protos.StreamCattachReply, error) {
	return s.recv()
}

func captureCattachLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var output bytes.Buffer
	oldOutput := log.StandardLogger().Out
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(oldOutput) })
	return &output
}

func waitCattachDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cattach shutdown")
	}
}

func newCattachTestMachine(t *testing.T, ctx context.Context, cancel context.CancelFunc, stream protos.CraneForeD_CattachStreamClient) *StateMachineOfCattach {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///cattach-test", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	receiver := new(CforedReplyReceiver)
	receiver.StartReplyReceiveRoutine(ctx, stream)
	machine := &StateMachineOfCattach{
		conn: conn, streamCancel: cancel, cforedReplyReceiver: receiver,
		state: WaitForward,
	}
	t.Cleanup(func() {
		cancel()
		conn.Close()
		receiver.Wait()
	})
	return machine
}

func TestCattachCloseWaitsForReceiver(t *testing.T) {
	output := captureCattachLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	stream := &cattachTestStream{recv: func() (*protos.StreamCattachReply, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		return nil, status.Error(codes.Canceled, "grpc: the client connection is closing")
	}}
	machine := newCattachTestMachine(t, ctx, cancel, stream)
	waitCattachDone(t, started)
	done := make(chan struct{})
	go func() {
		machine.Close()
		close(done)
	}()
	waitCattachDone(t, cancelled)
	select {
	case <-done:
		t.Fatal("Close returned before the receiver exited")
	default:
	}
	close(release)
	waitCattachDone(t, done)
	if output.Len() != 0 {
		t.Fatalf("normal shutdown wrote to stderr: %s", output.String())
	}
}

func TestCattachCloseWithFullReplyChannel(t *testing.T) {
	output := captureCattachLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	received := make(chan struct{})
	stream := &cattachTestStream{recv: func() (*protos.StreamCattachReply, error) {
		select {
		case received <- struct{}{}:
			return &protos.StreamCattachReply{}, nil
		case <-ctx.Done():
			return nil, status.Error(codes.Canceled, "stream cancelled")
		}
	}}
	machine := newCattachTestMachine(t, ctx, cancel, stream)
	// Leave the reply queue undrained, as happens after --layout enters End.
	// The extra reply forces the receiver to block while publishing it.
	for range cap(machine.cforedReplyReceiver.replyChannel) + 1 {
		waitCattachDone(t, received)
	}
	done := make(chan struct{})
	go func() {
		machine.Close()
		close(done)
	}()
	waitCattachDone(t, done)
	if output.Len() != 0 {
		t.Fatalf("normal shutdown wrote to stderr: %s", output.String())
	}
}

func TestCattachUnexpectedReceiveErrors(t *testing.T) {
	for _, recvErr := range []error{
		status.Error(codes.Canceled, "server cancelled stream"),
		status.Error(codes.Unavailable, "transport failed"),
		io.EOF,
	} {
		t.Run(recvErr.Error(), func(t *testing.T) {
			output := captureCattachLogs(t)
			ctx, cancel := context.WithCancel(context.Background())
			stream := &cattachTestStream{recv: func() (*protos.StreamCattachReply, error) {
				return nil, recvErr
			}}
			machine := newCattachTestMachine(t, ctx, cancel, stream)
			machine.StateWaitForward()
			machine.cforedReplyReceiver.Wait()
			if machine.state != End || machine.err != util.ErrorNetwork {
				t.Fatalf("state = %v, exit code = %v; want End and ErrorNetwork", machine.state, machine.err)
			}
			if !strings.Contains(output.String(), recvErr.Error()) {
				t.Fatalf("missing connection error in stderr: %q", output.String())
			}
		})
	}
}

type cattachLayoutServer struct {
	protos.UnimplementedCraneForeDServer
	replyErr error
}

func (s *cattachLayoutServer) CattachStream(stream grpc.BidiStreamingServer[protos.StreamCattachRequest, protos.StreamCattachReply]) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	connect := request.GetPayloadStepConnectReq()
	if request.GetType() != protos.StreamCattachRequest_STEP_CONNECT_REQUEST || connect.GetJobId() != 1 || connect.GetStepId() != 1 {
		return status.Error(codes.InvalidArgument, "expected a connection request for step 1.1")
	}
	if s.replyErr != nil {
		return s.replyErr
	}
	err = stream.Send(&protos.StreamCattachReply{
		Type: protos.StreamCattachReply_STEP_CONNECT_REPLY,
		Payload: &protos.StreamCattachReply_PayloadStepConnectReply{
			PayloadStepConnectReply: &protos.StreamCattachReply_StepConnectReply{
				Ok: true,
				StepInfo: &protos.CattachStepInfo{
					Ntasks: 2, NodeNum: 2, Nodelist: "crnd[1-2]",
					CranedTaskMap: map[string]*protos.CattachStepInfo_NodeTasks{
						"crnd1": {TaskIds: []uint32{0}},
						"crnd2": {TaskIds: []uint32{1}},
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	// Keep Recv blocked on the client until it initiates normal shutdown.
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestCattachCLIHelper(t *testing.T) {
	config := os.Getenv("CRANE_CATTACH_TEST_CONFIG")
	if config == "" {
		return
	}
	RootCmd.SetArgs([]string{"--config", config, "--layout", "1.1"})
	ParseCmdArgs()
}

func TestCattachLayoutCLI(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "remote cancellation", err: status.Error(codes.Canceled, "server cancelled stream")},
		{name: "transport failure", err: status.Error(codes.Unavailable, "transport failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Keep the Unix socket path below the platform limit even for long test names.
			dir, err := os.MkdirTemp("", "cattach-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(dir) })
			listener, err := net.Listen("unix", filepath.Join(dir, "cfored.sock"))
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			protos.RegisterCraneForeDServer(server, &cattachLayoutServer{replyErr: test.err})
			go server.Serve(listener)
			t.Cleanup(server.Stop)
			config := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(config, []byte(fmt.Sprintf("CraneBaseDir: %q\nCranedCforedSockPath: cfored.sock\nCraneCtldForInternalListenPort: '10013'\n", dir)), 0600); err != nil {
				t.Fatal(err)
			}
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, executable, "-test.run=^TestCattachCLIHelper$")
			for _, variable := range os.Environ() {
				key, _, _ := strings.Cut(variable, "=")
				switch strings.ToLower(key) {
				case "http_proxy", "https_proxy", "all_proxy", "no_proxy", "crane_cattach_test_config":
					continue
				}
				command.Env = append(command.Env, variable)
			}
			command.Env = append(command.Env, "CRANE_CATTACH_TEST_CONFIG="+config)
			var stdout, stderr bytes.Buffer
			command.Stdout, command.Stderr = &stdout, &stderr
			err = command.Run()
			if test.err != nil {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) || exitErr.ExitCode() != util.ErrorNetwork {
					t.Fatalf("exit = %v, want ErrorNetwork; stderr: %s", err, stderr.String())
				}
				if stdout.Len() != 0 || !strings.Contains(stderr.String(), test.err.Error()) {
					t.Fatalf("unexpected error output: stdout %q, stderr %q", stdout.String(), stderr.String())
				}
				return
			}
			if err != nil || stderr.Len() != 0 {
				t.Fatalf("cattach --layout: exit = %v, stderr = %q", err, stderr.String())
			}
			want := "Job step layout:\n        2 tasks, 2 nodes (crnd[1-2])\n\n        Node 0 (crnd1), 1 task(s): 0\n        Node 1 (crnd2), 1 task(s): 1\n\n"
			if stdout.String() != want {
				t.Fatalf("layout = %q, want %q", stdout.String(), want)
			}
		})
	}
}
