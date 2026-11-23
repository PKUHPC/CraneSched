/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

package crun

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/util"
	"github.com/spf13/cobra"
)

func TestMainCrunRejectsInvalidSignal(t *testing.T) {
	oldSignal := FlagSignal
	t.Cleanup(func() { FlagSignal = oldSignal })

	for _, tt := range []struct {
		input string
		why   string
	}{
		{"", "empty signal"},
		{"R:USR1@65536", "invalid signal time"},
		{"B:USR1@1", "B: is only supported for batch submissions"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().StringVar(&FlagSignal, "signal", "", "")
			if err := cmd.ParseFlags([]string{"--signal=" + tt.input}); err != nil {
				t.Fatal(err)
			}
			err := MainCrun(cmd, []string{"true"})
			argErr, ok := errors.AsType[*util.CraneError](err)
			if !ok || argErr.Code != util.ErrorCmdArg || !strings.Contains(argErr.Message, "--signal: "+tt.why) {
				t.Fatalf("got %v, want a --signal argument error containing %q", err, tt.why)
			}
		})
	}
}

type stdinTestStream struct {
	requests chan *protos.StreamCrunRequest
}

func (s *stdinTestStream) Send(request *protos.StreamCrunRequest) error {
	s.requests <- request
	return nil
}

func testInputFile(t *testing.T, data []byte) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { file.Close() })
	return file
}

func testInputPipe(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close(); writer.Close() })
	return reader, writer
}

func testInputForwarder(t *testing.T, input *os.File) (*StateMachineOfCrun, <-chan struct{}, *stdinTestStream) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stream := &stdinTestStream{requests: make(chan *protos.StreamCrunRequest, 8)}
	sender := newCrunStreamSender(ctx, stream)
	machine := &StateMachineOfCrun{
		stdinFd:            int(input.Fd()),
		stopReadCtx:        ctx,
		stopStepCb:         cancel,
		chanInputFromLocal: make(chan []byte, 2),
		sender:             sender,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		machine.forwardLocalRequests(nil)
	}()
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		machine.StdinReaderRoutine()
	}()
	t.Cleanup(func() {
		cancel()
		for _, finished := range []<-chan struct{}{readerDone, done} {
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Error("stdin goroutine did not stop")
			}
		}
		sender.Close()
	})
	return machine, done, stream
}

func waitStdinRequest(t *testing.T, requests <-chan *protos.StreamCrunRequest, eof bool) *protos.StreamCrunRequest {
	t.Helper()
	select {
	case request := <-requests:
		input := request.GetPayloadTaskIoForwardReq()
		if request.Type != protos.StreamCrunRequest_TASK_IO_FORWARD || input == nil || input.Eof != eof {
			t.Fatalf("stdin EOF=%t, got request %v", eof, request)
		}
		return request
	case <-time.After(time.Second):
		t.Fatalf("no stdin request with EOF=%t", eof)
		return nil
	}
}

func TestCrunStdinFileAndPipeEOF(t *testing.T) {
	payload := bytes.Repeat([]byte("stdin data\n"), 512)
	t.Run("regular file at current offset", func(t *testing.T) {
		file := testInputFile(t, append([]byte("skip"), payload...))
		if _, err := file.Seek(4, 0); err != nil {
			t.Fatal(err)
		}
		_, done, stream := testInputForwarder(t, file)
		assertForwardedInput(t, stream.requests, done, payload, false, nil)
	})

	t.Run("pipe data before EOF", func(t *testing.T) {
		for _, closeAfterRead := range []bool{false, true} {
			name := "writer closes after write"
			if closeAfterRead {
				name = "writer closes after read"
			}
			t.Run(name, func(t *testing.T) {
				reader, writer := testInputPipe(t)
				if _, err := writer.Write(payload); err != nil {
					t.Fatal(err)
				}
				if !closeAfterRead {
					if err := writer.Close(); err != nil {
						t.Fatal(err)
					}
				}
				_, done, stream := testInputForwarder(t, reader)
				assertForwardedInput(t, stream.requests, done, payload, closeAfterRead, func() {
					if err := writer.Close(); err != nil {
						t.Fatal(err)
					}
				})
			})
		}
	})
}

func assertForwardedInput(t *testing.T, requests <-chan *protos.StreamCrunRequest, done <-chan struct{},
	want []byte, closeAfterRead bool, closeWriter func()) {
	t.Helper()
	var got []byte
	writerClosed := false
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case request := <-requests:
			input := request.GetPayloadTaskIoForwardReq()
			if request.Type != protos.StreamCrunRequest_TASK_IO_FORWARD || input == nil {
				t.Fatalf("unexpected request %v", request)
			}
			if input.Eof {
				if !bytes.Equal(got, want) {
					t.Fatalf("forwarded %d bytes, want %d", len(got), len(want))
				}
				select {
				case <-done:
				case <-deadline.C:
					t.Fatal("forwarder did not exit after stdin EOF")
				}
				return
			}
			got = append(got, input.Msg...)
			if closeAfterRead && !writerClosed && len(got) > 0 {
				closeWriter()
				writerClosed = true
			}
		case <-deadline.C:
			t.Fatal("timed out waiting for stdin EOF")
		}
	}
}

func TestCrunStdinReaderStopsOnCancellation(t *testing.T) {
	reader, writer := testInputPipe(t)
	ctx, cancel := context.WithCancel(context.Background())
	machine := &StateMachineOfCrun{stdinFd: int(reader.Fd()), stopReadCtx: ctx, chanInputFromLocal: make(chan []byte)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		machine.StdinReaderRoutine()
	}()
	if _, err := writer.Write([]byte("first\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-machine.chanInputFromLocal:
	case <-time.After(time.Second):
		t.Fatal("reader did not forward input")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reader did not stop after cancellation")
	}
	if _, ok := <-machine.chanInputFromLocal; ok {
		t.Fatal("reader did not close its input channel")
	}
}

func TestCrunX11ForwardingContinuesAfterStdinEOF(t *testing.T) {
	t.Run("nil EOF message", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		stream := &stdinTestStream{requests: make(chan *protos.StreamCrunRequest, 2)}
		sender := newCrunStreamSender(ctx, stream)
		machine := &StateMachineOfCrun{
			stopReadCtx: ctx, stopStepCb: cancel, sender: sender,
			chanInputFromLocal: make(chan []byte),
			X11SessionMgr:      NewX11SessionMgr(&protos.X11Meta{}, &ctx),
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			machine.forwardLocalRequests(nil)
		}()
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("forwarder did not stop after cancellation")
			}
			sender.Close()
		})

		machine.chanInputFromLocal <- []byte("input")
		if input := waitStdinRequest(t, stream.requests, false).GetPayloadTaskIoForwardReq(); string(input.Msg) != "input" {
			t.Fatalf("forwarded stdin = %q", input.Msg)
		}
		machine.chanInputFromLocal <- nil
		waitStdinRequest(t, stream.requests, true)
		request := &protos.StreamCrunRequest{
			Type: protos.StreamCrunRequest_STEP_X11_FORWARD,
			Payload: &protos.StreamCrunRequest_PayloadStepX11ForwardReq{
				PayloadStepX11ForwardReq: &protos.StreamCrunRequest_StepX11ForwardReq{
					CranedId: "node1", Msg: []byte("X11 request"),
				},
			},
		}
		machine.X11SessionMgr.X11RequestChan <- request
		select {
		case got := <-stream.requests:
			if got != request {
				t.Fatalf("forwarded request = %v, want X11 request %v", got, request)
			}
		case <-done:
			t.Fatal("X11 forwarding stopped after stdin EOF")
		case <-time.After(time.Second):
			t.Fatal("X11 request was not forwarded")
		}
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("forwarder did not stop after cancellation")
		}
	})

	t.Run("without X11 EOF ends forwarding", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		stream := &stdinTestStream{requests: make(chan *protos.StreamCrunRequest, 2)}
		sender := newCrunStreamSender(ctx, stream)
		machine := &StateMachineOfCrun{
			stopReadCtx: ctx, stopStepCb: cancel, sender: sender,
			chanInputFromLocal: make(chan []byte),
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			machine.forwardLocalRequests(nil)
		}()
		t.Cleanup(sender.Close)
		machine.chanInputFromLocal <- nil
		waitStdinRequest(t, stream.requests, true)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("forwarder did not exit after EOF without X11")
		}
	})
}
