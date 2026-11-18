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
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/pkg/term/termios"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

type stdinRequestRecorder struct {
	requests chan *protos.StreamCrunRequest
}

func (r *stdinRequestRecorder) Send(request *protos.StreamCrunRequest) error {
	r.requests <- request
	return nil
}

func stdinTestFile(t *testing.T, contents []byte) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "in.txt")
	if err := os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func stdinTestPipe(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close(); w.Close() })
	return r, w
}

// Exercise both the input reader and the actual data/EOF request producer.
// The stream recorder replaces only the network transport.
func assertStdinForwarding(t *testing.T, f *os.File, want []byte, inputFlag bool, afterData func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &stdinRequestRecorder{requests: make(chan *protos.StreamCrunRequest, 128)}
	sender := newCrunStreamSender(ctx, r)
	defer sender.Close()
	m := &StateMachineOfCrun{
		job:                &protos.JobToCtld{Uid: uint32(os.Getuid())},
		stdinFd:            int(f.Fd()),
		stopReadCtx:        ctx,
		stopStepCb:         cancel,
		chanInputFromLocal: make(chan []byte, 2),
		sender:             sender,
	}
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
	if err != nil {
		t.Fatal(err)
	}
	readerDone := make(chan struct{})
	forwarderDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		if inputFlag {
			m.FileReaderRoutine(f.Name())
		} else {
			m.StdinReaderRoutine()
		}
	}()
	go func() {
		defer close(forwarderDone)
		m.forwardLocalRequests(nil)
	}()
	t.Cleanup(func() {
		cancel()
		for _, done := range []<-chan struct{}{readerDone, forwarderDone} {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("input goroutine did not stop after cancellation")
			}
		}
	})
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	var got []byte
	for {
		select {
		case request := <-r.requests:
			if request.Type != protos.StreamCrunRequest_TASK_IO_FORWARD {
				t.Fatalf("unexpected request type: %v", request.Type)
			}
			input := request.GetPayloadTaskIoForwardReq()
			got = append(got, input.Msg...)
			if afterData != nil && len(got) == len(want) {
				afterData()
				afterData = nil
			}
			if !input.Eof {
				continue
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("forwarded %d bytes, want %d; data differs", len(got), len(want))
			}
			select {
			case <-forwarderDone:
			case <-deadline.C:
				t.Fatal("forwarder did not stop after EOF")
			}
			if len(r.requests) != 0 {
				t.Fatal("unexpected requests after EOF")
			}
			if gotFlags, err := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0); err != nil || gotFlags != flags {
				t.Fatalf("stdin flags changed: got %d, want %d, error: %v", gotFlags, flags, err)
			}
			return
		case <-deadline.C:
			t.Fatalf("timed out waiting for EOF after %d bytes", len(got))
		}
	}
}

func TestCrunStdinForwarding(t *testing.T) {
	payload := bytes.Repeat([]byte("filedata\n"), 2000)
	for _, inputFlag := range []bool{false, true} {
		name := "redirect"
		if inputFlag {
			name = "input_flag"
		}
		t.Run(name, func(t *testing.T) {
			assertStdinForwarding(t, stdinTestFile(t, payload), payload, inputFlag, nil)
		})
	}
	t.Run("file_current_offset", func(t *testing.T) {
		f := stdinTestFile(t, []byte("skip\nfiledata\n"))
		if _, err := f.Seek(5, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		assertStdinForwarding(t, f, []byte("filedata\n"), false, nil)
	})
	t.Run("empty_file", func(t *testing.T) {
		assertStdinForwarding(t, stdinTestFile(t, nil), nil, false, nil)
	})
	t.Run("dev_null", func(t *testing.T) {
		f, err := os.Open(os.DevNull)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		assertStdinForwarding(t, f, nil, false, nil)
	})
	t.Run("closed_empty_pipe", func(t *testing.T) {
		r, w := stdinTestPipe(t)
		w.Close()
		assertStdinForwarding(t, r, nil, false, nil)
	})
	t.Run("closed_buffered_pipe", func(t *testing.T) {
		r, w := stdinTestPipe(t)
		data := []byte("piped_input\n")
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		w.Close()
		assertStdinForwarding(t, r, data, false, nil)
	})
	t.Run("pipe_closes_after_data_forwarded", func(t *testing.T) {
		r, w := stdinTestPipe(t)
		data := []byte("piped_input\n")
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		assertStdinForwarding(t, r, data, false, func() { w.Close() })
	})
	t.Run("streaming_pipe", func(t *testing.T) {
		r, w := stdinTestPipe(t)
		written := make(chan error, 1)
		go func() {
			_, err := w.Write(payload)
			w.Close()
			written <- err
		}()
		assertStdinForwarding(t, r, payload, false, nil)
		if err := <-written; err != nil {
			t.Fatal(err)
		}
	})
	for _, raw := range []bool{false, true} {
		name := "canonical_terminal_eof"
		if raw {
			name = "raw_terminal_hangup"
		}
		t.Run(name, func(t *testing.T) {
			master, slave, err := termios.Pty()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { master.Close(); slave.Close() })
			data := []byte("terminal_input\n")
			input := append([]byte(nil), data...)
			var afterData func()
			if raw {
				if _, err := term.MakeRaw(int(slave.Fd())); err != nil {
					t.Fatal(err)
				}
				afterData = func() { master.Close() }
			} else {
				attrs, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TCGETS)
				if err != nil {
					t.Fatal(err)
				}
				attrs.Lflag |= unix.ICANON
				attrs.Cc[unix.VEOF] = 4
				if err := unix.IoctlSetTermios(int(slave.Fd()), unix.TCSETS, attrs); err != nil {
					t.Fatal(err)
				}
				input = append(input, 4)
			}
			if _, err := master.Write(input); err != nil {
				t.Fatal(err)
			}
			assertStdinForwarding(t, slave, data, false, afterData)
		})
	}
}

func TestCrunStdinCancellation(t *testing.T) {
	for _, pendingInput := range []bool{false, true} {
		name := "idle_pipe"
		if pendingInput {
			name = "pending_input_without_receiver"
		}
		t.Run(name, func(t *testing.T) {
			r, w := stdinTestPipe(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			m := &StateMachineOfCrun{
				stdinFd: int(r.Fd()), stopReadCtx: ctx,
				chanInputFromLocal: make(chan []byte),
			}
			done := make(chan struct{})
			go func() {
				m.StdinReaderRoutine()
				close(done)
			}()
			if _, err := w.Write([]byte("first\n")); err != nil {
				t.Fatal(err)
			}
			select {
			case <-m.chanInputFromLocal:
			case <-time.After(5 * time.Second):
				t.Fatal("reader did not forward initial data")
			}
			if pendingInput {
				if _, err := w.Write([]byte("second\n")); err != nil {
					t.Fatal(err)
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("reader did not stop after cancellation")
			}
			if _, ok := <-m.chanInputFromLocal; ok {
				t.Fatal("input channel was not closed")
			}
		})
	}
}

func TestCrunX11ForwardingAfterStdinEOF(t *testing.T) {
	for _, test := range []struct {
		name   string
		nilEOF bool
		x11    bool
	}{
		{name: "closed_stdin_with_x11", x11: true},
		{name: "nil_eof_with_x11", nilEOF: true, x11: true},
		{name: "closed_stdin_without_x11"},
		{name: "nil_eof_without_x11", nilEOF: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			recorder := &stdinRequestRecorder{requests: make(chan *protos.StreamCrunRequest, 128)}
			sender := newCrunStreamSender(ctx, recorder)
			m := &StateMachineOfCrun{
				stopReadCtx: ctx, stopStepCb: cancel, sender: sender,
				chanInputFromLocal: make(chan []byte, 2),
			}
			if test.x11 {
				m.X11SessionMgr = NewX11SessionMgr(&protos.X11Meta{}, &ctx)
			}
			data := []byte("input before EOF\n")
			m.chanInputFromLocal <- data
			if test.nilEOF {
				m.chanInputFromLocal <- nil
			} else {
				close(m.chanInputFromLocal)
			}
			taskID := uint32(3)
			done := make(chan struct{})
			go func() {
				defer close(done)
				m.forwardLocalRequests(&taskID)
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("forwarder did not stop after cancellation")
				}
				sender.Close()
			})
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			for _, eof := range []bool{false, true} {
				select {
				case request := <-recorder.requests:
					input := request.GetPayloadTaskIoForwardReq()
					if request.Type != protos.StreamCrunRequest_TASK_IO_FORWARD || input == nil || input.Eof != eof {
						t.Fatalf("expected stdin request with EOF=%t, got %v", eof, request)
					}
					if input.TaskId == nil || *input.TaskId != taskID {
						t.Fatalf("stdin request lost destination task %d", taskID)
					}
					if (!eof && !bytes.Equal(input.Msg, data)) || (eof && len(input.Msg) != 0) {
						t.Fatalf("unexpected stdin data with EOF=%t: %q", eof, input.Msg)
					}
				case <-deadline.C:
					t.Fatalf("timed out waiting for stdin request with EOF=%t", eof)
				}
			}
			if test.x11 {
				// Send only after stdin EOF has reached the transport. More than
				// one X11 channel's capacity must still be forwarded, with no
				// repeated stdin EOF messages interleaved.
				for i := 0; i <= cap(m.X11SessionMgr.X11RequestChan); i++ {
					request := &protos.StreamCrunRequest{
						Type: protos.StreamCrunRequest_STEP_X11_FORWARD,
						Payload: &protos.StreamCrunRequest_PayloadStepX11ForwardReq{
							PayloadStepX11ForwardReq: &protos.StreamCrunRequest_StepX11ForwardReq{
								CranedId: "node1", LocalId: uint32(i), Msg: []byte("X11 response"),
							},
						},
					}
					m.X11SessionMgr.X11RequestChan <- request
					select {
					case got := <-recorder.requests:
						if got != request {
							t.Fatalf("expected X11 request %d after stdin EOF, got %v", i, got)
						}
					case <-done:
						t.Fatal("stdin EOF stopped X11 forwarding")
					case <-deadline.C:
						t.Fatalf("timed out forwarding X11 request %d after stdin EOF", i)
					}
				}
				cancel()
			}
			select {
			case <-done:
			case <-deadline.C:
				t.Fatal("forwarder did not stop")
			}
			if len(recorder.requests) != 0 {
				t.Fatal("unexpected requests after forwarding completed")
			}
		})
	}
}
