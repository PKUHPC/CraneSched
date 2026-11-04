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
	"CraneFrontEnd/generated/protos"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recordingCrunStream struct {
	active    atomic.Int32
	maxActive atomic.Int32
	count     atomic.Int32
}

func (stream *recordingCrunStream) Send(*protos.StreamCrunRequest) error {
	active := stream.active.Add(1)
	for {
		maximum := stream.maxActive.Load()
		if active <= maximum || stream.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	time.Sleep(time.Millisecond)
	stream.count.Add(1)
	stream.active.Add(-1)
	return nil
}

func TestCrunStreamSenderSerializesConcurrentWriters(t *testing.T) {
	stream := &recordingCrunStream{}
	sender := newCrunStreamSender(context.Background(), stream)
	defer sender.Close()

	const requestCount = 64
	var writers sync.WaitGroup
	for range requestCount {
		writers.Add(1)
		go func() {
			defer writers.Done()
			if err := sender.Send(context.Background(), &protos.StreamCrunRequest{}); err != nil {
				t.Errorf("Send() returned error: %v", err)
			}
		}()
	}
	writers.Wait()

	if got := stream.count.Load(); got != requestCount {
		t.Fatalf("Send() count = %d, want %d", got, requestCount)
	}
	if got := stream.maxActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent stream.Send calls = %d, want 1", got)
	}
}

type blockingCrunStream struct {
	started chan struct{}
	release chan struct{}
}

func (stream *blockingCrunStream) Send(*protos.StreamCrunRequest) error {
	close(stream.started)
	<-stream.release
	return nil
}

func TestCrunStreamSenderCallerCanCancelBlockedSend(t *testing.T) {
	stream := &blockingCrunStream{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	sender := newCrunStreamSender(context.Background(), stream)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- sender.Send(ctx, &protos.StreamCrunRequest{})
	}()

	<-stream.started
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Send() did not return after its context was cancelled")
	}

	close(stream.release)
	sender.Close()
}

type failingCrunStream struct {
	err error
}

func (stream *failingCrunStream) Send(*protos.StreamCrunRequest) error {
	return stream.err
}

func TestCrunStreamSenderStopsAfterSendFailure(t *testing.T) {
	wantErr := errors.New("send failed")
	sender := newCrunStreamSender(
		context.Background(),
		&failingCrunStream{err: wantErr},
	)
	defer sender.Close()

	if err := sender.Send(context.Background(), &protos.StreamCrunRequest{}); !errors.Is(err, wantErr) {
		t.Fatalf("first Send() error = %v, want %v", err, wantErr)
	}
	if err := sender.Send(context.Background(), &protos.StreamCrunRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("second Send() error = %v, want context.Canceled", err)
	}
}

func TestValidTerminalSize(t *testing.T) {
	tests := []struct {
		name          string
		rows, columns int
		want          bool
	}{
		{name: "normal", rows: 24, columns: 80, want: true},
		{name: "maximum", rows: 65535, columns: 65535, want: true},
		{name: "zero rows", rows: 0, columns: 80},
		{name: "zero columns", rows: 24, columns: 0},
		{name: "rows overflow", rows: 65536, columns: 80},
		{name: "columns overflow", rows: 24, columns: 65536},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validTerminalSize(test.rows, test.columns); got != test.want {
				t.Fatalf("validTerminalSize(%d, %d) = %t, want %t", test.rows, test.columns, got, test.want)
			}
		})
	}
}
