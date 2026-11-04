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
	"sync"
)

type crunRequestStream interface {
	Send(*protos.StreamCrunRequest) error
}

type crunSendRequest struct {
	message *protos.StreamCrunRequest
	result  chan error
}

// crunStreamSender is the only owner allowed to call Send on CrunStream.
// gRPC permits one concurrent reader and writer, but not multiple writers.
type crunStreamSender struct {
	ctx      context.Context
	cancel   context.CancelFunc
	stream   crunRequestStream
	requests chan crunSendRequest
	done     chan struct{}
	close    sync.Once
}

func newCrunStreamSender(
	parent context.Context,
	stream crunRequestStream,
) *crunStreamSender {
	ctx, cancel := context.WithCancel(parent)
	sender := &crunStreamSender{
		ctx:      ctx,
		cancel:   cancel,
		stream:   stream,
		requests: make(chan crunSendRequest, 128),
		done:     make(chan struct{}),
	}
	go sender.run()
	return sender
}

func (sender *crunStreamSender) run() {
	defer close(sender.done)
	for {
		select {
		case <-sender.ctx.Done():
			return
		case request := <-sender.requests:
			err := sender.stream.Send(request.message)
			request.result <- err
			if err != nil {
				sender.cancel()
				return
			}
		}
	}
}

func (sender *crunStreamSender) Send(
	ctx context.Context,
	message *protos.StreamCrunRequest,
) error {
	result := make(chan error, 1)
	request := crunSendRequest{message: message, result: result}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sender.ctx.Done():
		return sender.ctx.Err()
	case sender.requests <- request:
	}

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-sender.ctx.Done():
		select {
		case err := <-result:
			return err
		default:
			return sender.ctx.Err()
		}
	}
}

func (sender *crunStreamSender) Close() {
	sender.close.Do(sender.cancel)
	<-sender.done
}
