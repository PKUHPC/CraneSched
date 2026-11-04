/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

package cfored

import (
	"CraneFrontEnd/generated/protos"
	"sync/atomic"
	"testing"
)

func TestTerminalResizeRequiresSupervisorCapability(t *testing.T) {
	keeper := NewCranedChannelKeeper()
	valid := &atomic.Bool{}
	valid.Store(true)
	requests := make(chan *protos.StreamCrunRequest, 1)
	attachments := make(chan *protos.StreamCattachRequest, 1)
	request := &protos.StreamCrunRequest{Type: protos.StreamCrunRequest_TERMINAL_RESIZE}

	keeper.supervisorUpAndSetMsgToSupervisorChannel(
		1, 0, "craned0", requests, attachments, valid, false,
	)
	keeper.forwardTerminalResizeToSingleSupervisor(1, 0, "craned0", request)
	select {
	case <-requests:
		t.Fatal("resize was forwarded to a supervisor without resize capability")
	default:
	}

	keeper.supervisorUpAndSetMsgToSupervisorChannel(
		1, 0, "craned0", requests, attachments, valid, true,
	)
	keeper.forwardTerminalResizeToSingleSupervisor(1, 0, "craned0", request)
	select {
	case got := <-requests:
		if got != request {
			t.Fatal("forwarded resize request does not match the original request")
		}
	default:
		t.Fatal("resize was not forwarded to a supervisor with resize capability")
	}
}
