// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package cacctmgr

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/util"
	"google.golang.org/grpc"
)

type qosModifyClient struct {
	protos.CraneCtldClient
	reply *protos.ModifyQosReply
}

func (c qosModifyClient) ModifyQos(context.Context, *protos.ModifyQosRequest, ...grpc.CallOption) (*protos.ModifyQosReply, error) {
	return c.reply, nil
}

func TestModifyQosFailureExplainsError(t *testing.T) {
	oldStub, oldJSON, oldForce := stub, FlagJson, FlagForce
	t.Cleanup(func() { stub, FlagJson, FlagForce = oldStub, oldJSON, oldForce })
	FlagJson, FlagForce = false, false

	for _, tc := range []struct {
		name   string
		errors []*protos.RichError
		want   string
	}{
		{"missing details", nil, "controller returned no error details"},
		{"permission denied", []*protos.RichError{{Code: protos.ErrCode_ERR_PERMISSION_USER, Description: "alice"}}, "Your permission is insufficient"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub = qosModifyClient{reply: &protos.ModifyQosReply{RichErrorList: tc.errors}}
			err := ModifyQos(nil, "normal")
			craneErr, ok := errors.AsType[*util.CraneError](err)
			if !ok || craneErr.Code != util.ErrorBackend || !strings.Contains(craneErr.Message, tc.want) {
				t.Fatalf("wanted backend error containing %q, got %v", tc.want, err)
			}
		})
	}
}
